"""CPU-only PDF text & table extraction using PyMuPDF + pdfplumber.

No GPU, no OCR model, no timeouts. Works on PDFs with embedded text
(which covers all files in pdfsmanual/).

Handles:
- Merged/spanning header cells (common in electrical catalogs)
- Multi-level column headers
- Text-based fallback when pdfplumber misses tables
"""

import os
import re
import time

import fitz
import pdfplumber


def has_embedded_text(pdf_path: str, sample_pages: int = 3) -> bool:
    """Quick check: does the PDF have selectable text on the first few pages?"""
    doc = fitz.open(pdf_path)
    pages_to_check = min(len(doc), sample_pages)
    text_found = False
    for i in range(pages_to_check):
        if doc[i].get_text().strip():
            text_found = True
            break
    doc.close()
    return text_found


def extract_text_fitz(pdf_path: str) -> str:
    """Extract all text from PDF using PyMuPDF (fast, CPU-only)."""
    doc = fitz.open(pdf_path)
    pages = []
    for page in doc:
        pages.append(page.get_text())
    doc.close()
    return "\n".join(pages)


def _clean_cell(cell) -> str:
    """Clean a table cell value."""
    if cell is None:
        return ""
    s = str(cell).strip()
    # Fix newlines inside cells
    s = re.sub(r'\n+', ' ', s)
    # Remove CID-encoded chars
    s = re.sub(r'\(cid:\d+\)', '', s)
    return s.strip()


def _fix_merged_headers(headers: list[str], first_data_row: list[str] | None) -> list[str]:
    """Fix headers with many empty cells from merged/spanning columns.

    Strategy:
    - If >50% of headers are empty, merge with first data row as sub-headers
    - Forward-fill empty headers from the last non-empty header
    """
    non_empty = [h for h in headers if h and h != "None"]
    total = len(headers)

    if total == 0:
        return headers

    # If most headers are empty, use first data row as sub-headers
    empty_ratio = (total - len(non_empty)) / total
    if empty_ratio > 0.5 and first_data_row:
        merged = []
        last_parent = ""
        for i, h in enumerate(headers):
            parent = h if (h and h != "None") else last_parent
            if h and h != "None":
                last_parent = h
            sub = first_data_row[i] if i < len(first_data_row) else ""
            sub = _clean_cell(sub)
            if parent and sub and parent != sub:
                merged.append(f"{parent} - {sub}")
            elif parent:
                merged.append(parent)
            elif sub:
                merged.append(sub)
            else:
                merged.append(f"Col_{i+1}")
        return merged

    # Forward-fill empty headers
    result = []
    last_non_empty = ""
    seen_count = {}
    for h in headers:
        if h and h != "None":
            last_non_empty = h
            seen_count[h] = seen_count.get(h, 0) + 1
            if seen_count[h] > 1:
                result.append(f"{h}_{seen_count[h]}")
            else:
                result.append(h)
        elif last_non_empty:
            seen_count[last_non_empty] = seen_count.get(last_non_empty, 0) + 1
            result.append(f"{last_non_empty}_{seen_count[last_non_empty]}")
        else:
            result.append(f"Col_{len(result)+1}")
    return result


def extract_tables_pdfplumber(pdf_path: str) -> list[dict]:
    """Extract tables from PDF using pdfplumber (CPU-only).

    Returns list of dicts matching the OCR pipeline format:
        [{"headers": [...], "rows": [[...], ...]}, ...]
    """
    tables = []

    # Try with line-based strategy first (works better for ruled-line catalogs)
    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 5,
        "join_tolerance": 5,
    }

    with pdfplumber.open(pdf_path) as pdf:
        for page_idx, page in enumerate(pdf.pages):
            # Try line-based first, fall back to text-based
            page_tables = page.extract_tables(table_settings)
            if not page_tables:
                page_tables = page.extract_tables()
            if not page_tables:
                continue

            for tidx, raw_table in enumerate(page_tables):
                if not raw_table or len(raw_table) < 2:
                    continue

                # Clean all cells
                cleaned = []
                for row in raw_table:
                    cleaned.append([_clean_cell(c) for c in row])

                # Find the first non-empty row to use as headers
                header_idx = 0
                for i, row in enumerate(cleaned):
                    non_empty = [c for c in row if c]
                    if len(non_empty) >= 2:  # At least 2 non-empty cells = likely a header
                        header_idx = i
                        break

                headers = cleaned[header_idx]
                remaining = cleaned[header_idx + 1:]

                # Fix merged/spanning headers
                first_data = remaining[0] if remaining else None
                headers = _fix_merged_headers(headers, first_data)

                # Check if first data row was consumed as sub-headers
                # (if >50% of original headers were empty, skip it)
                raw_headers = [_clean_cell(c) for c in raw_table[header_idx]]
                non_empty_h = [h for h in raw_headers if h]
                empty_ratio = (len(raw_headers) - len(non_empty_h)) / max(len(raw_headers), 1)
                if empty_ratio > 0.5 and remaining:
                    remaining = remaining[1:]  # First data row was used for sub-headers

                # Filter empty rows and rows that are all the same as headers
                rows = []
                for row in remaining:
                    if any(c for c in row):
                        rows.append(row)

                if rows and len(headers) >= 2:
                    tables.append({
                        "headers": headers,
                        "rows": rows,
                        "page": page_idx + 1,
                        "table_index": tidx,
                    })

    return tables


def _extract_tables_from_text(text: str, pdf_path: str) -> list[dict]:
    """Fallback: extract tabular data from raw fitz text using line alignment.

    Catches tables that pdfplumber misses (e.g., text-only layouts without rules).
    Looks for lines with consistent delimiters (multiple spaces, tabs) that form rows.
    """
    doc = fitz.open(pdf_path)
    tables = []

    for page_idx in range(len(doc)):
        page = doc[page_idx]
        page_text = page.get_text()
        lines = page_text.split('\n')

        # Find groups of consecutive lines with similar column counts
        # (lines with 2+ whitespace-separated tokens that repeat)
        tabular_lines = []
        for line in lines:
            # Split on 2+ spaces (common column separator in PDF text)
            parts = re.split(r'\s{2,}', line.strip())
            if len(parts) >= 3:
                tabular_lines.append(parts)
            elif tabular_lines and len(tabular_lines) >= 3:
                # End of a table block — save it
                if len(tabular_lines) >= 3:
                    headers = tabular_lines[0]
                    rows = tabular_lines[1:]
                    tables.append({
                        "headers": headers,
                        "rows": rows,
                        "page": page_idx + 1,
                        "table_index": len(tables),
                    })
                tabular_lines = []

        # Handle table at end of page
        if len(tabular_lines) >= 3:
            headers = tabular_lines[0]
            rows = tabular_lines[1:]
            tables.append({
                "headers": headers,
                "rows": rows,
                "page": page_idx + 1,
                "table_index": len(tables),
            })

    doc.close()
    return tables


def _ocr_page_by_page(pdf_path: str) -> tuple[str, list[dict]]:
    """GPU OCR fallback for scanned/image-only PDFs.

    Renders each page as an image and runs DeepSeek-OCR-2 one page at a time,
    clearing CUDA cache between pages to minimize GPU memory usage.
    """
    import subprocess, json

    ocr_venv_python = os.path.expanduser("~/mcb_test/ocr_complete/venv/bin/python")
    ocr_dir = os.path.expanduser("~/mcb_test/ocr_complete")

    if not os.path.exists(ocr_venv_python):
        raise RuntimeError("OCR venv not found at ~/mcb_test/ocr_complete/venv")

    doc = fitz.open(pdf_path)
    num_pages = len(doc)
    doc.close()

    all_text = []
    all_tables = []

    # Process page-by-page to control GPU memory
    for page_idx in range(num_pages):
        ocr_script = f"""
import sys, json, gc, torch
sys.path.insert(0, '{ocr_dir}')
from pipeline import processor

# Render single page to temp image
import fitz
doc = fitz.open('{pdf_path}')
page = doc[{page_idx}]
zoom = 300 / 72
mat = fitz.Matrix(zoom, zoom)
pix = page.get_pixmap(matrix=mat)
import tempfile, os
tmp = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
pix.save(tmp.name)
doc.close()

text, tables, dt, method, file_type, num_pages = processor.process(tmp.name)
os.unlink(tmp.name)

# Clear GPU memory immediately
gc.collect()
if torch.cuda.is_available():
    torch.cuda.empty_cache()

result = {{"text": text, "tables": tables, "page": {page_idx + 1}}}
print("__PAGE_OCR_JSON__")
print(json.dumps(result))
"""
        try:
            proc = subprocess.run(
                [ocr_venv_python, "-c", ocr_script],
                capture_output=True, text=True, timeout=120,
                cwd=ocr_dir,
                env={**os.environ, "PYTORCH_CUDA_ALLOC_CONF": "expandable_segments:True"},
            )
            if proc.returncode != 0:
                print(f"  [OCR] Page {page_idx+1} failed: {proc.stderr.strip().split(chr(10))[-1]}")
                continue

            marker = "__PAGE_OCR_JSON__\n"
            if marker in proc.stdout:
                result = json.loads(proc.stdout.split(marker, 1)[1])
                all_text.append(result.get("text", ""))
                for t in result.get("tables", []):
                    t["page"] = page_idx + 1
                    all_tables.append(t)
        except subprocess.TimeoutExpired:
            print(f"  [OCR] Page {page_idx+1} timed out, skipping")
            continue
        except Exception as e:
            print(f"  [OCR] Page {page_idx+1} error: {e}")
            continue

    return "\n".join(all_text), all_tables


def process_page_range(pdf_path: str, start_page: int = 1, end_page: int | None = None) -> tuple:
    """Extract from a specific page range. Pages are 1-indexed.

    Returns: (text, tables, dt, method, file_type, num_pages_processed)
    """
    t0 = time.time()
    doc = fitz.open(pdf_path)
    total_pages = len(doc)
    start_idx = max(0, start_page - 1)
    end_idx = min(total_pages, end_page) if end_page else total_pages

    # Text extraction for the range
    pages_text = []
    for i in range(start_idx, end_idx):
        pages_text.append(doc[i].get_text())
    doc.close()
    text = "\n".join(pages_text)

    # Table extraction for the range
    tables = []
    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 5,
        "join_tolerance": 5,
    }
    with pdfplumber.open(pdf_path) as pdf:
        for page_idx in range(start_idx, min(end_idx, len(pdf.pages))):
            page = pdf.pages[page_idx]
            page_tables = page.extract_tables(table_settings)
            if not page_tables:
                page_tables = page.extract_tables()
            if not page_tables:
                continue

            for tidx, raw_table in enumerate(page_tables):
                if not raw_table or len(raw_table) < 2:
                    continue
                cleaned = [[_clean_cell(c) for c in row] for row in raw_table]
                header_idx = 0
                for i, row in enumerate(cleaned):
                    if len([c for c in row if c]) >= 2:
                        header_idx = i
                        break
                headers = cleaned[header_idx]
                remaining = cleaned[header_idx + 1:]
                first_data = remaining[0] if remaining else None
                headers = _fix_merged_headers(headers, first_data)
                raw_headers = [_clean_cell(c) for c in raw_table[header_idx]]
                non_empty_h = [h for h in raw_headers if h]
                empty_ratio = (len(raw_headers) - len(non_empty_h)) / max(len(raw_headers), 1)
                if empty_ratio > 0.5 and remaining:
                    remaining = remaining[1:]
                rows = [r for r in remaining if any(c for c in r)]
                if rows and len(headers) >= 2:
                    tables.append({
                        "headers": headers, "rows": rows,
                        "page": page_idx + 1, "table_index": tidx,
                    })

    dt = time.time() - t0
    num_processed = end_idx - start_idx
    return text, tables, dt, f"pdfplumber+fitz (CPU, pages {start_page}-{end_idx})", "pdf", num_processed


def process(pdf_path: str) -> tuple:
    """Drop-in replacement for ocr_complete processor.process().

    Strategy:
    1. If PDF has embedded text → CPU-only extraction (no GPU)
    2. If scanned/image-only → page-by-page GPU OCR with cache clearing

    Returns: (text, tables, dt, method, file_type, num_pages)
    """
    t0 = time.time()

    doc = fitz.open(pdf_path)
    num_pages = len(doc)
    doc.close()

    embedded = has_embedded_text(pdf_path)

    if embedded:
        # Fast CPU path
        text = extract_text_fitz(pdf_path)
        tables = extract_tables_pdfplumber(pdf_path)

        # Supplement with text-based extraction if pdfplumber found too few
        if len(tables) < num_pages * 0.1 and num_pages > 5:
            text_tables = _extract_tables_from_text(text, pdf_path)
            if len(text_tables) > len(tables):
                tables = text_tables

        method = "pdfplumber+fitz (CPU)"
    else:
        # GPU OCR fallback — page-by-page to control memory
        print(f"  [PDF] No embedded text, using GPU OCR page-by-page...")
        text, tables = _ocr_page_by_page(pdf_path)
        method = "deepseek-ocr (GPU, page-by-page)"

    dt = time.time() - t0
    file_type = "pdf"

    return text, tables, dt, method, file_type, num_pages


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        pdf_path = sys.argv[1]
        text, tables, dt, method, ftype, pages = process(pdf_path)
        print(f"File: {os.path.basename(pdf_path)}")
        print(f"Pages: {pages}, Text: {len(text)} chars, Tables: {len(tables)}, Time: {dt:.2f}s")
        for i, t in enumerate(tables[:10]):
            print(f"  Table {i+1}: {len(t['headers'])} cols, {len(t['rows'])} rows (page {t.get('page', '?')})")
            print(f"    Headers: {t['headers'][:5]}")
            if t['rows']:
                print(f"    Row 1: {t['rows'][0][:5]}")
    else:
        print("Usage: python pdf_extractor.py <path.pdf>")
