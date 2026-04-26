"""Catalog extraction facade — OCR + LLM extraction pipeline.

Delegates OCR to ocr_complete virtualenv, extracts products via LLM,
and saves in mitsubishi_test format (product_model UNIQUE, spec_group support).
"""

from __future__ import annotations

import glob
import json
import os
import re
import subprocess
from pathlib import Path

from .db import (
    get_db,
    bulk_upsert_products,
    is_file_processed,
    mark_file_processed,
)

PROJECT_DIR = Path(__file__).resolve().parents[1]
OCR_COMPLETE_DIR = PROJECT_DIR / "ocr_complete"
OCR_VENV_PYTHON = OCR_COMPLETE_DIR / "venv" / "bin" / "python"
JSON_MARKER = "__EXTRACTOR_V1_JSON__"

TABLE_LLM_PROMPT = """You are an electrical/industrial product data extractor. Given a table from a product catalog PDF, extract every product row into structured JSON.

For each product row, extract:
- "product_name": a clear product name (include brand if known)
- "product_model": the catalog, part, or order number (this MUST be unique per product)
- "category": one of MCB, MCCB, RCCB, RCBO, ACB, Isolator, Contactor, Relay, Switch, Fuse, SPD, Starter, Controller, or Other
- "subcategory": a more specific classification if identifiable
- "brand": the manufacturer name
- "description": a brief product description if available
- "mrp": the price if listed (keep currency symbol)
- "hsn_code": the HSN/HS code if listed
- "specs": a dictionary of all useful specifications found in the row
- "spec_group": a label for the group of specs (e.g. "Specifications", "Electrical Ratings", "Environmental Specifications")

Rules:
- Extract every distinct row or product variant
- If a column header applies to all rows, include it in each product's specs
- Catalog or part numbers are usually alphanumeric codes
- Prices may be in INR, USD, EUR, or unlabeled; keep the unit when present
- If data is unclear, keep what is explicit and skip what is not
- Return only a JSON array
"""


def _run_ocr_complete(script: str, payload: dict, timeout: int = 900) -> dict:
    """Execute a helper script inside the ocr_complete virtualenv."""
    proc = subprocess.run(
        [str(OCR_VENV_PYTHON), "-c", script],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        timeout=timeout,
        cwd=str(OCR_COMPLETE_DIR),
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip() or proc.stdout.strip()
        raise RuntimeError(stderr.splitlines()[-1] if stderr else "ocr_complete subprocess failed")

    marker = f"{JSON_MARKER}\n"
    if marker not in proc.stdout:
        raise RuntimeError("ocr_complete subprocess did not emit JSON payload")
    return json.loads(proc.stdout.split(marker, 1)[1])


def _auto_brand(filename: str, fallback: str | None = None) -> str | None:
    if fallback:
        return fallback
    lower_name = filename.lower()
    brands = {
        "schneider": "Schneider",
        "siemens": "Siemens",
        "abb": "ABB",
        "legrand": "Legrand",
        "hager": "Hager",
        "mitsubishi": "Mitsubishi Electric",
        "lauritz": "Lauritz Knudsen",
        "lk": "Lauritz Knudsen",
        "havells": "Havells",
        "polycab": "Polycab",
        "l&t": "L&T",
        "tridium": "Tridium",
    }
    for key, value in brands.items():
        if key in lower_name:
            return value
    return None


def _infer_brand_from_text(text: str) -> str | None:
    lower_text = text.lower()
    brands = {
        "schneider": "Schneider",
        "siemens": "Siemens",
        "abb": "ABB",
        "legrand": "Legrand",
        "hager": "Hager",
        "mitsubishi": "Mitsubishi Electric",
        "lauritz knudsen": "Lauritz Knudsen",
        "lk": "Lauritz Knudsen",
        "havells": "Havells",
        "polycab": "Polycab",
        "l&t": "L&T",
        "tridium": "Tridium",
    }
    for key, value in brands.items():
        if key in lower_text:
            return value
    return None


def _tables_to_text(tables, filename: str, brand_hint: str | None = None, max_rows: int = 50) -> str:
    """Flatten OCR tables into text so LLM/pattern extractors get context too."""
    parts = [f"Source document: {filename}"]
    if brand_hint:
        parts.append(f"Brand: {brand_hint}")
    for index, table in enumerate(tables, start=1):
        headers = [str(h).strip() for h in table.get("headers", [])]
        rows = table.get("rows", [])
        if not headers and not rows:
            continue
        parts.append(f"Table {index}")
        if headers:
            parts.append("Columns: " + " | ".join(headers))
        for row in rows[:max_rows]:
            cells = [str(cell).strip() for cell in row]
            parts.append(" | ".join(cells))
        if len(rows) > max_rows:
            parts.append(f"... ({len(rows) - max_rows} more rows)")
    return "\n".join(parts)


def _normalize_llm_product(item: dict, brand_hint: str | None = None, filename: str | None = None) -> dict | None:
    """Convert raw LLM extraction output into our DB format."""
    model = item.get("product_model") or item.get("identifier") or item.get("model")
    if not model:
        return None

    name = item.get("product_name") or item.get("name") or model
    category = item.get("category") or item.get("type")
    subcategory = item.get("subcategory")
    brand = item.get("brand") or brand_hint
    description = item.get("description")
    mrp = item.get("mrp") or item.get("price")
    hsn_code = item.get("hsn_code")
    spec_group = item.get("spec_group") or "Specifications"

    specs = item.get("specs", {})
    if isinstance(specs, list):
        specs = {s.get("key", f"spec_{i}"): s.get("value", "") for i, s in enumerate(specs)}

    # Add source document as a spec
    if filename:
        specs["Source Document"] = filename

    return {
        "product_name": name,
        "product_model": str(model).strip(),
        "description": description,
        "category": category,
        "subcategory": subcategory,
        "brand": brand,
        "hsn_code": str(hsn_code) if hsn_code else None,
        "mrp": str(mrp) if mrp else None,
        "specs": specs,
        "spec_group": spec_group,
        "catalogue_name": filename,
    }


def _extract_from_tables_with_specialized_llm(tables, filename, brand_hint=None):
    """Fallback table-oriented LLM extraction using vLLM directly."""
    script = f"""
import json
import requests
import sys

from pipeline import extractor

payload = json.load(sys.stdin)

def format_table(headers, rows, filename, max_rows=50):
    lines = [f"Source document: {{filename}}"]
    lines.append("Columns: " + " | ".join(str(h) for h in headers))
    lines.append("---")
    for row in rows[:max_rows]:
        cells = [str(row[i]) if i < len(row) else "" for i in range(len(headers))]
        lines.append(" | ".join(cells))
    if len(rows) > max_rows:
        lines.append(f"... ({{len(rows) - max_rows}} more rows)")
    return "\\n".join(lines)

all_products = []
for tidx, table in enumerate(payload["tables"]):
    headers = table.get("headers", [])
    rows = table.get("rows", [])
    if not headers or not rows:
        continue

    raw_rows = [headers] + rows
    col_counts = [len(row) for row in raw_rows]
    if col_counts and (max(col_counts) - min(col_counts) > 1):
        continue

    normalised = extractor.normalize_table(raw_rows)
    headers = normalised[0]
    rows = normalised[1:]
    table_text = format_table(headers, rows, payload["filename"])
    brand_line = f"Brand: {{payload['brand_hint']}}\\n" if payload.get("brand_hint") else ""
    prompt = f\"\"\"{TABLE_LLM_PROMPT}

{{brand_line}}Table {{tidx + 1}} from "{{payload['filename']}}":

{{table_text}}

Extract all products as a JSON array:\"\"\"

    resp = requests.post(
        extractor.VLLM_URL,
        json={{
            "model": extractor.MODEL,
            "messages": [
                {{"role": "system", "content": "You extract structured product data from catalogs. Output valid JSON only."}},
                {{"role": "user", "content": prompt}},
            ],
            "max_tokens": 8192,
            "temperature": 0.05,
            "chat_template_kwargs": {{"enable_thinking": False}},
        }},
        timeout=180,
    )
    if resp.status_code != 200:
        continue

    content = resp.json()["choices"][0]["message"]["content"]
    items = extractor._parse_json_response(content) or []
    for item in items:
        all_products.append(item)

print("{JSON_MARKER}")
print(json.dumps(all_products))
"""
    return _run_ocr_complete(
        script,
        {
            "tables": tables,
            "filename": filename,
            "brand_hint": brand_hint,
        },
        timeout=900,
    )


def extract_from_tables(tables, filename, brand_hint=None):
    """Delegate table extraction to ocr_complete.pipeline.extractor.extract_products."""
    brand_hint = _auto_brand(filename, brand_hint)
    script = f"""
import json
import sys

from pipeline import extractor

payload = json.load(sys.stdin)
products = extractor.extract_products(
    payload["ocr_text"],
    tables=payload["tables"],
    brand=payload.get("brand_hint"),
    source_page=payload.get("source_page"),
)
print("{JSON_MARKER}")
print(json.dumps(products))
"""
    result = _run_ocr_complete(
        script,
        {
            "tables": tables,
            "ocr_text": _tables_to_text(tables, filename, brand_hint),
            "brand_hint": brand_hint,
            "source_page": None,
        },
        timeout=600,
    )
    if not result:
        result = _extract_from_tables_with_specialized_llm(tables, filename, brand_hint)

    # Normalize into our DB format
    normalized = []
    for item in result:
        product = _normalize_llm_product(item, brand_hint=brand_hint, filename=filename)
        if product:
            if not product.get("brand") or product["brand"] == "Unknown":
                inferred = brand_hint or _infer_brand_from_text(
                    f"{product.get('product_name', '')} {filename}"
                )
                if inferred:
                    product["brand"] = inferred
            normalized.append(product)
    return normalized


def save_products(products, catalogue_name=None):
    """Save products to DB using bulk upsert. Returns (inserted, skipped)."""
    return bulk_upsert_products(products, catalogue_name=catalogue_name)


def _parse_pipeline_messages(messages: list[str], filename: str, brand_hint: str | None) -> dict:
    """Extract a compact stats dict from yielded pipeline status messages."""
    stats = {
        "filename": filename,
        "brand": brand_hint,
        "messages": messages,
        "extracted": 0,
        "inserted": 0,
        "updated": 0,
        "errors": 0,
    }
    for message in messages:
        match = re.search(r"After dedup:\s+(\d+)\s+products", message)
        if match:
            stats["extracted"] = int(match.group(1))
        match = re.search(r"Inserted:\s+(\d+),\s+Updated:\s+(\d+),\s+Errors:\s+(\d+)", message)
        if match:
            stats["inserted"] = int(match.group(1))
            stats["updated"] = int(match.group(2))
            stats["errors"] = int(match.group(3))
        match = re.search(r"Done!\s+(\d+)\s+products", message)
        if match and not stats["extracted"]:
            stats["extracted"] = int(match.group(1))
    return stats


def process_catalog(file_path, brand_hint=None):
    """Delegate single-catalog processing to the unified ocr_complete pipeline."""
    abs_path = os.path.abspath(file_path)
    filename = os.path.basename(abs_path)
    already_done, prev_record = is_file_processed(abs_path)
    if already_done:
        return {
            "filename": filename,
            "already_processed": True,
            "previous": prev_record,
        }

    script = f"""
import json
import sys

from pipeline import product_pipeline

payload = json.load(sys.stdin)
brand_hint = payload.get("brand_hint")
if brand_hint:
    iterator = product_pipeline._process_single_pdf(
        payload["file_path"],
        brand=brand_hint,
        extract_images=False,
    )
else:
    iterator = product_pipeline.run_pipeline(
        pdf_path=payload["file_path"],
        extract_images=False,
    )

messages = [str(item) for item in iterator]
print("{JSON_MARKER}")
print(json.dumps({{"messages": messages}}))
"""
    result = _run_ocr_complete(
        script,
        {"file_path": abs_path, "brand_hint": brand_hint},
        timeout=3600,
    )
    stats = _parse_pipeline_messages(result.get("messages", []), filename, brand_hint)
    mark_file_processed(
        abs_path,
        filename,
        products_inserted=stats["inserted"] + stats["updated"],
        products_skipped=stats["errors"],
    )
    return stats


def process_catalog_from_tables(tables, filename, brand_hint=None):
    """Extract and save products from pre-OCR tables."""
    brand = _auto_brand(filename, brand_hint)
    products = extract_from_tables(tables, filename, brand)
    if not products:
        return {"extracted": 0, "inserted": 0, "skipped": 0}

    inserted, skipped = save_products(products, catalogue_name=filename)
    return {"extracted": len(products), "inserted": inserted, "skipped": skipped}


def batch_process_folder(folder_path, brand_hint=None):
    """Process a folder of PDFs through the extraction pipeline."""
    from .image_extractor import extract_images_from_pdf, link_images_to_products

    folder_path = os.path.abspath(folder_path)
    pdf_files = sorted(glob.glob(os.path.join(folder_path, "*.pdf")))
    if not pdf_files:
        pdf_files = sorted(glob.glob(os.path.join(folder_path, "**/*.pdf"), recursive=True))
    if not pdf_files:
        return {"error": "No PDFs found", "folder": folder_path}

    results = []
    total_inserted = 0
    total_updated = 0
    total_errors = 0
    total_images_linked = 0
    processed = 0
    skipped_files = 0
    failed_files = 0

    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        already_done, prev_record = is_file_processed(pdf_path)
        if already_done:
            skipped_files += 1
            results.append({
                "filename": filename,
                "status": "skipped",
                "reason": "already_processed",
                "previous": prev_record,
            })
            continue

        try:
            file_brand = _auto_brand(filename, brand_hint)
            stats = process_catalog(pdf_path, brand_hint=file_brand)

            images = extract_images_from_pdf(pdf_path)
            linked = link_images_to_products(images, filename, pdf_path=pdf_path)

            total_inserted += stats["inserted"]
            total_updated += stats["updated"]
            total_errors += stats["errors"]
            total_images_linked += linked
            processed += 1

            mark_file_processed(
                pdf_path,
                filename,
                products_inserted=stats["inserted"] + stats["updated"],
                products_skipped=stats["errors"],
                images_linked=linked,
            )
            results.append({
                "filename": filename,
                "status": "processed",
                **stats,
                "images_linked": linked,
            })
        except Exception as exc:
            failed_files += 1
            results.append({
                "filename": filename,
                "status": "failed",
                "error": str(exc),
            })

    return {
        "folder": folder_path,
        "total_pdfs": len(pdf_files),
        "processed": processed,
        "skipped_files": skipped_files,
        "failed_files": failed_files,
        "total_inserted": total_inserted,
        "total_updated": total_updated,
        "total_errors": total_errors,
        "total_images_linked": total_images_linked,
        "results": results,
    }
