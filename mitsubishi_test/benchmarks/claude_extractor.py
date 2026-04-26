"""Claude CLI Extractor — OCR + structured extraction using Claude Code CLI.

Uses the same approach as NeuraReport:
  - claude -p --bare --model sonnet (via subprocess)
  - Vision: sends page images to Claude for OCR
  - Extraction: sends OCR'd tables to Claude for structured product data
  - Stores results in lk_catalog_claude DB for benchmarking
"""

import os
import re
import json
import shutil
import subprocess
import tempfile
import time
import base64
import psycopg2
import fitz
from PIL import Image

CLAUDE_BIN = shutil.which("claude") or os.path.expanduser("~/.local/bin/claude")
CLAUDE_MODEL = "sonnet"
TIMEOUT = 120

CATALOG_DB = dict(dbname="lk_catalog_claude", user="postgres", host="localhost", port=5432)
IMAGE_DIR = os.path.expanduser("~/mcb_test/images")


def _get_db():
    return psycopg2.connect(**CATALOG_DB)


# ══════════════════════════════════════════════════════
# Claude CLI wrapper (same pattern as NeuraReport)
# ══════════════════════════════════════════════════════

def _call_claude(prompt, model=CLAUDE_MODEL):
    """Call Claude Code CLI with a text prompt.

    Uses: claude -p --bare --model sonnet
    Input via stdin, output from stdout.
    """
    cmd = [CLAUDE_BIN, "-p", "--bare", "--model", model]

    with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as f:
        f.write(prompt)
        prompt_file = f.name

    try:
        # Route through LiteLLM proxy (same as NeuraReport)
        env = os.environ.copy()
        env['ANTHROPIC_BASE_URL'] = 'http://localhost:4000'
        env['ANTHROPIC_API_KEY'] = 'dummy'

        with open(prompt_file, 'r', encoding='utf-8') as pf:
            result = subprocess.run(
                cmd, stdin=pf, capture_output=True, text=True,
                timeout=TIMEOUT, env=env,
            )

        if result.returncode != 0:
            error = (result.stderr or result.stdout or "").strip()[:300]
            print(f"  [Claude] CLI error (rc={result.returncode}): {error}")
            return None

        return result.stdout.strip()

    except subprocess.TimeoutExpired:
        print(f"  [Claude] Timeout after {TIMEOUT}s")
        return None
    except Exception as e:
        print(f"  [Claude] Error: {e}")
        return None
    finally:
        os.unlink(prompt_file)


# ══════════════════════════════════════════════════════
# OCR via Claude Vision (page images → text + tables)
# ══════════════════════════════════════════════════════

def _render_pdf_pages(pdf_path, dpi=200):
    """Render PDF pages as PNG images in a persistent location."""
    out_dir = os.path.join(IMAGE_DIR, "claude_ocr")
    os.makedirs(out_dir, exist_ok=True)
    doc = fitz.open(pdf_path)
    pages = []
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    for i in range(len(doc)):
        pix = doc[i].get_pixmap(matrix=mat)
        img_path = os.path.join(out_dir, f"page_{i+1}.png")
        pix.save(img_path)
        pages.append((i + 1, img_path))
    doc.close()
    return pages


def ocr_page_with_claude(image_path, page_num):
    """OCR a single page image using Claude Vision via CLI.

    Passes the image file path directly to Claude CLI.
    Returns extracted text with tables.
    """
    prompt = f"""Look at the image file at {image_path}. This is page {page_num} from an electrical product catalog PDF (Lauritz Knudsen MCB catalog).

Extract ALL text and tables from this page. For tables, output them in clean markdown table format with | separators.

Be precise with:
- Catalog/part numbers (alphanumeric codes like BB10160C, AU15S10803C)
- Prices/MRP values (in ₹)
- Ratings (amps like 0.5A, 1A, 6A, 10A, 16A, 20A, 25A, 32A, 40A, 50A, 63A)
- All column headers
- Pole configurations (SP, DP, TP, FP)
- Curve types (B, C, D)

Output the COMPLETE content — every single row. Don't summarize or skip any rows."""

    print(f"  [Claude OCR] Processing page {page_num}...")
    t0 = time.time()
    result = _call_claude(prompt)
    dt = time.time() - t0
    print(f"  [Claude OCR] Page {page_num}: {len(result or '')} chars, {dt:.1f}s")
    return result or ""


def ocr_pdf_with_claude(pdf_path):
    """OCR an entire PDF using Claude Vision.

    Returns (full_text, time_seconds).
    """
    filename = os.path.basename(pdf_path)
    print(f"\n[Claude OCR] Processing: {filename}")

    pages = _render_pdf_pages(pdf_path)
    all_text = []
    total_time = 0

    for page_num, img_path in pages:
        t0 = time.time()
        text = ocr_page_with_claude(img_path, page_num)
        dt = time.time() - t0
        total_time += dt
        all_text.append(f"--- Page {page_num} ---\n{text}")

    combined = "\n\n".join(all_text)
    print(f"[Claude OCR] Done: {len(combined)} chars, {total_time:.1f}s total")
    return combined, total_time


# ══════════════════════════════════════════════════════
# Structured extraction via Claude LLM
# ══════════════════════════════════════════════════════

EXTRACTION_PROMPT = """You are an electrical product data extractor. Given OCR text from an electrical catalog page, extract EVERY product into structured JSON.

For EACH product, extract:
- "product_name": Clear name like "MCB SP 16A C-Curve" (include brand if known)
- "product_model": The catalog/part number (e.g. "BB10160C", "AU15S10803C")
- "category": One of: MCB, MCCB, RCCB, RCBO, DC MCB, HR MCB, Solar Combo MCB, Other
- "specs": Dictionary of ALL key-value specs: rating, poles, voltage, price/MRP, curve type, breaking capacity, modules, standards, etc.

Rules:
- Extract EVERY product row, not just samples
- Catalog numbers are alphanumeric (e.g. BB10160C, BK2040DC, SC221616)
- Include MRP/price if present
- Return ONLY a JSON array, no explanation"""


def extract_products_with_claude(ocr_text, filename):
    """Extract structured products from OCR text using Claude.

    Processes page by page to stay within context limits.
    Returns list of product dicts.
    """
    all_products = []

    # Split into pages
    pages = re.split(r'--- Page \d+ ---', ocr_text)
    pages = [p.strip() for p in pages if p.strip()]

    for i, page_text in enumerate(pages):
        if len(page_text) < 50:
            continue

        prompt = f"""{EXTRACTION_PROMPT}

Source: {filename}, Page {i + 1}

OCR Text:
{page_text}

Extract all products as a JSON array:"""

        print(f"  [Claude Extract] Page {i + 1} ({len(page_text)} chars)...")
        t0 = time.time()
        raw = _call_claude(prompt)
        dt = time.time() - t0

        if not raw:
            print(f"  [Claude Extract] Page {i + 1}: no response")
            continue

        products = _parse_json(raw)
        for p in products:
            p["_source_file"] = filename
            p["_page"] = i + 1
        all_products.extend(products)
        print(f"  [Claude Extract] Page {i + 1}: {len(products)} products, {dt:.1f}s")

    return all_products


def _parse_json(text):
    """Extract JSON array from Claude response."""
    if not text:
        return []
    text = re.sub(r'^```(?:json)?\s*', '', text.strip())
    text = re.sub(r'\s*```$', '', text.strip())
    try:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    print(f"  [Claude] JSON parse failed ({len(text)} chars)")
    return []


# ══════════════════════════════════════════════════════
# Database storage
# ══════════════════════════════════════════════════════

def save_products(products):
    """Save extracted products to lk_catalog_claude DB."""
    conn = _get_db()
    inserted = 0
    skipped = 0

    try:
        with conn.cursor() as cur:
            for p in products:
                model = p.get("product_model", "").strip()
                name = p.get("product_name", "").strip()
                category = p.get("category", "Other").strip()
                specs = p.get("specs", {})

                if not model or not name:
                    skipped += 1
                    continue

                cur.execute("SELECT id FROM products WHERE product_model = %s", (model,))
                if cur.fetchone():
                    skipped += 1
                    continue

                desc_parts = [f"{k}: {v}" for k, v in list(specs.items())[:5]]
                description = f"{name}. {'; '.join(desc_parts)}" if desc_parts else name

                cur.execute(
                    """INSERT INTO products (product_name, product_model, description, category)
                       VALUES (%s, %s, %s, %s) RETURNING id""",
                    (name, model, description, category)
                )
                pid = cur.fetchone()[0]

                for key, value in specs.items():
                    if value and str(value).strip() and str(value).strip() != "-":
                        cur.execute(
                            "INSERT INTO product_specs (product_id, spec_key, spec_value) VALUES (%s, %s, %s)",
                            (pid, str(key), str(value))
                        )

                cur.execute(
                    "INSERT INTO product_specs (product_id, spec_key, spec_value) VALUES (%s, %s, %s)",
                    (pid, "Source Document", p.get("_source_file", ""))
                )
                cur.execute(
                    "INSERT INTO product_specs (product_id, spec_key, spec_value) VALUES (%s, %s, %s)",
                    (pid, "Extraction Method", "Claude CLI (sonnet)")
                )

                inserted += 1

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  [Claude DB] Error: {e}")
    finally:
        conn.close()

    return inserted, skipped


# ══════════════════════════════════════════════════════
# Full pipeline
# ══════════════════════════════════════════════════════

def process_catalog(pdf_path):
    """Full Claude pipeline: Vision OCR → LLM Extraction → DB.

    Returns stats dict.
    """
    filename = os.path.basename(pdf_path)
    print(f"\n{'='*60}")
    print(f"CLAUDE PIPELINE: {filename}")
    print(f"{'='*60}")

    t_start = time.time()

    # Step 1: OCR with Claude Vision
    print("\n[Step 1] Claude Vision OCR...")
    ocr_text, ocr_time = ocr_pdf_with_claude(pdf_path)

    # Step 2: Extract structured products
    print(f"\n[Step 2] Claude LLM Extraction...")
    products = extract_products_with_claude(ocr_text, filename)
    print(f"  Total extracted: {len(products)} products")

    # Step 3: Save to DB
    print(f"\n[Step 3] Saving to lk_catalog_claude DB...")
    inserted, skipped = save_products(products)

    total_time = time.time() - t_start

    stats = {
        "filename": filename,
        "pipeline": "Claude CLI (sonnet)",
        "ocr_time": ocr_time,
        "total_time": total_time,
        "ocr_chars": len(ocr_text),
        "products_extracted": len(products),
        "products_inserted": inserted,
        "products_skipped": skipped,
    }

    print(f"\n{'='*60}")
    print(f"DONE: {stats}")
    print(f"{'='*60}")
    return stats


# ══════════════════════════════════════════════════════
# Benchmark comparison
# ══════════════════════════════════════════════════════

def benchmark():
    """Compare lk_catalog (Qwen) vs lk_catalog_claude (Claude) databases."""

    qwen_db = dict(dbname="lk_catalog", user="postgres", host="localhost", port=5432)
    claude_db = dict(dbname="lk_catalog_claude", user="postgres", host="localhost", port=5432)

    print("\n" + "="*70)
    print("  BENCHMARK: Qwen3.5-27B vs Claude Sonnet")
    print("="*70)

    for label, db_params in [("Qwen3.5-27B (local)", qwen_db), ("Claude Sonnet (CLI)", claude_db)]:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()

        cur.execute("SELECT count(*) FROM products")
        total = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM product_specs")
        specs = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM products WHERE image_url IS NOT NULL")
        images = cur.fetchone()[0]
        cur.execute("SELECT category, count(*) FROM products GROUP BY category ORDER BY count(*) DESC")
        cats = cur.fetchall()

        print(f"\n── {label} ──")
        print(f"  Products: {total}")
        print(f"  Specs: {specs}")
        print(f"  Images: {images}")
        print(f"  Specs/product: {specs/total:.1f}" if total > 0 else "  Specs/product: 0")
        for cat, cnt in cats:
            print(f"    {cat}: {cnt}")

        conn.close()

    # Cross-validate: check common models
    qwen_conn = psycopg2.connect(**qwen_db)
    claude_conn = psycopg2.connect(**claude_db)

    qwen_cur = qwen_conn.cursor()
    claude_cur = claude_conn.cursor()

    claude_cur.execute("SELECT product_model FROM products")
    claude_models = set(r[0] for r in claude_cur.fetchall())

    qwen_cur.execute("SELECT product_model FROM products")
    qwen_models = set(r[0] for r in qwen_cur.fetchall())

    common = claude_models & qwen_models
    only_claude = claude_models - qwen_models
    only_qwen = qwen_models - claude_models

    print(f"\n── Cross-Validation ──")
    print(f"  Common models: {len(common)}")
    print(f"  Only in Claude: {len(only_claude)}")
    print(f"  Only in Qwen: {len(only_qwen)}")

    # Compare specs for common models
    if common:
        matches = 0
        mismatches = 0
        total_checks = 0

        for model in list(common)[:20]:  # Check first 20
            qwen_cur.execute("""SELECT s.spec_key, s.spec_value FROM product_specs s
                JOIN products p ON s.product_id = p.id WHERE p.product_model = %s
                AND s.spec_key NOT IN ('Source Document', 'Extraction Method')
                ORDER BY s.spec_key""", (model,))
            qwen_specs = {k: v for k, v in qwen_cur.fetchall()}

            claude_cur.execute("""SELECT s.spec_key, s.spec_value FROM product_specs s
                JOIN products p ON s.product_id = p.id WHERE p.product_model = %s
                AND s.spec_key NOT IN ('Source Document', 'Extraction Method')
                ORDER BY s.spec_key""", (model,))
            claude_specs = {k: v for k, v in claude_cur.fetchall()}

            all_keys = set(qwen_specs.keys()) | set(claude_specs.keys())
            for key in all_keys:
                total_checks += 1
                qv = qwen_specs.get(key, "")
                cv = claude_specs.get(key, "")
                if qv == cv:
                    matches += 1
                else:
                    mismatches += 1
                    if mismatches <= 10:
                        print(f"  DIFF [{model}] {key}: Qwen=\"{qv}\" Claude=\"{cv}\"")

        print(f"\n  Spec comparison ({len(list(common)[:20])} products):")
        print(f"    Matches: {matches}/{total_checks}")
        print(f"    Mismatches: {mismatches}/{total_checks}")
        if total_checks > 0:
            print(f"    Agreement: {matches/total_checks*100:.1f}%")

    qwen_conn.close()
    claude_conn.close()


def process_from_ocr_db(filename="LK - MCB.pdf"):
    """Use existing OCR data from lk_catalog DB, run Claude CLI for extraction only.

    This is the fair comparison: same OCR input, different LLM for extraction.
    """
    import sys
    sys.path.insert(0, os.path.expanduser("~/mcb_test/ocr_complete"))
    from pipeline import storage

    print(f"\n{'='*60}")
    print(f"CLAUDE EXTRACTION (from existing OCR): {filename}")
    print(f"{'='*60}")

    t_start = time.time()

    # Get OCR'd tables from lk_catalog
    conn = storage._get_db()
    cur = conn.cursor()
    cur.execute("""SELECT t.headers, t.rows
        FROM ocr_tables t JOIN ocr_runs r ON t.run_id = r.id
        WHERE r.filename = %s ORDER BY t.table_index""", (filename,))
    tables = cur.fetchall()
    conn.close()

    if not tables:
        print(f"  No OCR tables found for {filename}")
        return

    print(f"  Found {len(tables)} OCR'd tables")

    # Format tables as text and send to Claude for extraction
    all_products = []
    for tidx, (headers, rows) in enumerate(tables):
        if not headers or not rows or len(rows) < 2:
            continue

        # Format table
        lines = [f"Table {tidx+1} from {filename}"]
        lines.append("Columns: " + " | ".join(str(h) for h in headers))
        lines.append("---")
        for row in rows[:60]:
            cells = [str(row[i]) if i < len(row) else "" for i in range(len(headers))]
            lines.append(" | ".join(cells))
        table_text = "\n".join(lines)

        prompt = f"""{EXTRACTION_PROMPT}

{table_text}

Extract all products as a JSON array:"""

        print(f"  [Claude] Table {tidx+1} ({len(rows)} rows)...")
        t0 = time.time()
        raw = _call_claude(prompt)
        dt = time.time() - t0

        if raw:
            products = _parse_json(raw)
            for p in products:
                p["_source_file"] = filename
            all_products.extend(products)
            print(f"  [Claude] Table {tidx+1}: {len(products)} products, {dt:.1f}s")
        else:
            print(f"  [Claude] Table {tidx+1}: no response, {dt:.1f}s")

    print(f"\n  Total extracted: {len(all_products)} products")

    # Save to Claude DB
    inserted, skipped = save_products(all_products)
    total_time = time.time() - t_start

    stats = {
        "filename": filename,
        "pipeline": "DeepSeek-OCR + Claude CLI extraction",
        "total_time": total_time,
        "tables_processed": len(tables),
        "products_extracted": len(all_products),
        "products_inserted": inserted,
        "products_skipped": skipped,
    }
    print(f"\n{'='*60}")
    print(f"DONE: {stats}")
    return stats


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "benchmark":
        benchmark()
    elif len(sys.argv) > 1 and sys.argv[1] == "from-ocr":
        fname = sys.argv[2] if len(sys.argv) > 2 else "LK - MCB.pdf"
        process_from_ocr_db(fname)
        benchmark()
    elif len(sys.argv) > 1:
        process_catalog(sys.argv[1])
        benchmark()
    else:
        # Default: use existing OCR data + Claude extraction
        process_from_ocr_db("LK - MCB.pdf")
        benchmark()
