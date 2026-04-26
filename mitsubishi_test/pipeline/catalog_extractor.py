"""Product extraction pipeline — direct vLLM extraction with extraction levels.

No ocr_complete dependency. Uses:
- pipeline/pdf_extractor.py for text/table extraction (CPU-only)
- vLLM API directly for structured product extraction
- pipeline/db.py for database operations
"""

from __future__ import annotations

import glob
import json
import os
import re

import requests

from .db import get_db, get_meta_db as _get_meta_db, is_file_processed, mark_file_processed

VLLM_URL = os.environ.get("VLLM_URL", "http://localhost:8201/v1/chat/completions")
VLLM_MODEL = os.environ.get("VLLM_MODEL", "Qwen/Qwen3.5-27B-FP8")

# ── Extraction Level Prompts ──────────────────────────

_MRP_RULE = """- IMPORTANT: Any column containing price, cost, or monetary value (such as "L.P.", "L.P.(`)", "MRP", "Price", "Rate", "Unit Price", "List Price", "Tariff", "Cost", or any column with currency symbols like ₹, $, €, Rs) MUST be mapped to the key "mrp" in specs. Always use "mrp" as the key — never "price", "L.P.", "list_price", or other variants."""

_MODEL_RULE = """- PRODUCT MODEL: The "product_model" field MUST be the most specific unique identifier for the product. Use this priority:
  1. "Cat. No." / "Cat No." / "Catalog Number" column value (e.g. CS90232, DBSPN004CBS)
  2. "Order code" / "Ordering code" column value (e.g. 1SDA067000R1, 1SYS273012R0984)
  3. "Type code" / "Type" column value (e.g. MNX 9-2P, SB203 M-C0.5)
  4. Product/model name from column header (e.g. Conceptpower DPA 500)
  If BOTH a Cat. No./Order code AND a Type are present, use Cat. No./Order code as product_model and put Type in specs as "type_code".
- CRITICAL: EVERY ROW in a product listing table is a SEPARATE product. If a table has columns like "Size | Rating | Type | Cat. No. | MRP" with 20 rows, extract 20 individual products — NOT one grouped product. Each row = one product with its own product_model, product_name, and specs."""

_COMPARISON_TABLE_RULE = """- COMPARISON/SPECIFICATION TABLES: If the table compares multiple products side-by-side (e.g. columns are product names like "Conceptpower DPA 500" and "MegaFlex DPA", rows are specs like "UPS frame rated power"), extract EACH column as a separate product:
  - "product_model" = the column header (product/model name)
  - "product_name" = brand + column header
  - "category" = infer from context (e.g. UPS, Meter, Controller, Drive, etc.)
  - "specs" = all row values for that column as key-value pairs (row header = spec_key, cell value = spec_value)
- Only the product_name, product_model, and category go in the product. ALL other data (ratings, power, topology, wiring, dimensions, etc.) goes into "specs" as key-value pairs."""

_CATEGORY_RULE = """- "category": one of MCB, MCCB, RCCB, RCBO, ACB, Isolator, Contactor, Relay, Switch, Fuse, SPD, Starter, UPS, Meter, Drive, Controller, Sensor, Enclosure, Cable, Busbar, or Other"""

EXTRACTION_LEVELS = {
    "basic": {
        "label": "Basic — Names, models & categories only",
        "description": "Fast scan: extracts product name, model/part number, and category. No specs.",
        "prompt": f"""You are an electrical product data extractor. Given a table from an electrical catalog PDF, extract every product into structured JSON.

For each product, extract ONLY:
- "product_name": a clear product name (include brand if known)
- "product_model": the most specific unique identifier (see MODEL rules below)
{_CATEGORY_RULE}

Rules:
- Extract every distinct product or product variant
{_MODEL_RULE}
{_COMPARISON_TABLE_RULE}
- Return only a JSON array of objects with keys: product_name, product_model, category
""",
    },
    "standard": {
        "label": "Standard — Core specs (rating, poles, voltage, price)",
        "description": "Balanced extraction: product info plus key electrical specifications and pricing.",
        "prompt": f"""You are an electrical product data extractor. Given a table from an electrical catalog PDF, extract every product into structured JSON.

For each product, extract:
- "product_name": a clear product name like "MCB SP 16A C-Curve" and include brand if known
- "product_model": the most specific unique identifier (see MODEL rules below)
{_CATEGORY_RULE}
- "specs": a dictionary with key specifications as key-value pairs (rating, poles, voltage, breaking_capacity, curve_type, mrp, etc.)

Rules:
- Extract every distinct product or product variant
- If a column header or row label applies to all products, include it in each product's specs
{_MODEL_RULE}
{_MRP_RULE}
{_COMPARISON_TABLE_RULE}
- If data is unclear, keep what is explicit and skip what is not
- Return only a JSON array
""",
    },
    "detailed": {
        "label": "Detailed — All available specifications",
        "description": "Thorough extraction: every specification, dimension, standard, and technical field.",
        "prompt": f"""You are an electrical product data extractor. Given a table from an electrical catalog PDF, extract every product into structured JSON.

For each product, extract:
- "product_name": a clear product name like "MCB SP 16A C-Curve" and include brand if known
- "product_model": the most specific unique identifier (see MODEL rules below)
{_CATEGORY_RULE}
- "specs": a dictionary of ALL specifications as key-value pairs. Include every data field from the table: rating, poles, voltage, mrp, curve type, breaking capacity, dimensions, weight, standards, compliance, frame size, trip type, mounting type, series, HSN code, frequency, power, topology, wiring, efficiency, and any other technical fields present

Rules:
- Extract every distinct product or product variant
- If a column header or row label applies to all products, include it in each product's specs
{_MODEL_RULE}
{_MRP_RULE}
{_COMPARISON_TABLE_RULE}
- Include EVERY available data field — do not skip any column/row that has useful data
- If data is unclear, keep what is explicit and skip what is not
- Return only a JSON array
""",
    },
}


# ── Brand detection ──────────────────────────────────

def _auto_brand(filename: str, fallback: str | None = None) -> str | None:
    if fallback:
        return fallback
    lower_name = filename.lower()
    brands = {
        "schneider": "Schneider", "siemens": "Siemens", "abb": "ABB",
        "legrand": "Legrand", "hager": "Hager", "mitsubishi": "Mitsubishi",
        "lauritz": "Lauritz Knudsen", "lk": "Lauritz Knudsen",
        "havells": "Havells", "polycab": "Polycab", "l&t": "L&T",
    }
    for key, value in brands.items():
        if key in lower_name:
            return value
    return None


def _infer_brand_from_text(text: str) -> str | None:
    lower_text = text.lower()
    brands = {
        "schneider": "Schneider", "siemens": "Siemens", "abb": "ABB",
        "legrand": "Legrand", "hager": "Hager", "mitsubishi": "Mitsubishi",
        "lauritz knudsen": "Lauritz Knudsen", "havells": "Havells",
        "polycab": "Polycab", "l&t": "L&T",
    }
    for key, value in brands.items():
        if key in lower_text:
            return value
    return None


# ── LLM response parsing ────────────────────────────

def _parse_json_from_llm(content: str) -> list:
    """Extract JSON array from LLM response, handling markdown fences and extra text."""
    content = content.strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    match = re.search(r'```(?:json)?\s*\n?(.*?)```', content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    start = content.find('[')
    end = content.rfind(']')
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(content[start:end + 1])
        except json.JSONDecodeError:
            pass

    return []


# ── Core extraction ──────────────────────────────────

def _format_table(table: dict, tidx: int) -> str:
    """Format a single table for LLM prompt, including page context if available."""
    headers = table.get("headers", [])
    rows = table.get("rows", [])
    ctx = table.get("page_context", "")

    text = f"Table {tidx + 1} (page {table.get('page', '?')}):\n"
    if ctx:
        text += f"Page context: {ctx}\n"
    text += "Columns: " + " | ".join(str(h) for h in headers) + "\n---\n"
    for row in rows[:50]:
        cells = [str(row[i]) if i < len(row) else "" for i in range(len(headers))]
        text += " | ".join(cells) + "\n"
    if len(rows) > 50:
        text += f"... ({len(rows) - 50} more rows)\n"
    return text


def _estimate_tokens(text: str) -> int:
    """Rough token estimate (~4 chars per token)."""
    return len(text) // 4


def _merge_page_tables(tables: list[dict], ocr_pages: list[dict] | None = None) -> list[dict]:
    """Merge related tables from the same page into combined tables.

    Many catalogs (e.g. ABB) split product info across multiple tables on a page:
    - Table 1: Frame, Rating, Poles
    - Table 2: Ordering code, Price (N variant)
    - Table 3: Ordering code, Price (S variant)

    This function groups tables by page and merges them so the LLM sees
    the full context (product name + specs + ordering codes + prices).
    Also includes page-level context (headings, descriptions) from OCR output.
    """
    # Group tables by page
    by_page = {}
    for t in tables:
        page = t.get("page", 0)
        by_page.setdefault(page, []).append(t)

    # Build page context lookup from OCR pages
    page_context = {}
    if ocr_pages:
        for p in ocr_pages:
            text = p.get("text", "")
            # Extract non-table text (headings, descriptions)
            import re as _re
            clean = _re.sub(r'<table>.*?</table>', '', text, flags=_re.DOTALL)
            clean = _re.sub(r'<\|[^|]+\|>[^<]*', '', clean)  # Remove OCR tags
            clean = _re.sub(r'\!\[.*?\]\(.*?\)', '', clean)   # Remove image refs
            lines = [l.strip() for l in clean.split('\n') if l.strip() and len(l.strip()) > 3]
            if lines:
                page_context[p["page"]] = " | ".join(lines[:5])

    merged = []
    for page_num in sorted(by_page.keys()):
        page_tables = by_page[page_num]

        # If only 1 table on page, keep as-is but add context
        if len(page_tables) == 1:
            t = page_tables[0].copy()
            ctx = page_context.get(page_num, "")
            if ctx:
                t["page_context"] = ctx
            merged.append(t)
            continue

        # Check if tables are related (same row count = likely split columns)
        row_counts = [len(t.get("rows", [])) for t in page_tables]

        # Find the "specs" table (has Frame/Rating/Poles/Type columns)
        specs_table = None
        order_tables = []
        other_tables = []

        for t in page_tables:
            h_lower = " ".join(h.lower() for h in t.get("headers", []))
            if any(kw in h_lower for kw in ["frame", "rating", "in (a)", "poles", "type", "size"]):
                specs_table = t
            elif any(kw in h_lower for kw in ["ordering", "order", "code", "cat. no", "cat no"]):
                order_tables.append(t)
            else:
                other_tables.append(t)

        # If we have a specs table + ordering code tables, merge them
        if specs_table and order_tables:
            spec_headers = specs_table.get("headers", [])
            spec_rows = specs_table.get("rows", [])

            # For each ordering table, combine with spec rows
            for ot_idx, ot in enumerate(order_tables):
                ot_headers = ot.get("headers", [])
                ot_rows = ot.get("rows", [])

                # Determine variant label (N/S/H/etc.) from header or position
                variant = ""
                for h in ot_headers:
                    if "l.p" in h.lower() or "ordering" in h.lower():
                        continue
                    if h.strip():
                        variant = h.strip()
                        break
                if not variant:
                    variant = f"Variant_{ot_idx+1}"

                # Merge: combine spec columns + ordering columns per row
                combined_headers = spec_headers + [f"Ordering code ({variant})", f"L.P. ({variant})"]
                combined_rows = []
                for i in range(max(len(spec_rows), len(ot_rows))):
                    spec_row = spec_rows[i] if i < len(spec_rows) else [""] * len(spec_headers)
                    ot_row = ot_rows[i] if i < len(ot_rows) else [""] * len(ot_headers)
                    combined_rows.append(list(spec_row) + list(ot_row[:2]))

                ctx = page_context.get(page_num, "")
                merged.append({
                    "headers": combined_headers,
                    "rows": combined_rows,
                    "page": page_num,
                    "table_index": 0,
                    "page_context": ctx,
                    "merged": True,
                })

            # Also include other tables from this page
            for t in other_tables:
                t_copy = t.copy()
                t_copy["page_context"] = page_context.get(page_num, "")
                merged.append(t_copy)
        else:
            # Can't merge — keep all tables with context
            for t in page_tables:
                t_copy = t.copy()
                t_copy["page_context"] = page_context.get(page_num, "")
                merged.append(t_copy)

    return merged


def _call_vllm(prompt: str, level_prompt: str, brand_hint: str | None,
               categories: list[str] | None = None) -> list:
    """Single vLLM call, returns list of product dicts."""
    brand_line = f"Brand: {brand_hint}\n" if brand_hint else ""
    category_line = ""
    if categories:
        cat_str = ", ".join(categories)
        category_line = (
            f"\nIMPORTANT: Extract ONLY products belonging to these categories: {cat_str}. "
            f"Skip all rows that do not match these categories. "
            f"If no rows match, return an empty array [].\n"
        )
    full_prompt = f"""{level_prompt}

{brand_line}{category_line}{prompt}

Extract all products as a JSON array:"""

    try:
        resp = requests.post(
            VLLM_URL,
            json={
                "model": VLLM_MODEL,
                "messages": [
                    {"role": "system", "content": "You extract structured product data from electrical catalogs. Output valid JSON only."},
                    {"role": "user", "content": full_prompt},
                ],
                "max_tokens": 16384,
                "temperature": 0.05,
                "chat_template_kwargs": {"enable_thinking": False},
            },
            timeout=None,  # No timeout — let vLLM take as long as needed
        )
        if resp.status_code != 200:
            print(f"  [Extract] vLLM returned {resp.status_code}: {resp.text[:200]}")
            return []

        content = resp.json()["choices"][0]["message"]["content"]
        return _parse_json_from_llm(content)

    except Exception as e:
        print(f"  [Extract] vLLM error: {e}")
        return []


def _items_to_products(items: list, brand_hint: str | None) -> list:
    """Convert raw LLM items to product dicts."""
    products = []
    for item in items:
        model = (item.get("product_model") or item.get("model")
                 or item.get("order_code") or item.get("ordering_code")
                 or item.get("cat_no") or item.get("catalog_number"))
        if not model:
            continue
        products.append({
            "product_name": item.get("product_name") or item.get("name", ""),
            "product_model": str(model).strip(),
            "category": item.get("category", "Other"),
            "brand": brand_hint or item.get("brand", ""),
            "specs": item.get("specs", {}),
        })
    return products


MAX_BATCH_TOKENS = 2000  # One table per batch — prevents LLM from grouping rows

# Global progress callback — set by UI to stream updates
_extract_progress_callback = None


def set_progress_callback(cb):
    global _extract_progress_callback
    _extract_progress_callback = cb


def extract_from_tables(tables, filename, brand_hint=None, level="detailed",
                        categories=None, ocr_pages=None):
    """Extract products from tables by batching multiple tables into vLLM calls.

    Args:
        categories: list of category strings to extract (e.g. ["MCB", "MCCB"]).
                    If None or empty, extracts all categories.
        ocr_pages: list of {"page": int, "text": str} from DeepSeek-OCR-2 output.
                   Used to add page context and merge related tables.
    """
    brand_hint = _auto_brand(filename, brand_hint)
    level_prompt = EXTRACTION_LEVELS.get(level, EXTRACTION_LEVELS["detailed"])["prompt"]
    all_products = []

    # Filter to tables with actual content
    valid_tables = []
    for t in tables:
        headers = t.get("headers", [])
        rows = t.get("rows", [])
        if headers and rows:
            valid_tables.append(t)

    # Merge related tables from the same page (e.g. specs + ordering codes)
    valid_tables = _merge_page_tables(valid_tables, ocr_pages)

    print(f"  [Extract] {len(valid_tables)} tables (after merge) from {filename}")

    # Build batches — group tables until we hit token limit
    batches = []
    current_batch = []
    current_tokens = 0

    for tidx, table in enumerate(valid_tables):
        table_text = _format_table(table, tidx)
        table_tokens = _estimate_tokens(table_text)

        # If single table exceeds limit, send it alone
        if table_tokens > MAX_BATCH_TOKENS:
            if current_batch:
                batches.append(current_batch)
                current_batch = []
                current_tokens = 0
            batches.append([(tidx, table, table_text)])
            continue

        # If adding this table would exceed limit, start new batch
        if current_tokens + table_tokens > MAX_BATCH_TOKENS and current_batch:
            batches.append(current_batch)
            current_batch = []
            current_tokens = 0

        current_batch.append((tidx, table, table_text))
        current_tokens += table_tokens

    if current_batch:
        batches.append(current_batch)

    print(f"  [Extract] Batched into {len(batches)} vLLM calls")

    # Process batches concurrently (up to 3 parallel vLLM calls)
    from concurrent.futures import ThreadPoolExecutor, as_completed

    if categories:
        print(f"  [Extract] Category filter: {categories}")

    def _process_batch(batch_idx, batch):
        combined_text = f"Source document: {filename}\n\n"
        for _tidx, _table, table_text in batch:
            combined_text += table_text + "\n"
        items = _call_vllm(combined_text, level_prompt, brand_hint, categories=categories)
        return batch_idx, _items_to_products(items, brand_hint)

    max_workers = min(3, len(batches))
    completed = 0

    # If a progress callback is provided, use it
    progress_cb = _extract_progress_callback

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_process_batch, i, b): i for i, b in enumerate(batches)}
        for future in as_completed(futures):
            batch_idx, products = future.result()
            all_products.extend(products)
            completed += 1
            msg = f"Batch {completed}/{len(batches)} done — {len(products)} products (total: {len(all_products)})"
            print(f"  [Extract] {msg}")
            if progress_cb:
                progress_cb(completed, len(batches), len(all_products))

    # Fill missing brands
    for product in all_products:
        if not product.get("brand") or product.get("brand") == "Unknown":
            inferred = brand_hint or _infer_brand_from_text(
                f"{product.get('product_name', '')} {filename}"
            )
            if inferred:
                product["brand"] = inferred

    # Filter specs by extraction level
    if level == "basic":
        for product in all_products:
            product.pop("specs", None)
    elif level == "standard":
        keep_keys = {"rating", "poles", "voltage", "breaking_capacity", "curve_type",
                     "mrp", "price", "MRP", "Price"}
        for product in all_products:
            specs = product.get("specs", {})
            product["specs"] = {
                k: v for k, v in specs.items()
                if k.lower().replace(" ", "_") in {s.lower() for s in keep_keys} or k in keep_keys
            }

    print(f"  [Extract] Total: {len(all_products)} products from {filename}")
    return all_products or []


# ── Save products to DB ──────────────────────────────

_MRP_KEYS = {"mrp", "MRP", "price", "Price", "L.P.", "L.P.(`)", "l.p.", "lp",
             "list_price", "List Price", "unit_price", "Unit Price", "rate", "Rate",
             "tariff", "Tariff", "cost", "Cost"}


def _extract_mrp(specs: dict) -> str | None:
    """Find the MRP/price value from specs, regardless of what key the LLM used."""
    for key in _MRP_KEYS:
        val = specs.get(key)
        if val:
            if isinstance(val, dict):
                val = val.get("value", str(val))
            return str(val).strip()
    return None


def _normalize_spec_key(key: str) -> str:
    """Normalize a spec key — maps price variants to 'mrp'."""
    if key.lower().replace(" ", "_").replace(".", "") in {
        k.lower().replace(" ", "_").replace(".", "") for k in _MRP_KEYS
    }:
        return "mrp"
    return key


def _flatten_spec_value(value) -> str:
    """Flatten a spec value (dict/list) to a string."""
    if isinstance(value, dict):
        return value.get("value", json.dumps(value))
    if isinstance(value, list):
        return ", ".join(str(x) for x in value)
    return str(value).strip() if value is not None else ""


def save_products(products):
    """Save extracted products to mitsubishi_test DB. No duplicates.

    - If product_model already exists: only fills in empty fields (never overwrites existing data)
    - If a spec key already exists for a product: skips it (keeps original)
    - Only genuinely new products and new specs are added

    Returns (new_count, existing_count).
    """
    conn = get_db()
    new_count = 0
    existing_count = 0

    try:
        with conn.cursor() as cur:
            for product in products:
                model = product.get("product_model", "").strip()
                name = product.get("product_name", "").strip()
                if not model:
                    existing_count += 1
                    continue

                specs = product.get("specs", {})
                mrp = _extract_mrp(specs)

                # Check if product already exists
                cur.execute("SELECT id, product_name, category, brand, mrp FROM products WHERE product_model = %s", (model,))
                existing = cur.fetchone()

                if existing:
                    # Product exists — only fill in NULL/empty fields, never overwrite
                    pid, ex_name, ex_cat, ex_brand, ex_mrp = existing
                    updates = []
                    params = []
                    if not ex_name and name:
                        updates.append("product_name = %s")
                        params.append(name)
                    if not ex_cat and product.get("category"):
                        updates.append("category = %s")
                        params.append(product.get("category"))
                    if not ex_brand and product.get("brand"):
                        updates.append("brand = %s")
                        params.append(product.get("brand"))
                    if not ex_mrp and mrp:
                        updates.append("mrp = %s")
                        params.append(mrp)
                    if not existing and product.get("description"):
                        updates.append("description = %s")
                        params.append(product.get("description"))

                    if updates:
                        params.append(pid)
                        cur.execute(f"UPDATE products SET {', '.join(updates)} WHERE id = %s", params)

                    existing_count += 1
                else:
                    # New product — insert
                    try:
                        cur.execute(
                            """
                            INSERT INTO products (product_name, product_model, category, brand, mrp, description, catalogue_name)
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            RETURNING id
                            """,
                            (name, model, product.get("category"), product.get("brand"),
                             mrp, product.get("description"), product.get("catalogue_name")),
                        )
                        pid = cur.fetchone()[0]
                        new_count += 1
                    except Exception as e:
                        print(f"  [Save] Insert failed {model}: {e}")
                        existing_count += 1
                        continue

                # Save specs — only add NEW spec keys, never overwrite existing
                for key, value in specs.items():
                    if not key or value is None:
                        continue
                    key = _normalize_spec_key(key)
                    value = _flatten_spec_value(value)
                    if not value:
                        continue

                    # Skip if this spec already exists for this product
                    cur.execute(
                        "SELECT 1 FROM product_specs WHERE product_id = %s AND spec_key = %s",
                        (pid, key),
                    )
                    if cur.fetchone():
                        continue  # Spec already exists, don't overwrite

                    cur.execute(
                        """
                        INSERT INTO product_specs (product_id, product_model, spec_key, spec_value, category)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (pid, model, key, value, product.get("category")),
                    )

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  [Save] DB error: {e}")
        raise
    finally:
        conn.close()

    print(f"  [Save] {new_count} new, {existing_count} existing (no duplicates)")
    return new_count, existing_count


# ── High-level pipeline functions ────────────────────

def process_catalog(file_path, brand_hint=None, level="detailed", categories=None):
    """Full pipeline: PDF -> tables -> LLM extract -> save to DB."""
    from .pdf_extractor import process as pdf_process
    from .image_extractor import extract_images_from_pdf, link_images_to_products

    abs_path = os.path.abspath(file_path)
    filename = os.path.basename(abs_path)

    already_done, prev_record = is_file_processed(abs_path)
    if already_done:
        return {
            "filename": filename,
            "already_processed": True,
            "previous": prev_record,
        }

    text, tables, dt, method, file_type, num_pages = pdf_process(abs_path)

    products = extract_from_tables(tables, filename, brand_hint, level=level, categories=categories)

    try:
        inserted, skipped = save_products(products)
    except Exception as e:
        print(f"  [Pipeline] Save failed for {filename}, file NOT marked as processed: {e}")
        return {
            "filename": filename,
            "extracted": len(products),
            "inserted": 0,
            "skipped": 0,
            "images_linked": 0,
            "method": method,
            "time": dt,
            "error": str(e),
        }

    images = extract_images_from_pdf(abs_path)
    linked = link_images_to_products(images, filename, pdf_path=abs_path)

    mark_file_processed(abs_path, filename,
                        products_inserted=inserted,
                        products_skipped=skipped,
                        images_linked=linked)

    return {
        "filename": filename,
        "extracted": len(products),
        "inserted": inserted,
        "skipped": skipped,
        "images_linked": linked,
        "method": method,
        "time": dt,
    }


def process_catalog_from_tables(tables, filename, brand_hint=None, level="detailed"):
    """Extract and save products from pre-extracted tables."""
    brand = _auto_brand(filename, brand_hint)
    products = extract_from_tables(tables, filename, brand, level=level)
    if not products:
        return {"extracted": 0, "inserted": 0, "skipped": 0}

    inserted, skipped = save_products(products)
    return {"extracted": len(products), "inserted": inserted, "skipped": skipped}


def batch_process_folder(folder_path, brand_hint=None, level="detailed"):
    """Process all PDFs in a folder."""
    from .image_extractor import extract_images_from_pdf, link_images_to_products
    from .pdf_extractor import process as pdf_process

    folder_path = os.path.abspath(folder_path)
    pdf_files = sorted(glob.glob(os.path.join(folder_path, "*.pdf")))
    if not pdf_files:
        pdf_files = sorted(glob.glob(os.path.join(folder_path, "**/*.pdf"), recursive=True))
    if not pdf_files:
        return {"error": "No PDFs found", "folder": folder_path}

    results = []
    total_inserted = total_skipped = total_images = 0
    processed = skipped_files = failed_files = 0

    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        already_done, prev_record = is_file_processed(pdf_path)
        if already_done:
            skipped_files += 1
            results.append({"filename": filename, "status": "skipped"})
            continue

        try:
            stats = process_catalog(pdf_path, brand_hint=_auto_brand(filename, brand_hint), level=level)
            total_inserted += stats.get("inserted", 0)
            total_skipped += stats.get("skipped", 0)
            total_images += stats.get("images_linked", 0)
            processed += 1
            results.append({"filename": filename, "status": "processed", **stats})
        except Exception as exc:
            failed_files += 1
            results.append({"filename": filename, "status": "failed", "error": str(exc)})

    return {
        "folder": folder_path,
        "total_pdfs": len(pdf_files),
        "processed": processed,
        "skipped_files": skipped_files,
        "failed_files": failed_files,
        "total_inserted": total_inserted,
        "total_skipped": total_skipped,
        "total_images_linked": total_images,
        "results": results,
    }
