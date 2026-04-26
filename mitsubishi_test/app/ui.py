"""MCB Catalog Manager — Gradio UI for electrical product catalog pipeline.

Tabs:
1. Upload & Extract — upload PDF, select extraction level, preview & approve
2. Product Browser — browse/search/filter products in DB
3. Review & Edit — edit products, additions go to mitsubishi_user_data
4. Ask AI — chatbot for product queries
5. Export — download DB as CSV/Excel
"""

import os
import sys
import json
import tempfile
import re

import gradio as gr
import pandas as pd
import psycopg2
import requests

# Add project root to path
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

from pipeline.db import (
    get_db as _get_db, get_user_db as _get_user_db,
    is_file_processed, mark_file_processed,
    save_user_product, save_user_specs, get_user_products, get_user_product_detail,
)
from pipeline.catalog_extractor import EXTRACTION_LEVELS, set_progress_callback

VLLM_URL = os.environ.get("VLLM_URL", "http://localhost:8201/v1/chat/completions")
MODEL = os.environ.get("VLLM_MODEL", "Qwen/Qwen3.5-27B-FP8")

CATEGORY_CHOICES = [
    "All", "MCB", "MCCB", "RCCB", "RCBO", "ACB", "Isolator",
    "Contactor", "Relay", "Switch", "Fuse", "SPD", "Starter", "Other",
]

# In-memory preview store (session-level)
_preview_cache = {}


# ==============================================================
# Tab 1: Upload & Extract (with levels + category filter + preview)
# ==============================================================

def _run_extract(file_path):
    """Extract text and tables from PDF using CPU-only pipeline."""
    from pipeline.pdf_extractor import process as pdf_process
    text, tables, dt, method, file_type, num_pages = pdf_process(file_path)
    return {
        "text": text, "tables": tables, "dt": dt,
        "method": method, "file_type": file_type, "num_pages": num_pages,
    }


def _level_choice_to_key(choice):
    if not choice:
        return "detailed"
    lower = choice.lower()
    if "basic" in lower:
        return "basic"
    if "standard" in lower:
        return "standard"
    return "detailed"


def _build_preview_df(products, level_key):
    """Build an editable preview DataFrame from products list."""
    rows = []
    for p in products:
        specs = p.get("specs") or {}
        row = {
            "Product Name": p.get("product_name", ""),
            "Model": p.get("product_model", ""),
            "Category": p.get("category", ""),
            "Brand": p.get("brand", ""),
            "MRP": _flat_spec(specs, "mrp", "MRP", "price", "Price"),
        }
        if level_key in ("standard", "detailed"):
            row["Rating"] = _flat_spec(specs, "rating", "Rating")
            row["Poles"] = _flat_spec(specs, "poles", "Poles")
            row["Voltage"] = _flat_spec(specs, "voltage", "Voltage")
        if level_key == "detailed":
            row["Breaking Cap."] = _flat_spec(specs, "breaking_capacity", "Breaking Capacity")
            row["Curve Type"] = _flat_spec(specs, "curve_type", "Curve Type")
            shown = {"mrp", "MRP", "price", "Price", "rating", "Rating", "poles", "Poles",
                     "voltage", "Voltage", "breaking_capacity", "Breaking Capacity",
                     "curve_type", "Curve Type"}
            extra = {k: _flatten_value(v) for k, v in specs.items() if k not in shown}
            row["Other Specs"] = "; ".join(f"{k}={v}" for k, v in list(extra.items())[:6])
        rows.append(row)
    return pd.DataFrame(rows)


def _flatten_value(v):
    """Flatten a spec value that might be a dict or list."""
    if isinstance(v, dict):
        return v.get("value", str(v))
    if isinstance(v, list):
        return ", ".join(str(x) for x in v)
    return str(v) if v is not None else ""


def _flat_spec(specs, *keys):
    """Get first non-empty spec value from multiple possible keys, flattening dicts."""
    for k in keys:
        v = specs.get(k)
        if v:
            return _flatten_value(v)
    return ""


def _render_pdf_previews(file_path, max_pages=6, dpi=120):
    """Render first N pages of a PDF as images for preview gallery."""
    import fitz
    doc = fitz.open(file_path)
    pages_to_render = min(len(doc), max_pages)
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    preview_images = []

    for i in range(pages_to_render):
        pix = doc[i].get_pixmap(matrix=mat)
        img_path = os.path.join(
            tempfile.gettempdir(),
            f"pdf_preview_{os.path.basename(file_path)}_{i+1}.png"
        )
        pix.save(img_path)
        preview_images.append((img_path, f"Page {i+1}"))

    doc.close()
    return preview_images


def process_and_preview(file_path, level, category_filter, page_range):
    """Upload PDF -> extract -> LLM -> auto-save -> return preview with page images."""
    if not file_path:
        return "Upload a PDF to begin.", None, None

    fname = os.path.basename(file_path)
    level_key = _level_choice_to_key(level)

    yield f"**Processing:** {fname}\n\n_Step 1/2: Extracting text & tables..._", None, None

    try:
        start_page, end_page = 1, None
        if page_range and page_range.strip():
            pr = page_range.strip()
            if "-" in pr:
                parts = pr.split("-", 1)
                start_page = int(parts[0].strip())
                end_page = int(parts[1].strip())
            else:
                start_page = int(pr)
                end_page = start_page

        if end_page:
            from pipeline.pdf_extractor import process_page_range
            text, tables, dt, method, file_type, num_pages = process_page_range(
                file_path, start_page, end_page)
        else:
            ocr_result = _run_extract(file_path)
            text = ocr_result["text"]
            tables = ocr_result["tables"]
            dt = ocr_result["dt"]
            method = ocr_result["method"]
    except ValueError:
        yield "**Invalid page range.** Use format: `1-50` or leave empty for all pages.", None, None
        return
    except Exception as e:
        yield f"**Extraction Failed:** {e}", None, None
        return

    # Render PDF page previews
    page_previews = _render_pdf_previews(file_path, max_pages=8, dpi=120)

    if not tables:
        yield f"**No tables found** in {fname}. Only raw text ({len(text)} chars).", None, page_previews
        return

    gpu_note = " (zero GPU)" if "CPU" in method else ""

    # Build category list for LLM-level filtering
    cat_list = None
    if category_filter:
        if isinstance(category_filter, list):
            cats = [c for c in category_filter if c and c != "All"]
            if cats:
                cat_list = cats
        elif isinstance(category_filter, str) and category_filter != "All":
            cat_list = [category_filter]

    cat_label = ", ".join(cat_list) if cat_list else "All"
    yield (
        f"**Tables Found:** {len(tables)} tables, {dt:.1f}s{gpu_note}\n\n"
        f"_Step 2/2: Extracting **{cat_label}** products at **{level_key}** level..._"
    ), None, page_previews

    try:
        from pipeline.catalog_extractor import extract_from_tables

        set_progress_callback(lambda c, t, n: None)
        products = extract_from_tables(tables, fname, level=level_key, categories=cat_list)
        set_progress_callback(None)
    except Exception as e:
        set_progress_callback(None)
        yield f"**Product Extraction Failed:** {e}", None, page_previews
        return

    if not products:
        yield f"**No {cat_label} products extracted** from {len(tables)} tables.", None, page_previews
        return

    # Check if this file was already processed
    already_done, prev_record = is_file_processed(file_path)

    # Auto-save extracted products to mitsubishi_test (main DB)
    # Only new products and new specs are added — no duplicates
    from pipeline.catalog_extractor import save_products
    from pipeline.image_extractor import extract_images_from_pdf, link_images_to_products

    new_count, existing_count = save_products(products)
    images = extract_images_from_pdf(file_path)
    linked = link_images_to_products(images, fname, pdf_path=file_path)
    mark_file_processed(file_path, fname,
                        products_inserted=new_count,
                        products_skipped=existing_count,
                        images_linked=linked)

    reprocess_note = ""
    if already_done:
        reprocess_note = f"\n- **Re-processed:** only new data added (file was previously processed on {prev_record.get('processed_at', 'N/A')})"

    df = _build_preview_df(products, level_key)

    # Cache for user edits
    _preview_cache[fname] = {
        "products": products,
        "file_path": file_path,
        "filename": fname,
        "tables": tables,
        "text": text,
        "level": level_key,
    }

    yield (
        f"**Saved to Database!** {len(products)} products extracted at **{level_key}** level.\n"
        f"- **New products:** {new_count}\n"
        f"- **Already existing (skipped):** {existing_count}\n"
        f"- **Images:** {len(images)} extracted, {linked} linked"
        f"{reprocess_note}\n\n"
        f"Review the table below. Edit cells to correct data, then click "
        f"**Save User Additions** to save your changes to User Data."
    ), df, page_previews


def save_previewed_products(filename_key):
    """Save previewed products to the main DB + extract/link images."""
    if not filename_key or filename_key not in _preview_cache:
        matches = [k for k in _preview_cache if filename_key and filename_key in k]
        if matches:
            filename_key = matches[0]
        else:
            return "No preview data found. Run extraction first."

    cached = _preview_cache[filename_key]
    products = cached["products"]
    file_path = cached["file_path"]
    fname = cached["filename"]

    try:
        from pipeline.catalog_extractor import save_products
        from pipeline.image_extractor import extract_images_from_pdf, link_images_to_products

        inserted, skipped = save_products(products)

        images = extract_images_from_pdf(file_path)
        linked = link_images_to_products(images, fname, pdf_path=file_path)

        mark_file_processed(file_path, fname,
                            products_inserted=inserted,
                            products_skipped=skipped,
                            images_linked=linked)

        return (
            f"**Saved to Main DB!**\n"
            f"- Products: {inserted} new, {skipped} skipped\n"
            f"- Images: {len(images)} extracted, {linked} linked"
        )
    except Exception as e:
        return f"**Save Failed:** {e}"


def save_user_additions(filename_key, edited_df):
    """Save user-edited preview data to mitsubishi_user_data."""
    if edited_df is None or edited_df.empty:
        return "No data to save."

    if not filename_key or filename_key not in _preview_cache:
        matches = [k for k in _preview_cache if filename_key and filename_key in k]
        if matches:
            filename_key = matches[0]

    saved = 0
    for _, row in edited_df.iterrows():
        model = str(row.get("Model", "")).strip()
        name = str(row.get("Product Name", "")).strip()
        if not model and not name:
            continue

        product_dict = {
            "product_name": name,
            "product_model": model,
            "category": str(row.get("Category", "")),
            "brand": str(row.get("Brand", "")),
            "mrp": str(row.get("MRP", "")),
        }
        user_pid = save_user_product(product_dict, change_type="preview_edit")

        spec_cols = {"Rating", "Poles", "Voltage", "Breaking Cap.", "Curve Type", "Other Specs"}
        specs = {}
        for col in spec_cols:
            val = str(row.get(col, "")).strip()
            if val and val != "nan":
                if col == "Other Specs":
                    for pair in val.split(";"):
                        if "=" in pair:
                            k, v = pair.split("=", 1)
                            specs[k.strip()] = v.strip()
                else:
                    specs[col] = val

        if specs:
            save_user_specs(user_pid, specs, change_type="preview_edit")

        saved += 1

    return f"**Saved {saved} products to User Data** (mitsubishi_user_data)"


# ==============================================================
# Tab 1b: Batch processing
# ==============================================================

def process_batch(file_paths, level, category_filter):
    """Upload multiple PDFs -> process each with selected extraction level."""
    if not file_paths:
        yield "Upload PDF files to begin.", ""
        return

    pdf_files = [f for f in file_paths if f.lower().endswith(".pdf")]
    if not pdf_files:
        yield "No PDF files found in the upload.", ""
        return

    level_key = _level_choice_to_key(level)
    # Parse category filter (may be list from multiselect or single string)
    cat_list = None
    if category_filter:
        if isinstance(category_filter, list):
            cats = [c for c in category_filter if c and c != "All"]
            if cats:
                cat_list = cats
        elif isinstance(category_filter, str) and category_filter != "All":
            cat_list = [category_filter]

    cat_label = ", ".join(cat_list) if cat_list else "All"
    total = len(pdf_files)
    yield f"**Batch:** Found {total} PDFs. Level: **{level_key}**, Categories: **{cat_label}**\n", ""

    results = []
    total_inserted = 0
    total_skipped = 0
    total_images = 0

    for idx, file_path in enumerate(pdf_files, 1):
        fname = os.path.basename(file_path)

        already_done, prev_record = is_file_processed(file_path)
        if already_done:
            results.append(f"- **{fname}**: Skipped (already processed)")
            yield f"**Batch Progress:** {idx}/{total}\n\n" + "\n".join(results) + "\n", ""
            continue

        results.append(f"- **{fname}**: Processing...")
        yield f"**Batch Progress:** {idx}/{total}\n\n" + "\n".join(results) + "\n", ""

        try:
            ocr_result = _run_extract(file_path)
            tables = ocr_result["tables"]
            dt = ocr_result["dt"]

            if not tables:
                results[-1] = f"- **{fname}**: No tables found, skipped"
                yield f"**Batch Progress:** {idx}/{total}\n\n" + "\n".join(results) + "\n", ""
                continue

            from pipeline.catalog_extractor import extract_from_tables, save_products
            from pipeline.image_extractor import extract_images_from_pdf, link_images_to_products

            products = extract_from_tables(tables, fname, level=level_key, categories=cat_list)
            inserted, skipped = save_products(products)

            images = extract_images_from_pdf(file_path)
            linked = link_images_to_products(images, fname, pdf_path=file_path)

            mark_file_processed(file_path, fname,
                                products_inserted=inserted,
                                products_skipped=skipped,
                                images_linked=linked)

            total_inserted += inserted
            total_skipped += skipped
            total_images += linked

            results[-1] = (f"- **{fname}**: {inserted} new, {skipped} skipped, "
                           f"{linked} images ({dt:.1f}s)")

        except Exception as e:
            results[-1] = f"- **{fname}**: FAILED -- {e}"

        yield f"**Batch Progress:** {idx}/{total}\n\n" + "\n".join(results) + "\n", ""

    summary = (
        f"**Batch Complete!**\n\n"
        f"- **Files:** {total}\n"
        f"- **Products:** {total_inserted} new, {total_skipped} skipped\n"
        f"- **Images:** {total_images} linked\n\n"
        f"### Per-file results:\n" + "\n".join(results)
    )
    yield summary, ""


# ==============================================================
# Tab 2: Product Browser
# ==============================================================

def get_categories():
    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT category FROM products WHERE category IS NOT NULL ORDER BY category")
        cats = ["All"] + [r[0] for r in cur.fetchall()]
        return cats
    finally:
        conn.close()


def get_brands():
    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT brand FROM products WHERE brand IS NOT NULL AND brand != '' ORDER BY brand")
        brands = ["All"] + [r[0] for r in cur.fetchall()]
        return brands
    finally:
        conn.close()


def browse_products(category, brand, search_text):
    conn = _get_db()
    try:
        cur = conn.cursor()
        conditions = []
        params = []
        if category and category != "All":
            conditions.append("p.category = %s")
            params.append(category)
        if brand and brand != "All":
            conditions.append("p.brand = %s")
            params.append(brand)
        if search_text and search_text.strip():
            conditions.append("""(p.product_name ILIKE %s OR p.product_model ILIKE %s
                OR EXISTS (SELECT 1 FROM product_specs s WHERE s.product_id = p.id
                           AND (s.spec_value ILIKE %s OR s.spec_key ILIKE %s)))""")
            params.extend([f"%{search_text}%", f"%{search_text}%", f"%{search_text}%", f"%{search_text}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        cur.execute(f"""
            SELECT p.id, p.product_name, p.product_model, p.category, p.brand,
                   p.mrp,
                   CASE WHEN p.image_url IS NOT NULL THEN 'Yes' ELSE 'No' END as has_image
            FROM products p {where}
            ORDER BY p.id
        """, params)

        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=["ID", "Product Name", "Model", "Category", "Brand", "MRP", "Image"])
    finally:
        conn.close()


def get_product_detail(product_id):
    """Fetch product details with specs and all images."""
    if not product_id or not product_id.strip():
        return "Select a product from the table above.", None, None, None

    conn = _get_db()
    try:
        cur = conn.cursor()
        lookup = product_id.strip()

        # Try as numeric ID first, then model match
        try:
            pid = int(lookup)
            cur.execute("""SELECT id, product_name, product_model, description, category, brand, mrp,
                                  image_url, alternate_image1, alternate_image2
                           FROM products WHERE id = %s""", (pid,))
        except (ValueError, TypeError):
            cur.execute("""SELECT id, product_name, product_model, description, category, brand, mrp,
                                  image_url, alternate_image1, alternate_image2
                           FROM products WHERE product_model ILIKE %s OR product_name ILIKE %s
                           ORDER BY CASE WHEN product_model = %s THEN 0 ELSE 1 END
                           LIMIT 1""", (f"%{lookup}%", f"%{lookup}%", lookup))

        row = cur.fetchone()
        if not row:
            return f"Product not found for '{lookup}'.", None, None, None

        pid, name, model, desc, cat, brand, mrp, img, alt1, alt2 = row

        cur.execute("SELECT spec_key, spec_value FROM product_specs WHERE product_id = %s ORDER BY spec_key", (pid,))
        specs = cur.fetchall()

        output = [f"## {name}", f"**Model:** {model} | **Category:** {cat} | **Brand:** {brand or 'N/A'} | **MRP:** {mrp or 'N/A'}", ""]
        if desc:
            output.append(f"_{desc}_\n")
        output.append("### Specifications\n")
        output.append("| Spec | Value |")
        output.append("|---|---|")
        for k, v in specs:
            output.append(f"| {k} | {v} |")

        img_path = img if (img and os.path.exists(img)) else None
        alt1_path = alt1 if (alt1 and os.path.exists(alt1)) else None
        alt2_path = alt2 if (alt2 and os.path.exists(alt2)) else None

        return "\n".join(output), img_path, alt1_path, alt2_path
    finally:
        conn.close()


# ==============================================================
# Tab 3: Review & Edit (saves to mitsubishi_user_data)
# ==============================================================

def get_review_products():
    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT p.id, p.product_name, p.product_model, p.category, p.brand,
                   count(s.id) as spec_count
            FROM products p LEFT JOIN product_specs s ON s.product_id = p.id
            GROUP BY p.id ORDER BY p.id DESC
        """)
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=["ID", "Name", "Model", "Category", "Brand", "Specs"])
    finally:
        conn.close()


def load_product_for_edit(product_id):
    """Load a product's details into the edit form."""
    if not product_id:
        return "", "", "", "", "", ""
    try:
        pid = int(product_id)
    except ValueError:
        return "", "", "", "", "", ""

    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT product_name, product_model, category, brand, mrp, description FROM products WHERE id = %s", (pid,))
        row = cur.fetchone()
        if not row:
            return "", "", "", "", "", ""
        return row[0] or "", row[1] or "", row[2] or "", row[3] or "", row[4] or "", row[5] or ""
    finally:
        conn.close()


def save_product_edit(product_id, new_name, new_model, new_category, new_brand, new_mrp, new_desc):
    """Save product edits to mitsubishi_user_data (not the main DB)."""
    if not product_id:
        return "Enter a product ID."
    try:
        pid = int(product_id)
    except ValueError:
        return "Invalid ID."

    product_dict = {
        "product_name": new_name,
        "product_model": new_model,
        "category": new_category,
        "brand": new_brand,
        "mrp": new_mrp,
        "description": new_desc,
    }

    user_pid = save_user_product(product_dict, original_product_id=pid, change_type="user_edit")
    return f"Saved edit for product #{pid} -> user data #{user_pid}"


def delete_product(product_id):
    if not product_id:
        return "Enter a product ID."
    try:
        pid = int(product_id)
    except ValueError:
        return "Invalid ID."

    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM products WHERE id = %s", (pid,))
        conn.commit()
        return f"Deleted product #{pid} and all its specs."
    finally:
        conn.close()


def add_spec_to_user_data(product_id, spec_key, spec_value):
    """Add a new spec — saves to mitsubishi_user_data."""
    if not product_id or not spec_key or not spec_value:
        return "Fill all fields."
    try:
        pid = int(product_id)
    except ValueError:
        return "Invalid ID."

    conn = _get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT product_name, product_model, category, brand, mrp FROM products WHERE id = %s", (pid,))
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        return f"Product #{pid} not found."

    product_dict = {
        "product_name": row[0],
        "product_model": row[1],
        "category": row[2],
        "brand": row[3],
        "mrp": row[4],
    }
    user_pid = save_user_product(product_dict, original_product_id=pid, change_type="spec_added")
    save_user_specs(user_pid, {spec_key: spec_value}, original_product_id=pid, change_type="user_added")

    return f"Added spec '{spec_key}' for product #{pid} -> user data #{user_pid}"


def get_user_data_table():
    """Fetch user-modified data for display."""
    rows = get_user_products()
    if not rows:
        return pd.DataFrame(columns=["ID", "Original ID", "Name", "Model", "Category", "Brand", "MRP", "Change Type", "Date"])
    return pd.DataFrame(rows, columns=["ID", "Original ID", "Name", "Model", "Category", "Brand", "MRP", "Change Type", "Date"])


def get_user_product_specs(user_product_id):
    """View specs for a user-data product."""
    if not user_product_id:
        return "Enter a user product ID."
    try:
        uid = int(user_product_id)
    except ValueError:
        return "Invalid ID."

    product, specs = get_user_product_detail(uid)
    if not product:
        return "User product not found."

    lines = [
        f"## {product.get('product_name', 'N/A')}",
        f"**Model:** {product.get('product_model', 'N/A')} | "
        f"**Original ID:** {product.get('original_product_id', 'N/A')} | "
        f"**Change:** {product.get('change_type', 'N/A')}",
        "",
        "| Spec | Value |",
        "|---|---|",
    ]
    for k, v in specs:
        lines.append(f"| {k} | {v} |")
    if not specs:
        lines.append("| _(no specs)_ | |")

    return "\n".join(lines)


# ==============================================================
# Tab 4: Ask AI (chatbot)
# ==============================================================

def _search_products_for_context(query, limit=10):
    conn = _get_db()
    try:
        cur = conn.cursor()
        words = [w for w in re.findall(r'\w+', query.lower()) if len(w) > 2]
        if not words:
            return "No products found."

        conditions = []
        params = []
        for w in words[:5]:
            conditions.append("""(p.product_name ILIKE %s OR p.product_model ILIKE %s
                OR EXISTS (SELECT 1 FROM product_specs s WHERE s.product_id = p.id AND s.spec_value ILIKE %s))""")
            params.extend([f"%{w}%", f"%{w}%", f"%{w}%"])

        where = " OR ".join(conditions)
        cur.execute(f"""
            SELECT DISTINCT p.id, p.product_name, p.product_model, p.category
            FROM products p
            WHERE {where}
            LIMIT %s
        """, params + [limit])
        products = cur.fetchall()

        if not products:
            return "No matching products found."

        context_parts = []
        for pid, name, model, cat in products:
            cur.execute("SELECT spec_key, spec_value FROM product_specs WHERE product_id = %s", (pid,))
            specs = {k: v for k, v in cur.fetchall()}
            spec_str = ", ".join(f"{k}: {v}" for k, v in list(specs.items())[:10])
            context_parts.append(f"[{name} ({model}, {cat})]: {spec_str}")

        return "\n".join(context_parts)
    finally:
        conn.close()


def chat(message, history):
    if not message.strip():
        yield "", history
        return

    history = history + [{"role": "user", "content": message}]
    yield "", history

    context = _search_products_for_context(message)

    system = "You are a product catalog assistant. Answer using ONLY the provided product data. Be precise with specs and prices."
    user_prompt = f"Product Data:\n{context}\n\nQuestion: {message}\n\nAnswer directly:"

    messages = [
        {"role": "system", "content": system},
        {"role": "user", "content": user_prompt},
    ]

    try:
        response = requests.post(VLLM_URL, json={
            "model": MODEL,
            "messages": messages,
            "max_tokens": 1024,
            "temperature": 0.1,
            "stream": True,
            "chat_template_kwargs": {"enable_thinking": False},
        }, stream=True, timeout=60)

        answer = ""
        for line in response.iter_lines():
            if not line:
                continue
            line = line.decode("utf-8")
            if not line.startswith("data: "):
                continue
            data = line[6:]
            if data.strip() == "[DONE]":
                break
            try:
                chunk = json.loads(data)
                token = chunk["choices"][0].get("delta", {}).get("content", "")
                if token:
                    answer += token
                    if len(answer) > 10:
                        updated = history + [{"role": "assistant", "content": answer}]
                        yield "", updated
            except (json.JSONDecodeError, KeyError):
                continue

        if not answer:
            answer = "Could not generate an answer."

        final = history + [{"role": "assistant", "content": answer}]
        yield "", final

    except Exception as e:
        final = history + [{"role": "assistant", "content": f"Error: {e}"}]
        yield "", final


# ==============================================================
# Tab 5: Export
# ==============================================================

def export_db(fmt, include_user_data):
    conn = _get_db()
    try:
        df_products = pd.read_sql("SELECT * FROM products ORDER BY id", conn)
        df_specs = pd.read_sql("""
            SELECT p.product_model, s.spec_key, s.spec_value
            FROM product_specs s JOIN products p ON s.product_id = p.id ORDER BY p.id
        """, conn)
    finally:
        conn.close()

    df_wide = df_specs.pivot_table(index="product_model", columns="spec_key", values="spec_value", aggfunc="first").reset_index()

    df_user = pd.DataFrame()
    if include_user_data:
        try:
            uconn = _get_user_db()
            df_user = pd.read_sql("SELECT * FROM products ORDER BY id", uconn)
            uconn.close()
        except Exception:
            pass

    ext = "xlsx" if fmt == "Excel (.xlsx)" else "csv"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}", prefix="catalog_export_")
    tmp.close()

    if ext == "xlsx":
        for col in df_products.select_dtypes(include=['datetimetz']).columns:
            df_products[col] = df_products[col].astype(str)
        if not df_user.empty:
            for col in df_user.select_dtypes(include=['datetimetz']).columns:
                df_user[col] = df_user[col].astype(str)
        with pd.ExcelWriter(tmp.name, engine="openpyxl") as writer:
            df_products.to_excel(writer, sheet_name="Products", index=False)
            df_wide.to_excel(writer, sheet_name="Specs (Pivot)", index=False)
            df_specs.to_excel(writer, sheet_name="Specs (Raw)", index=False)
            if not df_user.empty:
                df_user.to_excel(writer, sheet_name="User Edits", index=False)
    else:
        df_wide.to_csv(tmp.name, index=False)

    return tmp.name, f"Exported {len(df_products)} products -> `{os.path.basename(tmp.name)}`"


# ==============================================================
# Build Gradio App
# ==============================================================

def create_app():
    try:
        conn = _get_db()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM products")
        total = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM product_specs")
        specs = cur.fetchone()[0]
        cur.execute("SELECT category, count(*) FROM products GROUP BY category ORDER BY count(*) DESC")
        cats = cur.fetchall()
        conn.close()
        cat_summary = " | ".join(f"{c}: {n}" for c, n in cats if c)
    except Exception:
        total, specs, cat_summary = 0, 0, ""

    level_choices = [v["label"] for v in EXTRACTION_LEVELS.values()]

    with gr.Blocks(title="MCB Catalog Manager") as demo:
        gr.Markdown(f"""
        <div style="text-align:center;">
            <h1>Electrical Catalog Manager</h1>
            <p><em>Upload catalogs, extract products with configurable depth, preview & approve, browse & search, ask AI</em></p>
            <p style="font-size:0.9em;">{total} products | {specs} specs | {cat_summary}</p>
        </div>
        """)

        with gr.Tabs():

            # -- Tab 1: Upload & Extract --
            with gr.TabItem("Upload & Extract"):
                gr.Markdown("### Upload catalog PDFs and extract products")
                with gr.Tabs():
                    with gr.TabItem("Single PDF (Preview)"):
                        with gr.Row():
                            with gr.Column(scale=1):
                                upload_file = gr.File(label="Catalog PDF", file_types=[".pdf"], type="filepath")
                                level_dropdown = gr.Dropdown(
                                    choices=level_choices,
                                    value=level_choices[2],
                                    label="Extraction Level",
                                    info="Basic=names only, Standard=core specs, Detailed=everything",
                                )
                                category_filter = gr.Dropdown(
                                    choices=CATEGORY_CHOICES,
                                    value="All",
                                    label="Category Filter",
                                    info="Extract only selected categories (or 'All')",
                                    multiselect=True,
                                )
                                page_range_input = gr.Textbox(
                                    label="Page Range (optional)",
                                    placeholder="e.g. 1-50, or leave empty for all pages",
                                    info="For large PDFs — process a section at a time",
                                )
                                extract_btn = gr.Button("Extract & Preview", variant="primary")
                            with gr.Column(scale=2):
                                extract_status = gr.Markdown("_Upload a PDF, select level & category, then extract_")
                                pdf_preview = gr.Gallery(
                                    label="Document Preview",
                                    columns=3,
                                    height=300,
                                    object_fit="contain",
                                )

                        gr.Markdown("### Extraction Preview\n_Edit cells to correct data. Add rows for missing products. Then save._")
                        preview_table = gr.Dataframe(
                            label="Extracted Products (editable)",
                            interactive=True,
                            wrap=True,
                        )

                        save_key = gr.Textbox(label="Filename", interactive=False, visible=False)
                        with gr.Row():
                            save_btn = gr.Button("Re-save to Database", variant="secondary")
                            save_user_btn = gr.Button("Save User Additions (to User Data)", variant="primary")
                        save_status = gr.Markdown("")

                    with gr.TabItem("Batch (Multiple PDFs)"):
                        with gr.Row():
                            with gr.Column():
                                batch_files = gr.File(
                                    label="Catalog PDFs",
                                    file_types=[".pdf"],
                                    type="filepath",
                                    file_count="multiple",
                                )
                                batch_level = gr.Dropdown(
                                    choices=level_choices,
                                    value=level_choices[2],
                                    label="Extraction Level",
                                )
                                batch_cat = gr.Dropdown(
                                    choices=CATEGORY_CHOICES,
                                    value="All",
                                    label="Category Filter",
                                    multiselect=True,
                                )
                                batch_btn = gr.Button("Process All", variant="primary")
                            with gr.Column():
                                batch_status = gr.Markdown("_Upload PDFs to begin batch processing_")
                                batch_log = gr.Textbox(label="Batch Log", lines=15, visible=True)

            # -- Tab 2: Browse Products --
            with gr.TabItem("Browse Products"):
                gr.Markdown("### Browse & search the product catalog")
                with gr.Row():
                    browse_brand = gr.Dropdown(choices=get_brands(), value="All", label="Brand")
                    browse_cat = gr.Dropdown(choices=get_categories(), value="All", label="Category")
                    browse_search = gr.Textbox(label="Search", placeholder="Product name or model...")
                    browse_btn = gr.Button("Search", variant="primary")
                browse_table = gr.Dataframe(
                    value=browse_products("All", "All", ""),
                    label="Products"
                )
                with gr.Row():
                    detail_id = gr.Textbox(label="Enter Product ID or Model Number", lines=1)
                    detail_btn = gr.Button("View Details")
                with gr.Row():
                    with gr.Column(scale=2):
                        detail_output = gr.Markdown("_Select a product ID above_")
                    with gr.Column(scale=1):
                        detail_image = gr.Image(label="Product Image", height=250)
                        with gr.Row():
                            detail_alt1 = gr.Image(label="Alternate Image 1", height=150)
                            detail_alt2 = gr.Image(label="Alternate Image 2", height=150)

            # -- Tab 3: Review & Edit --
            with gr.TabItem("Review & Edit"):
                gr.Markdown("### Review, edit, or delete products\n_Edits and new specs are saved to **User Data** (mitsubishi_user_data)_")

                with gr.Tabs():
                    with gr.TabItem("Main Catalog"):
                        review_table = gr.Dataframe(value=get_review_products(), label="Products in Main DB")
                        refresh_btn = gr.Button("Refresh List")

                        gr.Markdown("#### Edit Product")
                        with gr.Row():
                            edit_id = gr.Textbox(label="Product ID", lines=1)
                            load_btn = gr.Button("Load Product", variant="secondary")
                        with gr.Row():
                            edit_name = gr.Textbox(label="Product Name", lines=1)
                            edit_model = gr.Textbox(label="Model", lines=1)
                            edit_cat = gr.Textbox(label="Category", lines=1)
                        with gr.Row():
                            edit_brand = gr.Textbox(label="Brand", lines=1)
                            edit_mrp = gr.Textbox(label="MRP", lines=1)
                            edit_desc = gr.Textbox(label="Description", lines=1)
                        with gr.Row():
                            save_edit_btn = gr.Button("Save Changes (to User Data)", variant="primary")
                            delete_btn = gr.Button("Delete Product", variant="stop")
                        edit_status = gr.Markdown("")

                        gr.Markdown("#### Add Spec (saves to User Data)")
                        with gr.Row():
                            spec_pid = gr.Textbox(label="Product ID", lines=1)
                            spec_key = gr.Textbox(label="Spec Key", lines=1)
                            spec_val = gr.Textbox(label="Spec Value", lines=1)
                            add_spec_btn = gr.Button("Add Spec", variant="secondary")
                        spec_status = gr.Markdown("")

                    with gr.TabItem("User Data (Edits & Additions)"):
                        gr.Markdown("### Products modified or added by users")
                        user_data_table = gr.Dataframe(value=get_user_data_table(), label="User Data")
                        refresh_user_btn = gr.Button("Refresh")

                        with gr.Row():
                            user_detail_id = gr.Textbox(label="User Product ID", lines=1)
                            user_detail_btn = gr.Button("View Specs")
                        user_detail_output = gr.Markdown("")

            # -- Tab 4: Ask AI --
            with gr.TabItem("Ask AI"):
                gr.Markdown("### Ask questions about products in the catalog")
                chat_history = gr.Chatbot(label="Chat", height=400)
                with gr.Row():
                    chat_input = gr.Textbox(label="Ask", placeholder="e.g. What MCBs are available with C-Curve?", lines=1, scale=5)
                    chat_send = gr.Button("Send", variant="primary", scale=1)
                chat_clear = gr.Button("Clear Chat")

            # -- Tab 5: Export --
            with gr.TabItem("Export"):
                gr.Markdown("### Export catalog data")
                with gr.Row():
                    export_fmt = gr.Radio(choices=["CSV (.csv)", "Excel (.xlsx)"], value="Excel (.xlsx)", label="Format")
                    export_user = gr.Checkbox(label="Include User Data sheet", value=True)
                    export_btn = gr.Button("Export", variant="primary")
                export_status = gr.Markdown("")
                export_file = gr.File(label="Download")

        # -- Wire events --

        # Tab 1: Extract & Preview
        def _extract_and_fill_key(file_path, level, cat_filter, pages):
            fname = os.path.basename(file_path) if file_path else ""
            for status, df, gallery in process_and_preview(file_path, level, cat_filter, pages):
                yield status, df, gallery, fname

        extract_btn.click(
            _extract_and_fill_key,
            inputs=[upload_file, level_dropdown, category_filter, page_range_input],
            outputs=[extract_status, preview_table, pdf_preview, save_key],
        )
        save_btn.click(save_previewed_products, inputs=[save_key], outputs=[save_status])
        save_user_btn.click(save_user_additions, inputs=[save_key, preview_table], outputs=[save_status])

        # Tab 1: Batch (now includes category filter)
        batch_btn.click(process_batch, inputs=[batch_files, batch_level, batch_cat], outputs=[batch_status, batch_log])

        # Tab 2: Browse
        browse_btn.click(browse_products, inputs=[browse_cat, browse_brand, browse_search], outputs=[browse_table])
        browse_search.submit(browse_products, inputs=[browse_cat, browse_brand, browse_search], outputs=[browse_table])
        browse_brand.change(browse_products, inputs=[browse_cat, browse_brand, browse_search], outputs=[browse_table])
        browse_cat.change(browse_products, inputs=[browse_cat, browse_brand, browse_search], outputs=[browse_table])
        detail_btn.click(get_product_detail, inputs=[detail_id], outputs=[detail_output, detail_image, detail_alt1, detail_alt2])

        # Tab 3: Review & Edit
        refresh_btn.click(get_review_products, outputs=[review_table])
        load_btn.click(load_product_for_edit, inputs=[edit_id],
                       outputs=[edit_name, edit_model, edit_cat, edit_brand, edit_mrp, edit_desc])
        save_edit_btn.click(save_product_edit,
                            inputs=[edit_id, edit_name, edit_model, edit_cat, edit_brand, edit_mrp, edit_desc],
                            outputs=[edit_status])
        delete_btn.click(delete_product, inputs=[edit_id], outputs=[edit_status])
        add_spec_btn.click(add_spec_to_user_data, inputs=[spec_pid, spec_key, spec_val], outputs=[spec_status])

        # Tab 3: User Data
        refresh_user_btn.click(get_user_data_table, outputs=[user_data_table])
        user_detail_btn.click(get_user_product_specs, inputs=[user_detail_id], outputs=[user_detail_output])

        # Tab 4: Chat
        chat_send.click(chat, inputs=[chat_input, chat_history], outputs=[chat_input, chat_history])
        chat_input.submit(chat, inputs=[chat_input, chat_history], outputs=[chat_input, chat_history])
        chat_clear.click(lambda: ([], ""), outputs=[chat_history, chat_input])

        # Tab 5: Export
        export_btn.click(export_db, inputs=[export_fmt, export_user], outputs=[export_file, export_status])

    return demo


if __name__ == "__main__":
    demo = create_app()
    IMAGE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "images")
    demo.launch(server_name="0.0.0.0", server_port=7862, share=False, allowed_paths=[IMAGE_DIR])
