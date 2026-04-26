"""Extractor V1 — Gradio UI for catalog pipeline (mitsubishi_test DB format).

Tabs:
1. Upload & Extract — upload PDF, OCR + LLM extract, review products
2. Product Browser — browse/search/filter products in DB
3. Review & Approve — edit/approve/reject extracted products
4. Ask AI — chatbot for product queries
5. Export — download DB as CSV/Excel
"""

import os
import sys
import json
import time
import tempfile
import re

import gradio as gr
import pandas as pd
import psycopg2
import requests

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(PROJECT_DIR, "ocr_complete"))
sys.path.insert(0, PROJECT_DIR)

from pipeline.db import get_db as _get_db, DB_PARAMS, is_file_processed, mark_file_processed

VLLM_URL = "http://localhost:8200/v1/chat/completions"
MODEL = "Qwen/Qwen3.5-27B-FP8"


# ══════════════════════════════════════════════════════
# Tab 1: Upload & Extract
# ══════════════════════════════════════════════════════

def process_catalog(file_path):
    """Upload a catalog PDF -> OCR -> LLM extract -> save to DB."""
    if not file_path:
        yield "Upload a PDF to begin.", ""
        return

    fname = os.path.basename(file_path)
    yield f"**Processing:** {fname}\n\n_Step 1: OCR..._", ""

    try:
        import subprocess
        ocr_script = f"""
import sys, json
sys.path.insert(0, '{os.path.join(PROJECT_DIR, "ocr_complete")}')
from pipeline import processor
text, tables, dt, method, file_type, num_pages = processor.process('{file_path}')
result = {{"text": text, "tables": tables, "dt": dt, "method": method,
           "file_type": file_type, "num_pages": num_pages}}
print("__OCR_JSON__")
print(json.dumps(result))
"""
        ocr_venv_python = os.path.join(PROJECT_DIR, "ocr_complete", "venv", "bin", "python")
        proc = subprocess.run(
            [ocr_venv_python, "-c", ocr_script],
            capture_output=True, text=True, timeout=300,
            cwd=os.path.join(PROJECT_DIR, "ocr_complete"))

        if proc.returncode != 0:
            raise RuntimeError(proc.stderr.strip().split("\n")[-1])

        stdout = proc.stdout
        json_str = stdout.split("__OCR_JSON__\n", 1)[1]
        ocr_result = json.loads(json_str)
        text = ocr_result["text"]
        tables = ocr_result["tables"]
        dt = ocr_result["dt"]
        method = ocr_result["method"]
        num_pages = ocr_result["num_pages"]

        yield f"**OCR Done:** {len(text)} chars, {len(tables)} tables, {dt:.1f}s\n\n_Step 2: Extracting products..._", ""
    except Exception as e:
        yield f"**OCR Failed:** {e}", ""
        return

    if not tables:
        yield f"**No tables found** in {fname}. Only text was extracted ({len(text)} chars).", text
        return

    try:
        from pipeline.catalog_extractor import extract_from_tables, save_products
        from pipeline.image_extractor import extract_images_from_pdf, link_images_to_products

        products = extract_from_tables(tables, fname)
        yield f"**Extracted:** {len(products)} products\n\n_Step 3: Saving to DB..._", ""

        inserted, skipped = save_products(products, catalogue_name=fname)
        yield (
            f"**Saved:** {inserted} new, {skipped} skipped\n\n"
            f"_Step 4: Extracting images..._"
        ), ""

        images = extract_images_from_pdf(file_path)
        linked = link_images_to_products(images, fname, pdf_path=file_path)

        yield (
            f"**Done!**\n\n"
            f"- OCR: {len(text)} chars, {len(tables)} tables ({dt:.1f}s)\n"
            f"- Products: {len(products)} extracted, {inserted} new, {skipped} skipped\n"
            f"- Images: {len(images)} extracted, {linked} linked to products\n"
            f"- Method: {method}"
        ), text
    except Exception as e:
        yield f"**Extraction Failed:** {e}\n\nOCR was successful ({len(tables)} tables found).", text


def process_batch(file_paths):
    """Upload multiple catalog PDFs -> process each sequentially with dedup."""
    if not file_paths:
        yield "Upload PDF files to begin.", ""
        return

    pdf_files = [f for f in file_paths if f.lower().endswith(".pdf")]
    if not pdf_files:
        yield "No PDF files found in the upload.", ""
        return

    total = len(pdf_files)
    yield f"**Batch:** Found {total} PDFs. Starting...\n", ""

    results = []
    total_inserted = 0
    total_skipped = 0
    total_images = 0

    for idx, file_path in enumerate(pdf_files, 1):
        fname = os.path.basename(file_path)

        already_done, prev_record = is_file_processed(file_path)
        if already_done:
            results.append(f"- **{fname}**: Skipped (already processed on {prev_record['processed_at']})")
            progress = "\n".join(results)
            yield f"**Batch Progress:** {idx}/{total}\n\n{progress}\n", ""
            continue

        results.append(f"- **{fname}**: Processing...")
        progress = "\n".join(results)
        yield f"**Batch Progress:** {idx}/{total}\n\n{progress}\n", ""

        try:
            import subprocess
            ocr_script = f"""
import sys, json
sys.path.insert(0, '{os.path.join(PROJECT_DIR, "ocr_complete")}')
from pipeline import processor
text, tables, dt, method, file_type, num_pages = processor.process('{file_path}')
result = {{"text": text, "tables": tables, "dt": dt, "method": method,
           "file_type": file_type, "num_pages": num_pages}}
print("__OCR_JSON__")
print(json.dumps(result))
"""
            ocr_venv_python = os.path.join(PROJECT_DIR, "ocr_complete", "venv", "bin", "python")
            proc = subprocess.run(
                [ocr_venv_python, "-c", ocr_script],
                capture_output=True, text=True, timeout=300,
                cwd=os.path.join(PROJECT_DIR, "ocr_complete"))

            if proc.returncode != 0:
                raise RuntimeError(proc.stderr.strip().split("\n")[-1])

            stdout = proc.stdout
            json_str = stdout.split("__OCR_JSON__\n", 1)[1]
            ocr_result = json.loads(json_str)
            tables = ocr_result["tables"]
            dt = ocr_result["dt"]

            if not tables:
                results[-1] = f"- **{fname}**: No tables found, skipped"
                progress = "\n".join(results)
                yield f"**Batch Progress:** {idx}/{total}\n\n{progress}\n", ""
                continue

            from pipeline.catalog_extractor import extract_from_tables, save_products
            from pipeline.image_extractor import extract_images_from_pdf, link_images_to_products

            products = extract_from_tables(tables, fname)
            inserted, skipped = save_products(products, catalogue_name=fname)

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

        progress = "\n".join(results)
        yield f"**Batch Progress:** {idx}/{total}\n\n{progress}\n", ""

    summary = (
        f"**Batch Complete!**\n\n"
        f"- **Files:** {total} uploaded\n"
        f"- **Products:** {total_inserted} new, {total_skipped} skipped\n"
        f"- **Images:** {total_images} linked\n\n"
        f"### Per-file results:\n" + "\n".join(results)
    )
    yield summary, ""


# ══════════════════════════════════════════════════════
# Tab 2: Product Browser
# ══════════════════════════════════════════════════════

def get_categories():
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT category FROM products WHERE category IS NOT NULL ORDER BY category")
    cats = ["All"] + [r[0] for r in cur.fetchall()]
    conn.close()
    return cats


def browse_products(category, search_text):
    conn = _get_db()
    cur = conn.cursor()

    conditions = []
    params = []
    if category and category != "All":
        conditions.append("p.category = %s")
        params.append(category)
    if search_text and search_text.strip():
        conditions.append("(p.product_name ILIKE %s OR p.product_model ILIKE %s)")
        params.extend([f"%{search_text}%", f"%{search_text}%"])

    where = "WHERE " + " AND ".join(conditions) if conditions else ""

    cur.execute(f"""
        SELECT p.id, p.product_name, p.product_model, p.category, p.brand,
               CASE WHEN p.image_url IS NOT NULL THEN 'Yes' ELSE 'No' END as has_image
        FROM products p {where}
        ORDER BY p.id
    """, params)

    rows = cur.fetchall()
    conn.close()

    df = pd.DataFrame(rows, columns=["ID", "Product Name", "Model", "Category", "Brand", "Image"])
    return df


def get_product_detail(product_id):
    if not product_id or not product_id.strip():
        return "Select a product from the table above.", None

    conn = _get_db()
    cur = conn.cursor()

    lookup = product_id.strip()
    try:
        pid = int(lookup)
        cur.execute("SELECT id, product_name, product_model, description, category, subcategory, brand, mrp, image_url, alternate_image1, alternate_image2, catalogue_name FROM products WHERE id = %s", (pid,))
    except (ValueError, TypeError):
        cur.execute("SELECT id, product_name, product_model, description, category, subcategory, brand, mrp, image_url, alternate_image1, alternate_image2, catalogue_name FROM products WHERE product_model = %s", (lookup,))
        pid = None

    row = cur.fetchone()
    if not row:
        conn.close()
        return "Product not found.", None

    pid, name, model, desc, cat, subcat, brand, mrp, img, alt1, alt2, cat_name = row

    cur.execute("""
        SELECT spec_key, spec_value, spec_group FROM product_specs
        WHERE product_id = %s ORDER BY spec_group, spec_key
    """, (pid,))
    specs = cur.fetchall()
    conn.close()

    output = [f"## {name}", f"**Model:** {model} | **Category:** {cat} | **Brand:** {brand}", ""]
    if subcat:
        output.append(f"**Subcategory:** {subcat}")
    if mrp:
        output.append(f"**MRP:** {mrp}")
    if cat_name:
        output.append(f"**Catalogue:** {cat_name}")
    if desc:
        output.append(f"\n_{desc}_\n")

    # Group specs by spec_group
    groups = {}
    for k, v, g in specs:
        group = g or "General"
        groups.setdefault(group, []).append((k, v))

    for group_name, group_specs in groups.items():
        output.append(f"\n### {group_name}\n")
        output.append("| Spec | Value |")
        output.append("|---|---|")
        for k, v in group_specs:
            output.append(f"| {k} | {v} |")

    img_path = None
    if img and os.path.exists(img):
        img_path = img

    return "\n".join(output), img_path


# ══════════════════════════════════════════════════════
# Tab 3: Review & Approve (edit/delete products)
# ══════════════════════════════════════════════════════

def get_review_products():
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT p.id, p.product_name, p.product_model, p.category, p.brand,
               count(s.id) as spec_count
        FROM products p LEFT JOIN product_specs s ON s.product_id = p.id
        GROUP BY p.id ORDER BY p.id DESC
    """)
    rows = cur.fetchall()
    conn.close()
    return pd.DataFrame(rows, columns=["ID", "Name", "Model", "Category", "Brand", "Specs"])


def update_product(product_id, new_name, new_model, new_category, new_brand):
    if not product_id:
        return "Enter a product ID."
    try:
        pid = int(product_id)
    except ValueError:
        return "Invalid ID."

    conn = _get_db()
    cur = conn.cursor()
    cur.execute("""UPDATE products SET product_name = %s, product_model = %s, category = %s, brand = %s
                   WHERE id = %s""", (new_name, new_model, new_category, new_brand, pid))
    conn.commit()
    conn.close()
    return f"Updated product #{pid}"


def delete_product(product_id):
    if not product_id:
        return "Enter a product ID."
    try:
        pid = int(product_id)
    except ValueError:
        return "Invalid ID."

    conn = _get_db()
    cur = conn.cursor()
    cur.execute("DELETE FROM products WHERE id = %s", (pid,))
    conn.commit()
    conn.close()
    return f"Deleted product #{pid} and all its specs."


def add_spec(product_id, spec_key, spec_value, spec_group):
    if not product_id or not spec_key or not spec_value:
        return "Fill all required fields (Product ID, Key, Value)."
    try:
        pid = int(product_id)
    except ValueError:
        return "Invalid ID."

    conn = _get_db()
    cur = conn.cursor()

    # Get product_model and category for denormalized fields
    cur.execute("SELECT product_model, category FROM products WHERE id = %s", (pid,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return f"Product #{pid} not found."
    pmodel, pcat = row

    cur.execute("""
        INSERT INTO product_specs (product_id, spec_key, spec_value, product_model, category, spec_group)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (product_id, spec_key) DO UPDATE SET
            spec_value = EXCLUDED.spec_value,
            spec_group = COALESCE(EXCLUDED.spec_group, product_specs.spec_group)
    """, (pid, spec_key, spec_value, pmodel, pcat, spec_group or None))
    conn.commit()
    conn.close()
    return f"Added spec '{spec_key}' to product #{pid}"


# ══════════════════════════════════════════════════════
# Tab 4: Ask AI (chatbot)
# ══════════════════════════════════════════════════════

def _search_products_for_context(query, limit=10):
    """Search products DB for relevant context."""
    conn = _get_db()
    cur = conn.cursor()

    words = [w for w in re.findall(r'\w+', query.lower()) if len(w) > 2]
    if not words:
        conn.close()
        return "No products found."

    conditions = []
    params = []
    for w in words[:5]:
        conditions.append("""(p.product_name ILIKE %s OR p.product_model ILIKE %s
            OR EXISTS (SELECT 1 FROM product_specs s WHERE s.product_id = p.id AND s.spec_value ILIKE %s))""")
        params.extend([f"%{w}%", f"%{w}%", f"%{w}%"])

    where = " OR ".join(conditions)
    cur.execute(f"""
        SELECT DISTINCT p.id, p.product_name, p.product_model, p.category, p.brand
        FROM products p
        WHERE {where}
        LIMIT %s
    """, params + [limit])
    products = cur.fetchall()

    if not products:
        conn.close()
        return "No matching products found."

    context_parts = []
    for pid, name, model, cat, brand in products:
        cur.execute("SELECT spec_key, spec_value, spec_group FROM product_specs WHERE product_id = %s", (pid,))
        specs = cur.fetchall()
        spec_str = ", ".join(f"{k}: {v}" for k, v, _ in list(specs)[:10])
        context_parts.append(f"[{name} ({model}, {cat}, {brand})]: {spec_str}")

    conn.close()
    return "\n".join(context_parts)


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


# ══════════════════════════════════════════════════════
# Tab 5: Export
# ══════════════════════════════════════════════════════

def export_db(fmt):
    conn = _get_db()

    df_products = pd.read_sql("SELECT * FROM products ORDER BY id", conn)
    df_specs = pd.read_sql("""
        SELECT p.product_model, s.spec_key, s.spec_value, s.spec_group
        FROM product_specs s JOIN products p ON s.product_id = p.id ORDER BY p.id
    """, conn)
    conn.close()

    # Pivot specs
    df_wide = df_specs.pivot_table(index="product_model", columns="spec_key", values="spec_value", aggfunc="first").reset_index()

    ext = "xlsx" if fmt == "Excel (.xlsx)" else "csv"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}", prefix="catalog_export_")
    tmp.close()

    if ext == "xlsx":
        for col in df_products.select_dtypes(include=['datetimetz']).columns:
            df_products[col] = df_products[col].astype(str)
        with pd.ExcelWriter(tmp.name, engine="openpyxl") as writer:
            df_products.to_excel(writer, sheet_name="Products", index=False)
            df_wide.to_excel(writer, sheet_name="Specs (Pivot)", index=False)
            df_specs.to_excel(writer, sheet_name="Specs (Raw)", index=False)
    else:
        df_wide.to_csv(tmp.name, index=False)

    return tmp.name, f"Exported {len(df_products)} products -> `{os.path.basename(tmp.name)}`"


# ══════════════════════════════════════════════════════
# Build Gradio App
# ══════════════════════════════════════════════════════

def create_app():
    conn = _get_db()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM products")
    total = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM product_specs")
    specs = cur.fetchone()[0]
    cur.execute("SELECT category, count(*) FROM products WHERE category IS NOT NULL GROUP BY category ORDER BY count(*) DESC")
    cats = cur.fetchall()
    cur.execute("SELECT count(DISTINCT brand) FROM products WHERE brand IS NOT NULL")
    brand_count = cur.fetchone()[0]
    conn.close()

    cat_summary = " | ".join(f"{c}: {n}" for c, n in cats)

    with gr.Blocks(title="Extractor V1 — Catalog Manager") as demo:
        gr.Markdown(f"""
        <div style="text-align:center;">
            <h1>Catalog Manager — Extractor V1</h1>
            <p><em>Upload catalogs, extract products, browse & search, ask AI</em></p>
            <p style="font-size:0.9em;">{total} products | {specs} specs | {brand_count} brands | {cat_summary}</p>
        </div>
        """)

        with gr.Tabs():

            # -- Tab 1: Upload --
            with gr.TabItem("Upload & Extract"):
                gr.Markdown("### Upload catalog PDFs to extract products")
                with gr.Tabs():
                    with gr.TabItem("Single PDF"):
                        with gr.Row():
                            with gr.Column():
                                upload_file = gr.File(label="Catalog PDF", file_types=[".pdf"], type="filepath")
                                upload_btn = gr.Button("Process Catalog", variant="primary")
                            with gr.Column():
                                upload_status = gr.Markdown("_Upload a PDF to begin_")
                                upload_text = gr.Textbox(label="OCR Output", lines=15, visible=True)
                    with gr.TabItem("Batch (Multiple PDFs)"):
                        with gr.Row():
                            with gr.Column():
                                batch_files = gr.File(
                                    label="Catalog PDFs",
                                    file_types=[".pdf"],
                                    type="filepath",
                                    file_count="multiple",
                                )
                                batch_btn = gr.Button("Process All", variant="primary")
                            with gr.Column():
                                batch_status = gr.Markdown("_Upload multiple PDFs to begin batch processing_")
                                batch_log = gr.Textbox(label="Batch Log", lines=15, visible=True)

            # -- Tab 2: Browse --
            with gr.TabItem("Browse Products"):
                gr.Markdown("### Browse & search the product catalog")
                with gr.Row():
                    browse_cat = gr.Dropdown(choices=get_categories(), value="All", label="Category")
                    browse_search = gr.Textbox(label="Search", placeholder="Product name or model...")
                    browse_btn = gr.Button("Search", variant="primary")
                browse_table = gr.Dataframe(
                    value=browse_products("All", ""),
                    label="Products"
                )
                with gr.Row():
                    detail_id = gr.Textbox(label="Enter Product ID or Model Number", lines=1)
                    detail_btn = gr.Button("View Details")
                with gr.Row():
                    with gr.Column(scale=2):
                        detail_output = gr.Markdown("_Select a product ID above_")
                    with gr.Column(scale=1):
                        detail_image = gr.Image(label="Product Image", height=300)

            # -- Tab 3: Review --
            with gr.TabItem("Review & Edit"):
                gr.Markdown("### Review, edit, or delete products")
                review_table = gr.Dataframe(value=get_review_products(), label="Recent Products")
                refresh_btn = gr.Button("Refresh List")

                gr.Markdown("#### Edit Product")
                with gr.Row():
                    edit_id = gr.Textbox(label="Product ID", lines=1)
                    edit_name = gr.Textbox(label="Product Name", lines=1)
                    edit_model = gr.Textbox(label="Model", lines=1)
                    edit_cat = gr.Textbox(label="Category", lines=1)
                    edit_brand = gr.Textbox(label="Brand", lines=1)
                with gr.Row():
                    save_btn = gr.Button("Save Changes", variant="primary")
                    delete_btn = gr.Button("Delete Product", variant="stop")
                edit_status = gr.Markdown("")

                gr.Markdown("#### Add Spec")
                with gr.Row():
                    spec_pid = gr.Textbox(label="Product ID", lines=1)
                    spec_key = gr.Textbox(label="Spec Key", lines=1)
                    spec_val = gr.Textbox(label="Spec Value", lines=1)
                    spec_grp = gr.Textbox(label="Spec Group", lines=1, placeholder="e.g. Specifications")
                    add_spec_btn = gr.Button("Add Spec", variant="secondary")
                spec_status = gr.Markdown("")

            # -- Tab 4: Ask AI --
            with gr.TabItem("Ask AI"):
                gr.Markdown("### Ask questions about products in the catalog")
                chat_history = gr.Chatbot(label="Chat", height=400)
                with gr.Row():
                    chat_input = gr.Textbox(label="Ask", placeholder="e.g. What MCCB models are available from ABB?", lines=1, scale=5)
                    chat_send = gr.Button("Send", variant="primary", scale=1)
                chat_clear = gr.Button("Clear Chat")

            # -- Tab 5: Export --
            with gr.TabItem("Export"):
                gr.Markdown("### Export catalog data")
                with gr.Row():
                    export_fmt = gr.Radio(choices=["CSV (.csv)", "Excel (.xlsx)"], value="Excel (.xlsx)", label="Format")
                    export_btn = gr.Button("Export", variant="primary")
                export_status = gr.Markdown("")
                export_file = gr.File(label="Download")

        # -- Wire events --
        upload_btn.click(process_catalog, inputs=[upload_file], outputs=[upload_status, upload_text])
        batch_btn.click(process_batch, inputs=[batch_files], outputs=[batch_status, batch_log])
        browse_btn.click(browse_products, inputs=[browse_cat, browse_search], outputs=[browse_table])
        browse_search.submit(browse_products, inputs=[browse_cat, browse_search], outputs=[browse_table])
        detail_btn.click(get_product_detail, inputs=[detail_id], outputs=[detail_output, detail_image])

        refresh_btn.click(get_review_products, outputs=[review_table])
        save_btn.click(update_product, inputs=[edit_id, edit_name, edit_model, edit_cat, edit_brand], outputs=[edit_status])
        delete_btn.click(delete_product, inputs=[edit_id], outputs=[edit_status])
        add_spec_btn.click(add_spec, inputs=[spec_pid, spec_key, spec_val, spec_grp], outputs=[spec_status])

        chat_send.click(chat, inputs=[chat_input, chat_history], outputs=[chat_input, chat_history])
        chat_input.submit(chat, inputs=[chat_input, chat_history], outputs=[chat_input, chat_history])
        chat_clear.click(lambda: ([], ""), outputs=[chat_history, chat_input])

        export_btn.click(export_db, inputs=[export_fmt], outputs=[export_file, export_status])

    return demo


if __name__ == "__main__":
    demo = create_app()
    IMAGE_DIR = os.path.join(PROJECT_DIR, "data", "images")
    demo.launch(server_name="0.0.0.0", server_port=7863, share=False, allowed_paths=[IMAGE_DIR])
