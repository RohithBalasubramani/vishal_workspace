"""Accessories Extract — Gradio UI.

Tabs:
1. Upload & Extract — upload PDF, extract accessories, preview & save
2. Browse Accessories — browse/search/filter accessories
3. Product Mapping — view/edit accessory-to-product links
4. Export — download as CSV/Excel
"""

import os
import sys
import json
import tempfile
import re

import gradio as gr
import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

from pipeline.db import (
    get_db, init_db, is_file_processed, mark_file_processed,
    get_all_accessories, get_accessory_detail, get_mappings,
    save_product_mapping, get_product_db,
)

VLLM_URL = os.environ.get("VLLM_URL", "http://localhost:8201/v1/chat/completions")

_preview_cache = {}


# ==============================================================
# Tab 1: Upload & Extract
# ==============================================================

def process_and_preview(file_path, page_range):
    if not file_path:
        return "Upload a PDF to begin.", None, None

    fname = os.path.basename(file_path)
    yield f"**Processing:** {fname}\n\n_Rendering PDF previews..._", None, None

    from pipeline.pdf_extractor import render_pdf_previews
    previews = render_pdf_previews(file_path, max_pages=8)

    yield f"**Processing:** {fname}\n\n_Running DeepSeek-OCR-2..._", None, previews

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

        from pipeline.pdf_extractor import ocr_pdf, parse_tables_from_ocr
        ocr_pages = ocr_pdf(file_path, start_page, end_page)
        tables = parse_tables_from_ocr(ocr_pages)
    except Exception as e:
        yield f"**OCR Failed:** {e}", None, previews
        return

    if not tables:
        yield f"**No tables found** in {fname}.", None, previews
        return

    yield (
        f"**OCR Done:** {len(tables)} tables found.\n\n"
        f"_Extracting accessories via Qwen..._"
    ), None, previews

    try:
        from pipeline.catalog_extractor import extract_accessories, save_accessories
        accessories = extract_accessories(tables, fname)
    except Exception as e:
        yield f"**Extraction Failed:** {e}", None, previews
        return

    if not accessories:
        yield f"**No accessories extracted** from {len(tables)} tables.", None, previews
        return

    # Auto-save to DB
    new_count, existing_count = save_accessories(accessories)

    # Map to products
    from pipeline.product_mapper import map_accessories_to_products
    mappings = map_accessories_to_products(accessories)

    mark_file_processed(file_path, fname,
                        accessories_inserted=new_count,
                        accessories_skipped=existing_count,
                        mappings_created=mappings)

    # Build preview
    rows = []
    for acc in accessories:
        rows.append({
            "Name": acc.get("accessory_name", ""),
            "Model": acc.get("accessory_model", ""),
            "Category": acc.get("category", ""),
            "Sub-Category": acc.get("sub_category", ""),
            "Applies To": acc.get("applies_to", ""),
            "MRP": acc.get("mrp", ""),
        })
    df = pd.DataFrame(rows)

    _preview_cache[fname] = {"accessories": accessories}

    yield (
        f"**Saved!** {len(accessories)} accessories extracted.\n"
        f"- **New:** {new_count}\n"
        f"- **Existing:** {existing_count}\n"
        f"- **Product mappings:** {mappings}"
    ), df, previews


# ==============================================================
# Tab 2: Browse Accessories
# ==============================================================

def get_categories():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT category FROM accessories WHERE category IS NOT NULL ORDER BY category")
        return ["All"] + [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def get_sub_categories():
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT sub_category FROM accessories WHERE sub_category IS NOT NULL ORDER BY sub_category")
        return ["All"] + [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def browse_accessories(category, sub_category, search_text):
    conn = get_db()
    try:
        cur = conn.cursor()
        conditions = []
        params = []

        if category and category != "All":
            conditions.append("a.category = %s")
            params.append(category)
        if sub_category and sub_category != "All":
            conditions.append("a.sub_category = %s")
            params.append(sub_category)
        if search_text and search_text.strip():
            conditions.append("""(a.accessory_name ILIKE %s OR a.accessory_model ILIKE %s
                OR EXISTS (SELECT 1 FROM accessory_specs s WHERE s.accessory_id = a.id AND s.spec_value ILIKE %s))""")
            params.extend([f"%{search_text}%", f"%{search_text}%", f"%{search_text}%"])

        where = "WHERE " + " AND ".join(conditions) if conditions else ""

        cur.execute(f"""
            SELECT a.id, a.accessory_name, a.accessory_model, a.category, a.sub_category, a.mrp,
                   (SELECT count(*) FROM accessory_product_map m WHERE m.accessory_id = a.id) as linked_products
            FROM accessories a {where}
            ORDER BY a.id
        """, params)

        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=["ID", "Name", "Model", "Category", "Sub-Category", "MRP", "Linked Products"])
    finally:
        conn.close()


def view_accessory_detail(accessory_id):
    if not accessory_id:
        return "Enter an accessory ID.", None

    try:
        aid = int(str(accessory_id).strip())
    except ValueError:
        # Search by model
        conn = get_db()
        try:
            cur = conn.cursor()
            cur.execute("SELECT id FROM accessories WHERE accessory_model ILIKE %s LIMIT 1",
                        (f"%{accessory_id}%",))
            row = cur.fetchone()
            if not row:
                return f"Accessory not found for '{accessory_id}'.", None
            aid = row[0]
        finally:
            conn.close()

    accessory, specs, mappings = get_accessory_detail(aid)
    if not accessory:
        return "Accessory not found.", None

    lines = [
        f"## {accessory.get('accessory_name', 'N/A')}",
        f"**Model:** {accessory.get('accessory_model', 'N/A')} | "
        f"**Category:** {accessory.get('category', 'N/A')} | "
        f"**Sub-Category:** {accessory.get('sub_category', 'N/A')} | "
        f"**MRP:** {accessory.get('mrp', 'N/A')}",
        "",
        "### Specifications",
        "| Spec | Value |",
        "|---|---|",
    ]
    for k, v in specs:
        lines.append(f"| {k} | {v} |")
    if not specs:
        lines.append("| _(no specs)_ | |")

    lines.extend(["", "### Compatible Products", "| Product Model | Frame Size |", "|---|---|"])
    for pm, fs, notes in mappings:
        lines.append(f"| {pm} | {fs or 'N/A'} |")
    if not mappings:
        lines.append("| _(no mappings)_ | |")

    img_path = accessory.get("image_url")
    img = img_path if (img_path and os.path.exists(img_path)) else None

    return "\n".join(lines), img


# ==============================================================
# Tab 3: Product Mapping
# ==============================================================

def get_mapping_table():
    rows = get_mappings()
    if not rows:
        return pd.DataFrame(columns=["Accessory Name", "Accessory Model", "Sub-Category", "Product Model", "Frame Size", "MRP"])
    return pd.DataFrame(rows, columns=["Accessory Name", "Accessory Model", "Sub-Category", "Product Model", "Frame Size", "MRP"])


def search_products(search):
    """Search mitsubishi_test products for manual mapping."""
    if not search or not search.strip():
        return "Enter a product model or name to search."
    conn = get_product_db()
    try:
        cur = conn.cursor()
        cur.execute("""
            SELECT product_model, product_name, category FROM products
            WHERE (product_model ILIKE %s OR product_name ILIKE %s)
            AND category IN ('MCCB', 'ACB', 'MCB')
            LIMIT 20
        """, (f"%{search}%", f"%{search}%"))
        rows = cur.fetchall()
        if not rows:
            return f"No products found for '{search}'."
        lines = ["| Model | Name | Category |", "|---|---|---|"]
        for model, name, cat in rows:
            lines.append(f"| {model} | {name} | {cat} |")
        return "\n".join(lines)
    finally:
        conn.close()


def add_mapping(accessory_model, product_model, frame_size):
    if not accessory_model or not product_model:
        return "Fill both fields."
    conn = get_db()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id FROM accessories WHERE accessory_model = %s", (accessory_model.strip(),))
        row = cur.fetchone()
        if not row:
            return f"Accessory '{accessory_model}' not found."
        save_product_mapping(row[0], product_model.strip(), frame_size or None)
        return f"Mapped {accessory_model} → {product_model}"
    finally:
        conn.close()


# ==============================================================
# Tab 4: Export
# ==============================================================

def export_data(fmt):
    conn = get_db()
    try:
        df_acc = pd.read_sql("SELECT * FROM accessories ORDER BY id", conn)
        df_specs = pd.read_sql("""
            SELECT a.accessory_model, s.spec_key, s.spec_value
            FROM accessory_specs s JOIN accessories a ON s.accessory_id = a.id ORDER BY a.id
        """, conn)
        df_map = pd.read_sql("""
            SELECT a.accessory_name, a.accessory_model, m.product_model, m.applies_to_frame_size
            FROM accessory_product_map m JOIN accessories a ON a.id = m.accessory_id ORDER BY a.id
        """, conn)
    finally:
        conn.close()

    ext = "xlsx" if fmt == "Excel (.xlsx)" else "csv"
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=f".{ext}", prefix="accessories_export_")
    tmp.close()

    if ext == "xlsx":
        for col in df_acc.select_dtypes(include=['datetimetz']).columns:
            df_acc[col] = df_acc[col].astype(str)
        with pd.ExcelWriter(tmp.name, engine="openpyxl") as writer:
            df_acc.to_excel(writer, sheet_name="Accessories", index=False)
            df_specs.to_excel(writer, sheet_name="Specs", index=False)
            df_map.to_excel(writer, sheet_name="Product Mappings", index=False)
    else:
        df_acc.to_csv(tmp.name, index=False)

    return tmp.name, f"Exported {len(df_acc)} accessories → `{os.path.basename(tmp.name)}`"


# ==============================================================
# Build Gradio App
# ==============================================================

def create_app():
    try:
        conn = get_db()
        cur = conn.cursor()
        cur.execute("SELECT count(*) FROM accessories")
        total = cur.fetchone()[0]
        cur.execute("SELECT count(*) FROM accessory_product_map")
        mappings = cur.fetchone()[0]
        conn.close()
    except Exception:
        total, mappings = 0, 0

    with gr.Blocks(title="Accessories Extract") as demo:
        gr.Markdown(f"""
        <div style="text-align:center;">
            <h1>Accessories Extract Pipeline</h1>
            <p><em>Extract accessories from LVS catalogs, map to parent products in mitsubishi_test</em></p>
            <p style="font-size:0.9em;">{total} accessories | {mappings} product mappings</p>
        </div>
        """)

        with gr.Tabs():

            # -- Tab 1: Upload & Extract --
            with gr.TabItem("Upload & Extract"):
                gr.Markdown("### Upload a catalog PDF to extract accessories")
                with gr.Row():
                    with gr.Column(scale=1):
                        upload_file = gr.File(label="Catalog PDF", file_types=[".pdf"], type="filepath")
                        page_range = gr.Textbox(label="Page Range (optional)", placeholder="e.g. 26-29")
                        extract_btn = gr.Button("Extract Accessories", variant="primary")
                    with gr.Column(scale=2):
                        extract_status = gr.Markdown("_Upload a PDF to begin_")
                        pdf_preview = gr.Gallery(label="Document Preview", columns=3, height=250, object_fit="contain")

                gr.Markdown("### Extracted Accessories")
                preview_table = gr.Dataframe(label="Accessories (preview)", interactive=False, wrap=True)

            # -- Tab 2: Browse --
            with gr.TabItem("Browse Accessories"):
                gr.Markdown("### Browse & search accessories")
                with gr.Row():
                    browse_cat = gr.Dropdown(choices=["All", "Internal", "External"], value="All", label="Category")
                    browse_sub = gr.Dropdown(choices=get_sub_categories(), value="All", label="Sub-Category")
                    browse_search = gr.Textbox(label="Search", placeholder="Model or name...")
                    browse_btn = gr.Button("Search", variant="primary")
                browse_table = gr.Dataframe(label="Accessories")

                with gr.Row():
                    detail_id = gr.Textbox(label="Accessory ID or Model", lines=1)
                    detail_btn = gr.Button("View Details")
                with gr.Row():
                    with gr.Column(scale=2):
                        detail_output = gr.Markdown("_Select an accessory_")
                    with gr.Column(scale=1):
                        detail_image = gr.Image(label="Image", height=200)

            # -- Tab 3: Product Mapping --
            with gr.TabItem("Product Mapping"):
                gr.Markdown("### Accessory → Product Mappings")
                mapping_table = gr.Dataframe(value=get_mapping_table(), label="All Mappings")
                refresh_map_btn = gr.Button("Refresh")

                gr.Markdown("#### Add Manual Mapping")
                with gr.Row():
                    map_acc = gr.Textbox(label="Accessory Model", placeholder="e.g. AL-05SV*")
                    map_prod = gr.Textbox(label="Product Model (from mitsubishi_test)", placeholder="e.g. NF125-SV 3P 16A")
                    map_frame = gr.Textbox(label="Frame Size (optional)", placeholder="e.g. NF63-250AF")
                    map_btn = gr.Button("Add Mapping", variant="primary")
                map_status = gr.Markdown("")

                gr.Markdown("#### Search Products (mitsubishi_test)")
                with gr.Row():
                    prod_search = gr.Textbox(label="Search Product", placeholder="e.g. NF125")
                    prod_search_btn = gr.Button("Search")
                prod_results = gr.Markdown("")

            # -- Tab 4: Export --
            with gr.TabItem("Export"):
                gr.Markdown("### Export accessories data")
                with gr.Row():
                    export_fmt = gr.Radio(choices=["CSV (.csv)", "Excel (.xlsx)"], value="Excel (.xlsx)", label="Format")
                    export_btn = gr.Button("Export", variant="primary")
                export_status = gr.Markdown("")
                export_file = gr.File(label="Download")

        # -- Wire events --

        def _extract(fp, pr):
            fname = os.path.basename(fp) if fp else ""
            for status, df, gallery in process_and_preview(fp, pr):
                yield status, df, gallery

        extract_btn.click(_extract, inputs=[upload_file, page_range],
                          outputs=[extract_status, preview_table, pdf_preview])

        browse_btn.click(browse_accessories, inputs=[browse_cat, browse_sub, browse_search], outputs=[browse_table])
        browse_search.submit(browse_accessories, inputs=[browse_cat, browse_sub, browse_search], outputs=[browse_table])
        browse_cat.change(browse_accessories, inputs=[browse_cat, browse_sub, browse_search], outputs=[browse_table])
        browse_sub.change(browse_accessories, inputs=[browse_cat, browse_sub, browse_search], outputs=[browse_table])
        detail_btn.click(view_accessory_detail, inputs=[detail_id], outputs=[detail_output, detail_image])

        refresh_map_btn.click(get_mapping_table, outputs=[mapping_table])
        map_btn.click(add_mapping, inputs=[map_acc, map_prod, map_frame], outputs=[map_status])
        prod_search_btn.click(search_products, inputs=[prod_search], outputs=[prod_results])

        export_btn.click(export_data, inputs=[export_fmt], outputs=[export_file, export_status])

    return demo


if __name__ == "__main__":
    demo = create_app()
    demo.launch(server_name="0.0.0.0", server_port=7863, share=False)
