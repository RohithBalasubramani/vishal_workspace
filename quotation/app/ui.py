"""Quotation Builder — Gradio UI.

Tabs:
1. Build Quote — search catalog (interactive card grid), build cart, persist, export PDF/Excel
2. Saved Quotes — list, load, duplicate, delete
3. Catalog Browse — browse + add to cart with the same card grid
"""

from __future__ import annotations

import os
import sys
from datetime import date
from functools import lru_cache

import gradio as gr
import pandas as pd
from PIL import Image, ImageDraw

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_DIR)

from pipeline.db import (
    create_quotation,
    delete_quotation,
    get_quotation,
    list_accessory_categories,
    list_product_brands,
    list_product_categories,
    list_quotations,
    lookup_item,
    search_accessories,
    search_products,
    update_quotation,
)
from pipeline.calculator import compute_totals, format_inr
from pipeline.exporter import export_excel, export_pdf

CART_COLUMNS = [
    "Type",
    "Model",
    "Description",
    "Brand",
    "Category",
    "Unit Price",
    "Qty",
    "Disc %",
    "Line Total",
]

QUOTES_COLUMNS = ["ID", "Quote #", "Customer", "Project", "Date", "Status", "Lines", "Updated"]
SEARCH_COLUMNS = ["#", "Type", "Model", "Description", "Brand", "Category", "Unit Price"]
BROWSE_COLUMNS = ["Model", "Description", "Brand", "Unit Price"]
BROWSE_COLUMN_WIDTHS = ["140px", None, "90px", "110px"]

MAX_GALLERY_ITEMS = 60  # cap browse gallery thumbnails to keep the page snappy


@lru_cache(maxsize=1)
def _placeholder_image() -> Image.Image:
    img = Image.new("RGB", (240, 180), color=(236, 238, 242))
    draw = ImageDraw.Draw(img)
    text = "no image"
    bbox = draw.textbbox((0, 0), text)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text(((240 - tw) / 2, (180 - th) / 2), text, fill=(150, 154, 160))
    return img


def _resolve_image(path: str | None):
    """Return a Gallery-compatible image (file path) or PIL placeholder."""
    if not path:
        return _placeholder_image()
    if os.path.exists(path):
        return path
    return _placeholder_image()


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────

def _cart_to_df(cart: list[dict]) -> pd.DataFrame:
    totals = compute_totals(cart, gst_rate=0)
    rows = []
    for it in totals["items"]:
        rows.append({
            "Type": (it.get("item_type") or "").capitalize(),
            "Model": it.get("source_model", ""),
            "Description": it.get("item_name", ""),
            "Brand": it.get("brand", ""),
            "Category": it.get("category", ""),
            "Unit Price": float(it.get("unit_price", 0) or 0),
            "Qty": float(it.get("quantity", 1) or 0),
            "Disc %": float(it.get("discount_pct", 0) or 0),
            "Line Total": float(it.get("line_total", 0) or 0),
        })
    if not rows:
        return pd.DataFrame(columns=CART_COLUMNS)
    return pd.DataFrame(rows, columns=CART_COLUMNS)


def _df_to_cart(df, prev_cart: list[dict]) -> list[dict]:
    if df is None:
        return []
    if hasattr(df, "to_dict"):
        rows = df.to_dict(orient="records")
    else:
        rows = list(df) if df else []
    if not rows:
        return []
    new_cart = []
    for i, row in enumerate(rows):
        base = prev_cart[i] if i < len(prev_cart) else {}
        try:
            qty = float(row.get("Qty", 1) or 0)
        except (TypeError, ValueError):
            qty = 1.0
        try:
            disc = float(row.get("Disc %", 0) or 0)
        except (TypeError, ValueError):
            disc = 0.0
        try:
            unit = float(row.get("Unit Price", base.get("unit_price", 0)) or 0)
        except (TypeError, ValueError):
            unit = float(base.get("unit_price", 0) or 0)
        new_cart.append({
            "item_type": base.get("item_type", str(row.get("Type", "")).lower() or "product"),
            "source_model": base.get("source_model", row.get("Model", "")),
            "item_name": base.get("item_name", row.get("Description", "")),
            "brand": base.get("brand", row.get("Brand", "")),
            "category": base.get("category", row.get("Category", "")),
            "unit_price": unit,
            "quantity": qty,
            "discount_pct": disc,
        })
    return new_cart


def _format_totals_markdown(cart: list[dict], gst_rate) -> str:
    rate = float(gst_rate or 0)
    t = compute_totals(cart, gst_rate=rate)
    lines = [
        "### Totals",
        f"- **Items:** {len(t['items'])}",
        f"- **Subtotal:** {format_inr(t['subtotal'])}",
        f"- **CGST ({t['gst_rate']/2:g}%):** {format_inr(t['cgst'])}",
        f"- **SGST ({t['gst_rate']/2:g}%):** {format_inr(t['sgst'])}",
        f"- **GST total:** {format_inr(t['gst_amount'])}",
        f"### Grand Total: {format_inr(t['grand_total'])}",
    ]
    return "\n".join(lines)


def _search_results_df(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame(columns=SEARCH_COLUMNS)
    formatted = []
    for i, r in enumerate(rows):
        formatted.append({
            "#": i,
            "Type": (r.get("item_type") or "").capitalize(),
            "Model": r.get("source_model", ""),
            "Description": r.get("item_name", ""),
            "Brand": r.get("brand", ""),
            "Category": r.get("category", ""),
            "Unit Price": format_inr(r.get("unit_price", 0)),
        })
    return pd.DataFrame(formatted, columns=SEARCH_COLUMNS)


def _browse_results_df(rows: list[dict]) -> pd.DataFrame:
    """Compact dataframe for the Browse tab — single-line rows, no Type column."""
    if not rows:
        return pd.DataFrame(columns=BROWSE_COLUMNS)
    formatted = []
    for r in rows:
        formatted.append({
            "Model": r.get("source_model", ""),
            "Description": r.get("item_name", ""),
            "Brand": r.get("brand", ""),
            "Unit Price": format_inr(r.get("unit_price", 0)),
        })
    return pd.DataFrame(formatted, columns=BROWSE_COLUMNS)


def _preview_md(item: dict | None) -> str:
    if not item:
        return "_Click a row to preview a product here._"
    lines = [
        f"### `{item.get('source_model','')}`",
        f"**{item.get('item_name','')}**",
        "",
        f"- **Type:** {(item.get('item_type') or '').capitalize()}",
        f"- **Brand:** {item.get('brand') or '—'}",
        f"- **Category:** {item.get('category') or '—'}",
        f"- **Unit price:** {format_inr(item.get('unit_price', 0))}",
    ]
    return "\n".join(lines)


def _preview_image(item: dict | None):
    """Return an image (path or PIL) for the row-preview component, or None."""
    if not item:
        return None
    path = item.get("image_url")
    if path and os.path.exists(path):
        return path
    return None


def _gallery_items(rows: list[dict]) -> list:
    """Build gr.Gallery items: (image_path_or_PIL, caption) per row."""
    items = []
    for i, r in enumerate(rows[:MAX_GALLERY_ITEMS]):
        img = _resolve_image(r.get("image_url"))
        caption = (
            f"#{i} · {r.get('source_model','')} · "
            f"{format_inr(r.get('unit_price', 0))}\n"
            f"{(r.get('brand') or '')} · {(r.get('category') or '')}"
        )
        items.append((img, caption))
    return items


def _quotes_list_df() -> pd.DataFrame:
    rows = list_quotations(limit=500)
    if not rows:
        return pd.DataFrame(columns=QUOTES_COLUMNS)
    formatted = []
    for r in rows:
        formatted.append({
            "ID": r[0],
            "Quote #": r[1] or "",
            "Customer": r[2] or "",
            "Project": r[3] or "",
            "Date": str(r[4] or ""),
            "Status": r[5] or "",
            "Lines": int(r[6] or 0),
            "Updated": str(r[7] or "")[:19],
        })
    return pd.DataFrame(formatted, columns=QUOTES_COLUMNS)


def _add_item_to_cart(item: dict, cart: list[dict]) -> list[dict]:
    cart = list(cart or [])
    cart.append({
        "item_type": item.get("item_type", "product"),
        "source_model": item.get("source_model", ""),
        "item_name": item.get("item_name", ""),
        "brand": item.get("brand", ""),
        "category": item.get("category", ""),
        "unit_price": float(item.get("unit_price", 0) or 0),
        "quantity": 1.0,
        "discount_pct": 0.0,
    })
    return cart


# ──────────────────────────────────────────────────────────────
# Search & catalog handlers
# ──────────────────────────────────────────────────────────────

def do_search(source, query, p_category, p_brand, a_category):
    products = []
    accessories = []
    if source in ("Products", "Both"):
        products = search_products(query=query, category=p_category or "", brand=p_brand or "", limit=300)
    if source in ("Accessories", "Both"):
        accessories = search_accessories(query=query, category=a_category or "", limit=300)
    combined = products + accessories
    return (combined, _search_results_df(combined), None,
            _preview_md(None), _preview_image(None),
            f"Found {len(combined)} item(s).")


def do_browse(source, query, pcat, pbrand, acat):
    if source == "Products":
        rows = search_products(query=query, category=pcat or "", brand=pbrand or "", limit=200)
    else:
        rows = search_accessories(query=query, category=acat or "", limit=200)
    hint = f"_{len(rows)} item(s) found._"
    if len(rows) >= 200:
        hint = "_Showing first 200 results — refine the filters to narrow down._"
    return (rows, _browse_results_df(rows), None,
            _preview_md(None), _preview_image(None),
            hint)


def _extract_index(evt) -> int | None:
    if evt is None or evt.index is None:
        return None
    if isinstance(evt.index, (list, tuple)):
        return int(evt.index[0]) if len(evt.index) > 0 else None
    try:
        return int(evt.index)
    except (TypeError, ValueError):
        return None


def on_select_row(results, evt: gr.SelectData):
    """User clicked a row in a results dataframe. Return (selected_idx, preview_md, preview_image)."""
    if not results:
        return None, _preview_md(None), _preview_image(None)
    idx = _extract_index(evt)
    if idx is None or idx < 0 or idx >= len(results):
        return None, _preview_md(None), _preview_image(None)
    return idx, _preview_md(results[idx]), _preview_image(results[idx])




def add_selected_to_cart(selected_idx, results, cart, gst_rate):
    if selected_idx is None or not results:
        return _cart_to_df(cart), cart, _format_totals_markdown(cart, gst_rate), "Click a row in the results first."
    try:
        idx = int(selected_idx)
    except (TypeError, ValueError):
        return _cart_to_df(cart), cart, _format_totals_markdown(cart, gst_rate), "No row selected."
    if idx < 0 or idx >= len(results):
        return _cart_to_df(cart), cart, _format_totals_markdown(cart, gst_rate), "Selection out of range."
    item = results[idx]
    new_cart = _add_item_to_cart(item, cart)
    return (_cart_to_df(new_cart), new_cart,
            _format_totals_markdown(new_cart, gst_rate),
            f"Added **{item.get('source_model','')}** to cart.")


def add_by_model(model_input, search_results, cart, gst_rate):
    model = (model_input or "").strip()
    if not model:
        return _cart_to_df(cart), cart, _format_totals_markdown(cart, gst_rate), "Type a model code to add."
    # Try the current search results first
    for row in (search_results or []):
        if row["source_model"].lower() == model.lower():
            new_cart = _add_item_to_cart(row, cart)
            return (_cart_to_df(new_cart), new_cart,
                    _format_totals_markdown(new_cart, gst_rate),
                    f"Added {row['source_model']}.")
    # Fallback: catalog lookup
    for it_type in ("product", "accessory"):
        row = lookup_item(it_type, model)
        if row:
            new_cart = _add_item_to_cart(row, cart)
            return (_cart_to_df(new_cart), new_cart,
                    _format_totals_markdown(new_cart, gst_rate),
                    f"Added {row['source_model']} ({it_type}).")
    return (_cart_to_df(cart), cart,
            _format_totals_markdown(cart, gst_rate),
            f"Model '{model}' not found.")


def remove_from_cart(remove_indices, cart, gst_rate):
    indices: list[int] = []
    for tok in str(remove_indices or "").replace(" ", "").split(","):
        if not tok:
            continue
        try:
            indices.append(int(tok))
        except ValueError:
            continue
    if not indices:
        return (_cart_to_df(cart), cart,
                _format_totals_markdown(cart, gst_rate),
                "Enter row number(s) to remove (e.g. `0` or `1,3`).")
    drop = set(indices)
    new_cart = [c for i, c in enumerate(cart) if i not in drop]
    removed = len(cart) - len(new_cart)
    return (_cart_to_df(new_cart), new_cart,
            _format_totals_markdown(new_cart, gst_rate),
            f"Removed {removed} item(s).")


def clear_cart(gst_rate):
    return _cart_to_df([]), [], _format_totals_markdown([], gst_rate), "Cart cleared."


def sync_cart_edits(edited_df, cart, gst_rate):
    new_cart = _df_to_cart(edited_df, cart)
    return _cart_to_df(new_cart), new_cart, _format_totals_markdown(new_cart, gst_rate)


def refresh_totals(cart, gst_rate):
    return _format_totals_markdown(cart, gst_rate)


# ──────────────────────────────────────────────────────────────
# Quote persistence handlers
# ──────────────────────────────────────────────────────────────

def _header_from_inputs(quote_id, quote_number, customer, address, project, qdate, valid_until, gst_rate, status, notes):
    return {
        "id": quote_id,
        "quote_number": (quote_number or "").strip(),
        "customer_name": customer,
        "customer_address": address,
        "project_name": project,
        "quote_date": qdate or date.today().isoformat(),
        "valid_until": (valid_until or None),
        "gst_rate": float(gst_rate or 0),
        "status": status or "draft",
        "notes": notes,
    }


def save_quote(quote_id, quote_number, customer, address, project, qdate, valid_until, gst_rate, status, notes, cart):
    if not cart:
        return quote_id, quote_number, "Cannot save an empty quote — add at least one item.", _quotes_list_df()
    header = _header_from_inputs(quote_id, quote_number, customer, address, project, qdate, valid_until, gst_rate, status, notes)
    try:
        if quote_id:
            update_quotation(int(quote_id), header, cart)
            qn = quote_number or ""
            return int(quote_id), qn, f"Quote `{qn}` updated.", _quotes_list_df()
        new_id, qn = create_quotation(header, cart)
        return new_id, qn, f"Quote `{qn}` saved (id={new_id}).", _quotes_list_df()
    except Exception as e:
        return quote_id, quote_number, f"Save failed: {e}", _quotes_list_df()


def export_quote_pdf(quote_number, customer, address, project, qdate, valid_until, gst_rate, status, notes, cart):
    if not cart:
        return None, "Cannot export an empty quote."
    header = _header_from_inputs(None, quote_number, customer, address, project, qdate, valid_until, gst_rate, status, notes)
    if not header["quote_number"]:
        header["quote_number"] = f"DRAFT-{date.today().strftime('%Y%m%d')}"
    try:
        path = export_pdf(header, cart)
        return path, f"PDF generated: `{os.path.basename(path)}`"
    except Exception as e:
        return None, f"PDF export failed: {e}"


def export_quote_excel(quote_number, customer, address, project, qdate, valid_until, gst_rate, status, notes, cart):
    if not cart:
        return None, "Cannot export an empty quote."
    header = _header_from_inputs(None, quote_number, customer, address, project, qdate, valid_until, gst_rate, status, notes)
    if not header["quote_number"]:
        header["quote_number"] = f"DRAFT-{date.today().strftime('%Y%m%d')}"
    try:
        path = export_excel(header, cart)
        return path, f"Excel generated: `{os.path.basename(path)}`"
    except Exception as e:
        return None, f"Excel export failed: {e}"


def new_quote():
    return (
        None, "", "", "", "", date.today().isoformat(), "", 18.0, "draft", "",
        [], _cart_to_df([]), _format_totals_markdown([], 18.0), "New quote started.",
    )


def load_quote(quote_id):
    blank = (gr.update(),) * 13 + ("Enter a valid quote ID to load.",)
    try:
        qid = int(quote_id) if quote_id is not None else None
    except (TypeError, ValueError):
        return blank
    if qid is None:
        return blank
    header, items = get_quotation(qid)
    if not header:
        return (gr.update(),) * 13 + (f"Quote id={qid} not found.",)
    cart = []
    for it in items:
        cart.append({
            "item_type": it["item_type"],
            "source_model": it["source_model"],
            "item_name": it["item_name"],
            "brand": it["brand"],
            "category": it["category"],
            "unit_price": float(it["unit_price"]),
            "quantity": float(it["quantity"]),
            "discount_pct": float(it["discount_pct"]),
        })
    return (
        header["id"],
        header["quote_number"],
        header.get("customer_name") or "",
        header.get("customer_address") or "",
        header.get("project_name") or "",
        str(header.get("quote_date") or ""),
        str(header.get("valid_until") or ""),
        float(header.get("gst_rate") or 18),
        header.get("status") or "draft",
        header.get("notes") or "",
        cart,
        _cart_to_df(cart),
        _format_totals_markdown(cart, float(header.get("gst_rate") or 0)),
        f"Loaded quote `{header['quote_number']}` (id={header['id']}).",
    )


def duplicate_quote(quote_id):
    out = list(load_quote(quote_id))
    if isinstance(out[0], int):
        out[0] = None
        out[1] = ""
        out[-1] = "Duplicated. Edit and Save to assign a new quote number."
    return tuple(out)


def delete_quote(quote_id):
    try:
        qid = int(quote_id) if quote_id is not None else None
    except (TypeError, ValueError):
        return _quotes_list_df(), "Enter a valid quote ID to delete."
    if qid is None:
        return _quotes_list_df(), "Enter a valid quote ID to delete."
    delete_quotation(qid)
    return _quotes_list_df(), f"Deleted quote id={qid}."


# ──────────────────────────────────────────────────────────────
# UI assembly
# ──────────────────────────────────────────────────────────────

def create_app() -> gr.Blocks:
    try:
        product_categories = [""] + list_product_categories()
    except Exception:
        product_categories = [""]
    try:
        product_brands = [""] + list_product_brands()
    except Exception:
        product_brands = [""]
    try:
        accessory_categories = [""] + list_accessory_categories()
    except Exception:
        accessory_categories = [""]

    with gr.Blocks(title="Quotation Builder") as demo:
        gr.Markdown("# 🧾 Quotation Builder")
        gr.Markdown(
            "Search the catalog → click a result row → click **Add to Cart**. "
            "Adjust qty / discount inline, then save or export. Quotes persist to the `quotations` Postgres DB."
        )

        cart_state = gr.State([])
        search_state = gr.State([])
        browse_state = gr.State([])
        search_selected_idx = gr.State(None)
        browse_selected_idx = gr.State(None)
        current_quote_id = gr.State(None)

        # ─── Tab 1: Build Quote ───
        with gr.Tab("Build Quote"):
            with gr.Row():
                # Column 1: header form + actions
                with gr.Column(scale=1):
                    gr.Markdown("### Quote header")
                    quote_number_in = gr.Textbox(label="Quote number", placeholder="auto-generated on save")
                    customer_in = gr.Textbox(label="Customer name")
                    address_in = gr.Textbox(label="Customer address", lines=2)
                    project_in = gr.Textbox(label="Project name")
                    with gr.Row():
                        qdate_in = gr.Textbox(label="Quote date", value=date.today().isoformat())
                        valid_until_in = gr.Textbox(label="Valid until", placeholder="YYYY-MM-DD")
                    with gr.Row():
                        gst_rate_in = gr.Number(label="GST %", value=18, precision=2)
                        status_in = gr.Dropdown(
                            label="Status",
                            choices=["draft", "sent", "accepted", "rejected"],
                            value="draft",
                        )
                    notes_in = gr.Textbox(label="Notes", lines=2)

                    with gr.Row():
                        save_btn = gr.Button("💾 Save quote", variant="primary")
                        new_btn = gr.Button("➕ New")
                    with gr.Row():
                        pdf_btn = gr.Button("📄 Export PDF")
                        xlsx_btn = gr.Button("📊 Export Excel")
                    status_md = gr.Markdown("")
                    pdf_file = gr.File(label="PDF download", visible=True)
                    xlsx_file = gr.File(label="Excel download", visible=True)

                # Column 2: catalog search with card grid
                with gr.Column(scale=2):
                    gr.Markdown("### Search catalog")
                    source_in = gr.Radio(
                        choices=["Products", "Accessories", "Both"],
                        value="Products",
                        label="Source",
                    )
                    search_query_in = gr.Textbox(label="Search (model or name)", placeholder="e.g. CS94205 or MCCB")
                    with gr.Row():
                        p_cat_in = gr.Dropdown(label="Product category", choices=product_categories, value="")
                        p_brand_in = gr.Dropdown(label="Product brand", choices=product_brands, value="")
                    a_cat_in = gr.Dropdown(label="Accessory category", choices=accessory_categories, value="")
                    with gr.Row():
                        search_btn = gr.Button("🔍 Search", variant="primary", scale=1)
                        add_model_in = gr.Textbox(
                            label="Add by model",
                            placeholder="e.g. CS94205",
                            scale=2,
                        )
                        add_model_btn = gr.Button("➕ Add by model", scale=1)

                    gr.Markdown("#### Results — click any row, then **Add to Cart**")
                    results_df = gr.Dataframe(
                        value=_search_results_df([]),
                        headers=SEARCH_COLUMNS,
                        interactive=False,
                        wrap=True,
                        row_count=(0, "dynamic"),
                        label="Search results",
                    )
                    with gr.Row():
                        search_preview_img = gr.Image(
                            label="Preview",
                            height=160,
                            interactive=False,
                            scale=1,
                        )
                        search_preview_md = gr.Markdown(_preview_md(None))
                    add_selected_btn = gr.Button("➕ Add to Cart", variant="primary")

                # Column 3: cart
                with gr.Column(scale=2):
                    gr.Markdown("### Cart (edit Qty and Disc % inline)")
                    cart_df = gr.Dataframe(
                        value=_cart_to_df([]),
                        headers=CART_COLUMNS,
                        datatype=["str", "str", "str", "str", "str", "number", "number", "number", "number"],
                        interactive=True,
                        wrap=True,
                        row_count=(0, "dynamic"),
                        column_count=(len(CART_COLUMNS), "fixed"),
                        label="Line items",
                    )
                    with gr.Row():
                        remove_idx_in = gr.Textbox(
                            label="Row #(s) to remove",
                            placeholder="e.g. 0 or 1,3",
                            scale=2,
                        )
                        remove_btn = gr.Button("🗑 Remove", scale=1)
                        clear_btn = gr.Button("🧹 Clear cart")
                    totals_md = gr.Markdown(_format_totals_markdown([], 18))

        # ─── Tab 2: Saved Quotes ───
        with gr.Tab("Saved Quotes"):
            gr.Markdown("### Saved quotations")
            refresh_quotes_btn = gr.Button("🔄 Refresh list")
            quotes_df = gr.Dataframe(
                value=_quotes_list_df(),
                headers=QUOTES_COLUMNS,
                interactive=False,
                wrap=True,
                label="Quotes",
            )
            with gr.Row():
                qid_in = gr.Number(label="Quote ID", precision=0)
                load_btn = gr.Button("📂 Load to editor", variant="primary")
                dup_btn = gr.Button("📑 Duplicate")
                del_btn = gr.Button("🗑 Delete", variant="stop")
            saved_status = gr.Markdown("")

        # ─── Tab 3: Catalog Browse (details on left, image+actions on right) ───
        with gr.Tab("Catalog Browse"):
            gr.Markdown("### Browse catalog")

            with gr.Row():
                browse_source = gr.Radio(
                    choices=["Products", "Accessories"],
                    value="Products",
                    label="Source",
                    scale=1,
                )
                browse_query = gr.Textbox(
                    label="Search",
                    placeholder="model or name…",
                    scale=3,
                )
                browse_btn = gr.Button("🔍 Browse", variant="primary", scale=1)

            with gr.Accordion("Filters", open=False):
                with gr.Row():
                    browse_pcat = gr.Dropdown(label="Product category", choices=product_categories, value="")
                    browse_pbrand = gr.Dropdown(label="Product brand", choices=product_brands, value="")
                    browse_acat = gr.Dropdown(label="Accessory category", choices=accessory_categories, value="")

            with gr.Row(equal_height=True):
                with gr.Column(scale=3):
                    browse_df = gr.Dataframe(
                        value=_browse_results_df([]),
                        headers=BROWSE_COLUMNS,
                        column_widths=BROWSE_COLUMN_WIDTHS,
                        interactive=False,
                        wrap=False,
                        max_height=480,
                        row_count=(0, "dynamic"),
                        label="Products — click a row to preview",
                    )
                    browse_status = gr.Markdown("_Run a search above to load items._")

                with gr.Column(scale=2):
                    with gr.Group():
                        browse_preview_img = gr.Image(
                            show_label=False,
                            height=220,
                            interactive=False,
                        )
                        browse_preview_md = gr.Markdown(_preview_md(None))
                        browse_add_btn = gr.Button("➕ Add to Cart", variant="primary", size="lg")

        # ──────────────────────────────────────────────────────
        # Wiring
        # ──────────────────────────────────────────────────────

        load_outputs = [
            current_quote_id, quote_number_in, customer_in, address_in,
            project_in, qdate_in, valid_until_in, gst_rate_in, status_in,
            notes_in, cart_state, cart_df, totals_md, saved_status,
        ]

        search_btn.click(
            do_search,
            inputs=[source_in, search_query_in, p_cat_in, p_brand_in, a_cat_in],
            outputs=[search_state, results_df, search_selected_idx,
                     search_preview_md, search_preview_img, status_md],
        )
        results_df.select(
            on_select_row,
            inputs=[search_state],
            outputs=[search_selected_idx, search_preview_md, search_preview_img],
        )
        add_selected_btn.click(
            add_selected_to_cart,
            inputs=[search_selected_idx, search_state, cart_state, gst_rate_in],
            outputs=[cart_df, cart_state, totals_md, status_md],
        )
        add_model_btn.click(
            add_by_model,
            inputs=[add_model_in, search_state, cart_state, gst_rate_in],
            outputs=[cart_df, cart_state, totals_md, status_md],
        )
        remove_btn.click(
            remove_from_cart,
            inputs=[remove_idx_in, cart_state, gst_rate_in],
            outputs=[cart_df, cart_state, totals_md, status_md],
        )
        clear_btn.click(
            clear_cart,
            inputs=[gst_rate_in],
            outputs=[cart_df, cart_state, totals_md, status_md],
        )
        cart_df.input(
            sync_cart_edits,
            inputs=[cart_df, cart_state, gst_rate_in],
            outputs=[cart_df, cart_state, totals_md],
        )
        gst_rate_in.change(
            refresh_totals,
            inputs=[cart_state, gst_rate_in],
            outputs=[totals_md],
        )
        save_btn.click(
            save_quote,
            inputs=[current_quote_id, quote_number_in, customer_in, address_in,
                    project_in, qdate_in, valid_until_in, gst_rate_in, status_in,
                    notes_in, cart_state],
            outputs=[current_quote_id, quote_number_in, status_md, quotes_df],
        )
        pdf_btn.click(
            export_quote_pdf,
            inputs=[quote_number_in, customer_in, address_in, project_in,
                    qdate_in, valid_until_in, gst_rate_in, status_in, notes_in,
                    cart_state],
            outputs=[pdf_file, status_md],
        )
        xlsx_btn.click(
            export_quote_excel,
            inputs=[quote_number_in, customer_in, address_in, project_in,
                    qdate_in, valid_until_in, gst_rate_in, status_in, notes_in,
                    cart_state],
            outputs=[xlsx_file, status_md],
        )
        new_btn.click(
            new_quote,
            outputs=[
                current_quote_id, quote_number_in, customer_in, address_in,
                project_in, qdate_in, valid_until_in, gst_rate_in, status_in,
                notes_in, cart_state, cart_df, totals_md, status_md,
            ],
        )

        refresh_quotes_btn.click(_quotes_list_df, outputs=[quotes_df])
        load_btn.click(load_quote, inputs=[qid_in], outputs=load_outputs)
        dup_btn.click(duplicate_quote, inputs=[qid_in], outputs=load_outputs)
        del_btn.click(delete_quote, inputs=[qid_in], outputs=[quotes_df, saved_status])

        browse_btn.click(
            do_browse,
            inputs=[browse_source, browse_query, browse_pcat, browse_pbrand, browse_acat],
            outputs=[browse_state, browse_df, browse_selected_idx,
                     browse_preview_md, browse_preview_img, browse_status],
        )
        browse_df.select(
            on_select_row,
            inputs=[browse_state],
            outputs=[browse_selected_idx, browse_preview_md, browse_preview_img],
        )
        browse_add_btn.click(
            add_selected_to_cart,
            inputs=[browse_selected_idx, browse_state, cart_state, gst_rate_in],
            outputs=[cart_df, cart_state, totals_md, browse_status],
        )

    return demo


if __name__ == "__main__":
    create_app().launch(server_name="0.0.0.0", server_port=7864, share=False)
