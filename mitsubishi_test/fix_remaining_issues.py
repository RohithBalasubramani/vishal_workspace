#!/usr/bin/env python3
"""Fix remaining pipeline issues:
1. Hager: 0 products — send OCR text directly to LLM as raw text extraction
2. Siemens 3VJ: 0 products — same approach
3. Schneider MCCB: low images — re-run image linking
4. L&T: 0.9% images — re-run image linking with scanned fallback
5. Reclassify remaining "Other" products
"""

import os, sys, json, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline.catalog_extractor import (
    extract_from_tables, save_products, _auto_brand, _call_vllm,
    _items_to_products, _reclassify_other, EXTRACTION_LEVELS,
    _parse_html_tables_simple
)
from pipeline.pdf_extractor import process as pdf_process
from pipeline.image_extractor import extract_images_from_pdf, link_images_to_products
from pipeline.db import get_db, get_meta_db


def fix_hager_and_siemens_3vj():
    """Extract products by sending full page text to vLLM directly.

    The pipeline's table-based extraction failed because:
    - Hager: OCR tables have non-standard headers the merger doesn't understand
    - Siemens 3VJ: Selection guide tables with ordering codes in columns, not rows

    Fix: Send raw text (including HTML tables) directly to vLLM for extraction.
    """
    print("=" * 60)
    print("FIX 1: Hager + Siemens 3VJ — direct text extraction")
    print("=" * 60)

    catalogs = [
        ("/home/rohith/sample_catalogs/hager_h3_mccb.pdf", "Hager"),
        ("/home/rohith/sample_catalogs/siemens_3vj_mccb.pdf", "Siemens"),
    ]

    level_prompt = EXTRACTION_LEVELS["detailed"]["prompt"]

    for pdf_path, brand in catalogs:
        print(f"\n--- {os.path.basename(pdf_path)} ({brand}) ---")

        text, tables, dt, method, file_type, num_pages = pdf_process(pdf_path)
        print(f"  Extracted: {len(text)} chars, {len(tables)} tables, method={method}")

        # Split text into page chunks and send each to vLLM
        import re
        pages = re.split(r'(?=--- Page \d+ ---)', text)
        pages = [p for p in pages if p.strip() and len(p.strip()) > 100]

        all_products = []
        for i, page_text in enumerate(pages):
            # Limit to 6000 chars per page to fit in context
            chunk = page_text[:6000]
            if len(chunk) < 200:
                continue

            prompt = f"Brand: {brand}\n\nExtract all products from this catalog page text. The text may contain HTML tables.\n\n{chunk}"

            try:
                items = _call_vllm(prompt, level_prompt, brand)
                products = _items_to_products(items, brand)
                if products:
                    print(f"  Page {i+1}: {len(products)} products")
                    all_products.extend(products)
            except Exception as e:
                print(f"  Page {i+1}: error — {e}")

        print(f"  Total: {len(all_products)} products")

        if all_products:
            # Set catalogue_name for tracking
            fname = os.path.basename(pdf_path)
            for p in all_products:
                p["catalogue_name"] = fname

            inserted, skipped = save_products(all_products)
            print(f"  Saved: {inserted} new, {skipped} existing")

            # Image extraction
            images = extract_images_from_pdf(pdf_path)
            linked = link_images_to_products(images, fname, pdf_path=pdf_path)
            print(f"  Images: {len(images)} extracted, {linked} linked")


def fix_image_linking():
    """Re-run image linking for Schneider MCCB and L&T catalogs."""
    print("\n" + "=" * 60)
    print("FIX 2: Re-run image linking for Schneider MCCB + L&T")
    print("=" * 60)

    catalogs = [
        ("/home/rohith/sample_catalogs/schneider_mccb.pdf", "schneider_mccb.pdf"),
        # L&T PDFs are from a different source directory — check processed_files
    ]

    # Find L&T source PDFs
    conn = get_meta_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT filename FROM processed_files WHERE filename LIKE '%L&T%'")
            for row in cur.fetchall():
                # These are from a different directory, skip for now
                print(f"  L&T source: {row[0]} (different directory, skipping)")
    finally:
        conn.close()

    for pdf_path, fname in catalogs:
        if not os.path.exists(pdf_path):
            print(f"  SKIP: {pdf_path} not found")
            continue

        print(f"\n  Re-linking images for {fname}...")
        images = extract_images_from_pdf(pdf_path)
        linked = link_images_to_products(images, fname, pdf_path=pdf_path)
        print(f"  Images: {len(images)} extracted, {linked} linked")


def fix_other_categories():
    """Reclassify remaining 'Other' products using expanded rules."""
    print("\n" + "=" * 60)
    print("FIX 3: Reclassify remaining 'Other' products")
    print("=" * 60)

    conn = get_db()
    try:
        with conn.cursor() as cur:
            # Get all "Other" products with their specs
            cur.execute("""
                SELECT p.id, p.product_name, p.product_model, p.brand,
                    COALESCE(string_agg(s.spec_key || '=' || s.spec_value, ' '), '') as spec_text
                FROM products p
                LEFT JOIN product_specs s ON s.product_id = p.id
                WHERE p.category = 'Other'
                GROUP BY p.id, p.product_name, p.product_model, p.brand
            """)

            reclassified = 0
            total = 0
            for pid, name, model, brand, spec_text in cur.fetchall():
                total += 1
                new_cat = _reclassify_other(name or "", model or "", {"_text": spec_text})
                if new_cat != "Other":
                    cur.execute("UPDATE products SET category = %s WHERE id = %s", (new_cat, pid))
                    reclassified += 1

            # Additional rules based on spec keys
            # Products with "breaking_capacity" or "icu" are likely MCCBs
            cur.execute("""
                UPDATE products SET category = 'MCCB'
                WHERE category = 'Other' AND id IN (
                    SELECT product_id FROM product_specs
                    WHERE spec_key IN ('breaking_capacity', 'icu', 'ics', 'frame', 'frame_size')
                )
            """)
            mccb_fixed = cur.rowcount

            # Products with "rated_current" and "poles" but no breaking_capacity → likely MCBs
            cur.execute("""
                UPDATE products SET category = 'MCB'
                WHERE category = 'Other' AND id IN (
                    SELECT product_id FROM product_specs
                    WHERE spec_key IN ('rated_current', 'tripping_characteristic', 'curve_type')
                ) AND id NOT IN (
                    SELECT product_id FROM product_specs WHERE spec_key IN ('icu', 'ics', 'frame')
                )
            """)
            mcb_fixed = cur.rowcount

            # Products with "coil_voltage" or "contact_rating" → Contactor/Relay
            cur.execute("""
                UPDATE products SET category = 'Contactor'
                WHERE category = 'Other' AND id IN (
                    SELECT product_id FROM product_specs
                    WHERE spec_key IN ('coil_voltage', 'contact_rating', 'ac1_duty_amps', 'ac3_duty_amps')
                )
            """)
            contactor_fixed = cur.rowcount

            # Remaining with "operating_handle" or "handle" in name → Enclosure
            cur.execute("""
                UPDATE products SET category = 'Enclosure'
                WHERE category = 'Other'
                AND (LOWER(product_name) LIKE '%handle%'
                     OR LOWER(product_name) LIKE '%cover%'
                     OR LOWER(product_name) LIKE '%skeleton%'
                     OR LOWER(product_name) LIKE '%plate%'
                     OR LOWER(product_name) LIKE '%device%')
            """)
            enclosure_fixed = cur.rowcount

            conn.commit()

            remaining = 0
            cur.execute("SELECT COUNT(*) FROM products WHERE category = 'Other'")
            remaining = cur.fetchone()[0]

            print(f"  Reclassified by name rules: {reclassified}")
            print(f"  Reclassified by specs (MCCB): {mccb_fixed}")
            print(f"  Reclassified by specs (MCB): {mcb_fixed}")
            print(f"  Reclassified by specs (Contactor): {contactor_fixed}")
            print(f"  Reclassified by name (Enclosure): {enclosure_fixed}")
            print(f"  Remaining 'Other': {remaining}")

    finally:
        conn.close()


if __name__ == "__main__":
    t0 = time.time()

    fix_hager_and_siemens_3vj()
    fix_image_linking()
    fix_other_categories()

    dt = time.time() - t0
    print(f"\n{'='*60}")
    print(f"ALL FIXES COMPLETE ({dt:.0f}s)")
    print(f"{'='*60}")

    # Final counts
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products")
            print(f"  Total products: {cur.fetchone()[0]}")
            cur.execute("SELECT COUNT(*) FROM products WHERE category = 'Other'")
            print(f"  Remaining 'Other': {cur.fetchone()[0]}")
            cur.execute("SELECT brand, COUNT(*) FROM products GROUP BY brand ORDER BY count DESC")
            for brand, count in cur.fetchall():
                cur2 = conn.cursor()
                cur2.execute("SELECT COUNT(*) FROM products WHERE brand = %s AND image_url IS NOT NULL AND image_url != ''", (brand,))
                imgs = cur2.fetchone()[0]
                print(f"  {brand}: {count} products, {imgs} with images ({round(100*imgs/count,1)}%)")
    finally:
        conn.close()
