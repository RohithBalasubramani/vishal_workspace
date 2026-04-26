"""Image Extractor — Extract product images from catalog PDFs and link to DB.

Extracts:
1. Embedded images (product photos, diagrams) from PDF pages
2. Page renders (full page screenshots) as fallback
3. Links images to products by matching page proximity

Stores image paths in products.image_url, alternate_image1, alternate_image2
(mitsubishi_test format supports up to 3 images per product).
"""

import os
from math import sqrt
import fitz
from PIL import Image
import psycopg2
from rapidfuzz import fuzz

from .db import DB_PARAMS, is_file_processed, mark_file_processed

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
IMAGE_DIR = os.path.join(PROJECT_DIR, "data", "images", "products")
PAGE_DIR = os.path.join(PROJECT_DIR, "data", "images", "pages")

os.makedirs(IMAGE_DIR, exist_ok=True)
os.makedirs(PAGE_DIR, exist_ok=True)

MIN_IMAGE_SIZE = 40    # Skip images smaller than 40x40 (icons, bullets)
MIN_IMAGE_AREA = 2000  # Skip images with area < 2000 px (tiny icons)
MAX_ASPECT_RATIO = 5   # Skip banners/strips with extreme aspect ratios


def _get_db():
    from .db import get_db
    return get_db()


def extract_images_from_pdf(pdf_path, output_dir=None):
    """Extract all embedded images from a PDF.

    Returns list of dicts: [{path, page, x, y, width, height, index}]
    """
    if output_dir is None:
        basename = os.path.splitext(os.path.basename(pdf_path))[0]
        output_dir = os.path.join(IMAGE_DIR, basename)
    os.makedirs(output_dir, exist_ok=True)

    doc = fitz.open(pdf_path)
    extracted = []

    for page_idx in range(len(doc)):
        page = doc[page_idx]
        image_list = page.get_images(full=True)

        for img_idx, img_info in enumerate(image_list):
            xref = img_info[0]

            try:
                base_image = doc.extract_image(xref)
                if not base_image:
                    continue

                img_bytes = base_image["image"]
                img_ext = base_image.get("ext", "png")
                width = base_image.get("width", 0)
                height = base_image.get("height", 0)

                # Skip non-product images
                if width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE:
                    continue
                if width * height < MIN_IMAGE_AREA:
                    continue
                aspect = max(width, height) / max(min(width, height), 1)
                if aspect > MAX_ASPECT_RATIO:
                    continue
                page_rect = page.rect
                if width > page_rect.width * 0.9 and height > page_rect.height * 0.9:
                    continue

                img_name = f"page{page_idx + 1}_img{img_idx}.{img_ext}"
                img_path = os.path.join(output_dir, img_name)

                with open(img_path, "wb") as f:
                    f.write(img_bytes)

                rects = page.get_image_rects(xref)
                x, y = 0, 0
                if rects:
                    rect = rects[0]
                    x, y = rect.x0, rect.y0

                extracted.append({
                    "path": img_path,
                    "page": page_idx + 1,
                    "x": x,
                    "y": y,
                    "width": width,
                    "height": height,
                    "index": img_idx,
                })

            except Exception as e:
                print(f"  [Image] Failed to extract xref {xref} on page {page_idx + 1}: {e}")
                continue

    doc.close()
    print(f"  [Image] Extracted {len(extracted)} images from {os.path.basename(pdf_path)}")

    # Fallback: for pages with no extracted images, crop layout-detected image blocks
    pages_with_images = {img["page"] for img in extracted}
    doc = fitz.open(pdf_path)
    for page_idx in range(len(doc)):
        page_num = page_idx + 1
        if page_num in pages_with_images:
            continue

        page = doc[page_idx]
        img_blocks = [b for b in page.get_text("dict")["blocks"] if b["type"] == 1]
        if not img_blocks:
            continue

        best_block = max(img_blocks, key=lambda b: (b["bbox"][2] - b["bbox"][0]) * (b["bbox"][3] - b["bbox"][1]))
        bbox = best_block["bbox"]
        bw = bbox[2] - bbox[0]
        bh = bbox[3] - bbox[1]

        if bw < 30 or bh < 30:
            continue

        clip = fitz.Rect(bbox)
        clip = clip + fitz.Rect(-5, -5, 5, 5)
        clip = clip & page.rect

        zoom = 300 / 72
        mat = fitz.Matrix(zoom, zoom)
        pix = page.get_pixmap(matrix=mat, clip=clip)

        img_name = f"page{page_num}_crop0.png"
        img_path = os.path.join(output_dir, img_name)
        pix.save(img_path)

        extracted.append({
            "path": img_path,
            "page": page_num,
            "x": bbox[0],
            "y": bbox[1],
            "width": pix.width,
            "height": pix.height,
            "index": 0,
        })
        print(f"  [Image] Cropped product image from page {page_num} ({pix.width}x{pix.height})")

    doc.close()
    return extracted


def render_pages(pdf_path, output_dir=None, dpi=150):
    """Render each PDF page as a full screenshot."""
    if output_dir is None:
        output_dir = PAGE_DIR
    os.makedirs(output_dir, exist_ok=True)

    doc = fitz.open(pdf_path)
    rendered = []
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)

    for page_idx in range(len(doc)):
        pix = doc[page_idx].get_pixmap(matrix=mat)
        img_name = f"page_{page_idx + 1}.png"
        img_path = os.path.join(output_dir, img_name)
        pix.save(img_path)

        rendered.append({
            "path": img_path,
            "page": page_idx + 1,
            "width": pix.width,
            "height": pix.height,
        })

    doc.close()
    print(f"  [Image] Rendered {len(rendered)} pages from {os.path.basename(pdf_path)}")
    return rendered


def _is_product_image(img):
    """Filter out decorative/background images — keep only product photos."""
    w, h = img["width"], img["height"]
    area = w * h
    path = img.get("path", "")

    if area < MIN_IMAGE_AREA:
        return False
    if area > 500000:
        return False
    aspect = max(w, h) / max(min(w, h), 1)
    if aspect > 3.5:
        return False

    if path and os.path.exists(path):
        try:
            import numpy as np
            pil_img = Image.open(path).convert("RGB")
            pixels = np.array(pil_img)

            std = pixels.std()
            mean_brightness = pixels.mean()

            if std < 20:
                return False
            if mean_brightness < 50 and std < 40:
                return False

            quantized = (pixels // 32).reshape(-1, 3)
            from collections import Counter
            pixel_counts = Counter(map(tuple, quantized))
            dominant_pct = pixel_counts.most_common(1)[0][1] / len(quantized)
            if dominant_pct > 0.75:
                return False

        except Exception:
            pass

    return True


def _build_text_positions(pdf_path):
    """Scan each PDF page and return {page_num: [(text, x_center, y_center), ...]}."""
    doc = fitz.open(pdf_path)
    result = {}
    for page_idx in range(len(doc)):
        page = doc[page_idx]
        entries = []
        for block in page.get_text("dict")["blocks"]:
            if block["type"] != 0:
                continue
            for line in block["lines"]:
                for span in line["spans"]:
                    txt = span["text"].strip()
                    if txt:
                        bbox = span["bbox"]
                        x_center = (bbox[0] + bbox[2]) / 2
                        y_center = (bbox[1] + bbox[3]) / 2
                        entries.append((txt, x_center, y_center))
        result[page_idx + 1] = entries
    doc.close()
    return result


def _find_model_position(model, text_positions):
    """Find which page and (x, y) position a product model appears at."""
    clean_model = model.rstrip("*").strip()
    for page_num, entries in text_positions.items():
        for txt, x, y in entries:
            if fuzz.partial_ratio(clean_model, txt) > 85:
                return page_num, x, y
    return None, None, None


def _nearest_images(text_pos, page_images, top_n=3):
    """Find the top-N nearest product images to text_pos (x, y)."""
    tx, ty = text_pos
    candidates = []

    for img in page_images:
        ix = img["x"] + img["width"] / 2
        iy = img["y"] + img["height"] / 2

        if abs(iy - ty) > 300:
            continue

        dist = sqrt((ix - tx) ** 2 + (iy - ty) ** 2)
        candidates.append((img, dist))

    candidates.sort(key=lambda x: x[1])
    return candidates[:top_n]


def link_images_to_products(images, filename, pdf_path=None):
    """Link extracted product images to products in the DB.

    Uses text-proximity matching. Supports up to 3 images per product
    (image_url, alternate_image1, alternate_image2) per mitsubishi_test format.

    Returns number of products updated.
    """
    conn = _get_db()
    updated = 0

    text_positions = {}
    if pdf_path and os.path.exists(pdf_path):
        text_positions = _build_text_positions(pdf_path)

    try:
        with conn.cursor() as cur:
            # Find products from this source file
            cur.execute("""
                SELECT p.id, p.product_name, p.product_model, p.category
                FROM products p
                JOIN product_specs s ON s.product_id = p.id
                WHERE s.spec_key = 'Source Document' AND s.spec_value = %s
                AND p.image_url IS NULL
                ORDER BY p.id
            """, (filename,))
            products = cur.fetchall()

            if not products:
                cur.execute("""
                    SELECT p.id, p.product_name, p.product_model, p.category
                    FROM products p
                    WHERE p.image_url IS NULL
                    ORDER BY p.id
                """)
                products = cur.fetchall()

            if not products or not images:
                return 0

            # Filter to actual product images and group by page
            product_images = [img for img in images if _is_product_image(img)]
            images_by_page = {}
            for img in product_images:
                images_by_page.setdefault(img["page"], []).append(img)

            # Deduplicate: detect repeated images (logos/watermarks) by file hash
            import hashlib
            image_hashes = {}
            for img in product_images:
                try:
                    h = hashlib.md5(open(img["path"], "rb").read()).hexdigest()
                    image_hashes.setdefault(h, []).append(img["path"])
                except OSError:
                    pass
            logo_paths = set()
            for h, paths in image_hashes.items():
                if len(paths) >= 3:
                    logo_paths.update(paths)
                    print(f"  [Image] Excluded repeated image (logo/watermark): {os.path.basename(paths[0])} ({len(paths)} copies)")
            if logo_paths:
                for page_num in images_by_page:
                    images_by_page[page_num] = [img for img in images_by_page[page_num] if img["path"] not in logo_paths]

            # Link images: assign up to 3 nearest images per product
            for pid, pname, pmodel, category in products:
                if not text_positions:
                    continue

                page_num, x_pos, y_pos = _find_model_position(pmodel, text_positions)

                if not page_num or page_num not in images_by_page:
                    continue

                nearest = _nearest_images((x_pos, y_pos), images_by_page[page_num], top_n=3)
                if not nearest:
                    continue

                # Assign image_url (primary), alternate_image1, alternate_image2
                img_paths = [n[0]["path"] for n in nearest]

                update_parts = []
                params = []
                if len(img_paths) >= 1:
                    update_parts.append("image_url = %s")
                    params.append(img_paths[0])
                if len(img_paths) >= 2:
                    update_parts.append("alternate_image1 = %s")
                    params.append(img_paths[1])
                if len(img_paths) >= 3:
                    update_parts.append("alternate_image2 = %s")
                    params.append(img_paths[2])

                if update_parts:
                    params.append(pid)
                    cur.execute(
                        f"UPDATE products SET {', '.join(update_parts)} WHERE id = %s AND image_url IS NULL",
                        params,
                    )
                    if cur.rowcount > 0:
                        updated += 1

        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"  [Image] DB error: {e}")
    finally:
        conn.close()

    print(f"  [Image] Images extracted: {len(images)}")
    print(f"  [Image] Products linked: {updated}")
    return updated


def process_pdf_images(pdf_path):
    """Full pipeline: extract images from PDF -> link to products in DB."""
    filename = os.path.basename(pdf_path)
    print(f"\n[Image Extraction] {filename}")

    already_done, prev_record = is_file_processed(pdf_path)
    if already_done and prev_record.get("images_linked", 0) > 0:
        print(f"  [Image] Already processed on {prev_record['processed_at']} — "
              f"{prev_record['images_linked']} images linked previously")
        return {
            "filename": filename,
            "already_processed": True,
            "previous": prev_record,
        }

    images = extract_images_from_pdf(pdf_path)
    pages = render_pages(pdf_path)
    linked = link_images_to_products(images, filename, pdf_path=pdf_path)
    mark_file_processed(pdf_path, filename, images_linked=linked)

    stats = {
        "filename": filename,
        "embedded_images": len(images),
        "page_renders": len(pages),
        "products_linked": linked,
    }
    print(f"  Result: {stats}")
    return stats


def get_product_with_image(product_model):
    """Fetch a product with all its images and specs."""
    conn = _get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, product_name, product_model, description, image_url,
                       category, subcategory, brand, alternate_image1, alternate_image2
                FROM products WHERE product_model = %s
            """, (product_model,))
            row = cur.fetchone()
            if not row:
                return None

            pid, name, model, desc, image_url, category, subcategory, brand, alt1, alt2 = row

            cur.execute("""
                SELECT spec_key, spec_value, spec_group FROM product_specs
                WHERE product_id = %s ORDER BY spec_group, spec_key
            """, (pid,))
            specs = {}
            for k, v, g in cur.fetchall():
                specs[k] = {"value": v, "group": g}

            return {
                "id": pid,
                "product_name": name,
                "product_model": model,
                "description": desc,
                "image_url": image_url,
                "alternate_image1": alt1,
                "alternate_image2": alt2,
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "specs": specs,
            }
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        process_pdf_images(sys.argv[1])
    else:
        print("Usage: python image_extractor.py <pdf_path>")
