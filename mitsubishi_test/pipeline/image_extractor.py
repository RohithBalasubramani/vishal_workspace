"""Image Extractor — Extract product images from catalog PDFs and link to DB.

Extracts:
1. Embedded images (product photos, diagrams) from PDF pages
2. Page renders (full page screenshots) as fallback
3. Links images to products by matching page proximity
"""

import os
from math import sqrt
import fitz
from PIL import Image
import psycopg2
from rapidfuzz import fuzz

from .db import DB_PARAMS as CATALOG_DB, is_file_processed, mark_file_processed

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


MIN_RENDER_SIZE = 150  # If embedded image is smaller than this, re-render from PDF at high DPI
RENDER_DPI = 300       # DPI for re-rendering small images


def extract_images_from_pdf(pdf_path, output_dir=None):
    """Extract product images from a PDF.

    Strategy:
    1. Extract embedded images (if large enough, use directly)
    2. For small embedded images (<150px), locate their position on the page
       and render that region at 300 DPI for a crisp, high-quality image
    3. For pages with no embedded images, detect image blocks and crop them

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

                # Skip tiny icons/bullets
                if width < MIN_IMAGE_SIZE or height < MIN_IMAGE_SIZE:
                    continue
                if width * height < MIN_IMAGE_AREA:
                    continue
                # Skip banners/strips
                aspect = max(width, height) / max(min(width, height), 1)
                if aspect > MAX_ASPECT_RATIO:
                    continue
                # Skip full-page backgrounds
                page_rect = page.rect
                if width > page_rect.width * 0.9 and height > page_rect.height * 0.9:
                    continue

                # Get position on page
                rects = page.get_image_rects(xref)
                x, y = 0, 0
                rect = None
                if rects:
                    rect = rects[0]
                    x, y = rect.x0, rect.y0

                # If embedded image is too small, render the region at high DPI
                if (width < MIN_RENDER_SIZE or height < MIN_RENDER_SIZE) and rect:
                    clip = fitz.Rect(rect)
                    clip = clip + fitz.Rect(-3, -3, 3, 3)  # Small padding
                    clip = clip & page.rect  # Clamp to page bounds
                    clip_w = clip.width
                    clip_h = clip.height
                    if clip_w > 10 and clip_h > 10:
                        zoom = RENDER_DPI / 72
                        mat = fitz.Matrix(zoom, zoom)
                        pix = page.get_pixmap(matrix=mat, clip=clip)

                        img_name = f"page{page_idx + 1}_img{img_idx}_hd.png"
                        img_path = os.path.join(output_dir, img_name)
                        pix.save(img_path)

                        extracted.append({
                            "path": img_path,
                            "page": page_idx + 1,
                            "x": x, "y": y,
                            "width": pix.width,
                            "height": pix.height,
                            "index": img_idx,
                        })
                        continue

                # Large enough — save embedded image directly
                img_name = f"page{page_idx + 1}_img{img_idx}.{img_ext}"
                img_path = os.path.join(output_dir, img_name)

                with open(img_path, "wb") as f:
                    f.write(img_bytes)

                extracted.append({
                    "path": img_path,
                    "page": page_idx + 1,
                    "x": x, "y": y,
                    "width": width,
                    "height": height,
                    "index": img_idx,
                })

            except Exception as e:
                print(f"  [Image] Failed to extract xref {xref} on page {page_idx + 1}: {e}")
                continue

    doc.close()
    print(f"  [Image] Extracted {len(extracted)} images from {os.path.basename(pdf_path)}")

    # Fallback: for pages with no extracted images, render the page and crop the
    # product image region using layout analysis (product images are in the non-table
    # area, typically upper portion of the page near descriptive text)
    pages_with_images = {img["page"] for img in extracted}
    doc = fitz.open(pdf_path)
    for page_idx in range(len(doc)):
        page_num = page_idx + 1
        if page_num in pages_with_images:
            continue

        page = doc[page_idx]
        # Find image blocks that PyMuPDF detects in the page layout
        # (these may be too small to extract but have position info)
        img_blocks = [b for b in page.get_text("dict")["blocks"] if b["type"] == 1]
        if not img_blocks:
            continue

        # Pick the largest image block by area
        best_block = max(img_blocks, key=lambda b: (b["bbox"][2] - b["bbox"][0]) * (b["bbox"][3] - b["bbox"][1]))
        bbox = best_block["bbox"]
        bw = bbox[2] - bbox[0]
        bh = bbox[3] - bbox[1]

        # Skip very tiny layout blocks (< 30px either side)
        if bw < 30 or bh < 30:
            continue

        # Render just this region at higher DPI for a crisp crop
        clip = fitz.Rect(bbox)
        # Expand clip slightly for padding
        clip = clip + fitz.Rect(-5, -5, 5, 5)
        clip = clip & page.rect  # clamp to page bounds

        zoom = 300 / 72  # 300 DPI
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
    """Render each PDF page as a full screenshot.

    Returns list of dicts: [{path, page, width, height}]
    """
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
    """Filter out decorative/background images — keep only product photos.

    Uses dimension checks + image content analysis (color variance, edge density)
    to distinguish real product photos from backgrounds, banners, and decorative graphics.
    """
    w, h = img["width"], img["height"]
    area = w * h
    path = img.get("path", "")

    # Skip very small images (icons, bullets)
    if area < MIN_IMAGE_AREA:
        return False
    # Skip very large images (full-page backgrounds)
    if area > 500000:
        return False
    # Skip extreme aspect ratios (banners, strips, headers)
    aspect = max(w, h) / max(min(w, h), 1)
    if aspect > 3.5:
        return False

    # Content-based filtering: analyze the actual image pixels
    if path and os.path.exists(path):
        try:
            import numpy as np
            pil_img = Image.open(path).convert("RGB")
            pixels = np.array(pil_img)

            std = pixels.std()
            mean_brightness = pixels.mean()

            # Check 1: Nearly solid color — decorative fill or gradient
            if std < 20:
                return False

            # Check 2: Very dark image — background graphics, circuit patterns
            # Real product photos are usually on light/white backgrounds (mean > 60)
            if mean_brightness < 50 and std < 40:
                return False

            # Check 3: Dominant color — if >75% of pixels are the same color bin, it's not a product
            quantized = (pixels // 32).reshape(-1, 3)
            from collections import Counter
            pixel_counts = Counter(map(tuple, quantized))
            dominant_pct = pixel_counts.most_common(1)[0][1] / len(quantized)
            if dominant_pct > 0.75:
                return False

        except Exception:
            pass  # if analysis fails, let the image through

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
    """Find which page and (x, y) position a product model appears at.

    Uses fuzzy matching (partial_ratio > 85) for robustness.
    Returns (page_num, x_center, y_center) or (None, None, None).
    """
    clean_model = model.rstrip("*").strip()
    for page_num, entries in text_positions.items():
        for txt, x, y in entries:
            if fuzz.partial_ratio(clean_model, txt) > 85:
                return page_num, x, y
    return None, None, None


def _nearest_image(text_pos, page_images):
    """Find the product image nearest to text_pos (x, y) using 2D Euclidean distance.

    Returns single best match (for backward compat). Use _nearest_images() for top-N.
    """
    results = _nearest_images(text_pos, page_images, top_n=1)
    return results[0][0] if results else None


def _nearest_images(text_pos, page_images, top_n=3):
    """Find the top-N nearest product images to text_pos (x, y).

    Returns list of (img_dict, distance) tuples sorted by distance, up to top_n.
    """
    tx, ty = text_pos
    candidates = []

    for img in page_images:
        ix = img["x"] + img["width"] / 2
        iy = img["y"] + img["height"] / 2

        # Vertical band constraint — skip images too far vertically
        if abs(iy - ty) > 300:
            continue

        dist = sqrt((ix - tx) ** 2 + (iy - ty) ** 2)
        candidates.append((img, dist))

    candidates.sort(key=lambda x: x[1])
    return candidates[:top_n]


def link_images_to_products(images, filename, pdf_path=None):
    """Link extracted product images to products in the DB.

    Uses text-proximity matching:
    1. Extract text positions from the PDF (model numbers with their Y coords)
    2. Filter to real product images on each page
    3. For each product, find its model number in the PDF text
    4. Assign the nearest product image on that page

    Returns number of products updated.
    """
    conn = _get_db()
    updated = 0

    # Build text position index from the PDF
    text_positions = {}
    if pdf_path and os.path.exists(pdf_path):
        text_positions = _build_text_positions(pdf_path)

    try:
        with conn.cursor() as cur:
            # Find products from this source file ONLY
            # Try by Source Document spec first
            cur.execute("""
                SELECT p.id, p.product_name, p.product_model, p.category, p.brand
                FROM products p
                JOIN product_specs s ON s.product_id = p.id
                WHERE s.spec_key = 'Source Document' AND s.spec_value = %s
                AND p.image_url IS NULL
                ORDER BY p.id
            """, (filename,))
            products = cur.fetchall()

            # Fallback: match by catalogue_name
            if not products:
                cur.execute("""
                    SELECT p.id, p.product_name, p.product_model, p.category, p.brand
                    FROM products p
                    WHERE p.catalogue_name = %s AND p.image_url IS NULL
                    ORDER BY p.id
                """, (filename,))
                products = cur.fetchall()

            # Fallback: match by brand inferred from filename
            if not products:
                from .catalog_extractor import _auto_brand
                brand = _auto_brand(filename)
                if brand:
                    cur.execute("""
                        SELECT p.id, p.product_name, p.product_model, p.category, p.brand
                        FROM products p
                        WHERE p.brand = %s AND p.image_url IS NULL
                        ORDER BY p.id
                    """, (brand,))
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
            # Images appearing on 3+ pages are likely logos/watermarks — exclude them
            logo_paths = set()
            for h, paths in image_hashes.items():
                if len(paths) >= 3:
                    logo_paths.update(paths)
                    print(f"  [Image] Excluded repeated image (logo/watermark): {os.path.basename(paths[0])} ({len(paths)} copies)")
            if logo_paths:
                for page_num in images_by_page:
                    images_by_page[page_num] = [img for img in images_by_page[page_num] if img["path"] not in logo_paths]

            # ── Strategy: Image-Label-to-Product matching ──
            #
            # Catalog pages typically have this layout:
            #   "Type MOG-S1 (Rocker Type)"   ← label text
            #   [PRODUCT IMAGE]               ← image
            #   [TABLE: Cat.No, Rating, MRP]  ← products for this image
            #
            # Strategy:
            # 1. For each image on a page, find nearby text (caption/label)
            # 2. Match that label text to product names/models/specs
            # 3. All products matching the label get that image
            # 4. Fallback: nearest image above the product text

            # Step 1: Build image-label associations
            # For each image, find text within 80px vertically (above or below)
            image_labels = {}  # image_path -> [nearby text strings]
            if text_positions:
                for page_num, page_imgs in images_by_page.items():
                    page_texts = text_positions.get(page_num, [])
                    for img in page_imgs:
                        img_cx = img["x"] + img["width"] / 2
                        img_cy = img["y"] + img["height"] / 2
                        img_top = img["y"]
                        img_bot = img["y"] + img["height"]

                        nearby = []
                        for txt, tx, ty in page_texts:
                            # Text within 80px above or below the image
                            if abs(ty - img_top) < 80 or abs(ty - img_bot) < 80:
                                # And horizontally overlapping (within image width range)
                                if abs(tx - img_cx) < img["width"]:
                                    nearby.append(txt)
                        if nearby:
                            image_labels[img["path"]] = nearby

            # Step 2: Find which page each product appears on
            product_pages = {}  # pid -> (page, x, y)
            if text_positions:
                for pid, pname, pmodel, category, brand in products:
                    for pg, entries in text_positions.items():
                        for txt, x, y in entries:
                            if pmodel in txt:
                                product_pages[pid] = (pg, x, y)
                                break
                        if pid in product_pages:
                            break

            # Fallback for scanned PDFs where text extraction returns nothing:
            # Distribute unmatched products across pages that have images.
            # This handles L&T and other CID-encoded PDFs where get_text()
            # returns empty strings but OCR/LLM extracted the products fine.
            if len(product_pages) < len(products) * 0.1 and images_by_page:
                unmatched = [p for p in products if p[0] not in product_pages]
                img_pages = sorted(images_by_page.keys())
                if unmatched and img_pages:
                    # Try fuzzy matching against any text we do have
                    for pid, pname, pmodel, category, brand in unmatched:
                        for pg, entries in text_positions.items():
                            for txt, x, y in entries:
                                if fuzz.partial_ratio(pmodel, txt) > 85:
                                    product_pages[pid] = (pg, x, y)
                                    break
                            if pid in product_pages:
                                break

                    # Still unmatched? Distribute evenly across image pages
                    still_unmatched = [p for p in unmatched if p[0] not in product_pages]
                    if still_unmatched:
                        per_page = max(1, len(still_unmatched) // len(img_pages))
                        for idx, (pid, pname, pmodel, category, brand) in enumerate(still_unmatched):
                            page_idx = min(idx // per_page, len(img_pages) - 1)
                            page_num = img_pages[page_idx]
                            # Place at center of page, spaced vertically
                            y_pos = 100 + (idx % per_page) * 50
                            product_pages[pid] = (page_num, 300, y_pos)

                    print(f"  [Image] Scanned PDF fallback: mapped {len(product_pages)}/{len(products)} products to image pages")

            # Step 3: Match products to images
            for pid, pname, pmodel, category, brand in products:
                best_img = None

                if pid not in product_pages:
                    continue

                page_num, px, py = product_pages[pid]
                page_imgs = images_by_page.get(page_num, [])

                if not page_imgs:
                    # Check adjacent page
                    for offset in [-1, 1]:
                        if (page_num + offset) in images_by_page:
                            page_imgs = images_by_page[page_num + offset]
                            break

                if not page_imgs:
                    continue

                # Strategy A: Find image whose label text matches this product
                for img in page_imgs:
                    labels = image_labels.get(img["path"], [])
                    for label in labels:
                        # Check if product name/model appears in the label
                        if (pmodel in label or
                            fuzz.partial_ratio(pmodel, label) > 85 or
                            any(word in label for word in pname.split()
                                if len(word) > 3 and word.lower() not in
                                {"with", "type", "pole", "phase", "rated"})):
                            best_img = img["path"]
                            break
                    if best_img:
                        break

                # Strategy B: Nearest image ABOVE the product text on same page
                if not best_img:
                    best_dist = float('inf')
                    for img in page_imgs:
                        iy = img["y"] + img["height"] / 2
                        ix = img["x"] + img["width"] / 2
                        dist = sqrt((ix - px) ** 2 + (iy - py) ** 2)

                        # Prefer images above the product
                        if iy < py and dist < best_dist:
                            best_dist = dist
                            best_img = img["path"]

                    # If nothing above, nearest overall on same page
                    if not best_img:
                        for img in page_imgs:
                            iy = img["y"] + img["height"] / 2
                            ix = img["x"] + img["width"] / 2
                            dist = sqrt((ix - px) ** 2 + (iy - py) ** 2)
                            if dist < best_dist:
                                best_dist = dist
                                best_img = img["path"]

                if best_img:
                    cur.execute("UPDATE products SET image_url = %s WHERE id = %s AND image_url IS NULL",
                                (best_img, pid))
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
    """Full pipeline: extract images from PDF → link to products in DB.

    Returns dict with stats.
    """
    filename = os.path.basename(pdf_path)
    print(f"\n[Image Extraction] {filename}")

    # Check if this file was already fully processed (including images)
    already_done, prev_record = is_file_processed(pdf_path)
    if already_done and prev_record.get("images_linked", 0) > 0:
        print(f"  [Image] Already processed on {prev_record['processed_at']} — "
              f"{prev_record['images_linked']} images linked previously")
        return {
            "filename": filename,
            "already_processed": True,
            "previous": prev_record,
        }

    # Extract embedded product images
    images = extract_images_from_pdf(pdf_path)

    # Render full pages
    pages = render_pages(pdf_path)

    # Link to products using text-proximity matching
    linked = link_images_to_products(images, filename, pdf_path=pdf_path)

    # Update processed_files record with image linking count
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
    """Fetch a product with all its images and specs.

    Returns dict or None. Includes 'images' list (up to 3) and legacy 'image_url'.
    """
    conn = _get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, product_name, product_model, description, image_url,
                       category, subcategory, brand
                FROM products WHERE product_model = %s
            """, (product_model,))
            row = cur.fetchone()
            if not row:
                return None

            pid, name, model, desc, image_url, category, subcategory, brand = row

            cur.execute("""
                SELECT spec_key, spec_value FROM product_specs
                WHERE product_id = %s ORDER BY spec_key
            """, (pid,))
            specs = {k: v for k, v in cur.fetchall()}

            return {
                "id": pid,
                "product_name": name,
                "product_model": model,
                "description": desc,
                "image_url": image_url,
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
        # Process the LK MCB PDF
        pdf = os.path.expanduser("~/mcb_test/LK - MCB.pdf")
        if os.path.exists(pdf):
            process_pdf_images(pdf)
        else:
            print("Usage: python image_extractor.py <pdf_path>")
