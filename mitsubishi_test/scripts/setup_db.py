"""Create and populate the mcb_test database from LK-MCB.pdf OCR data."""

import json
import re
import psycopg2

SRC_DB = dict(dbname="lk_catalog_meta", user="postgres", host="localhost", port=5432)
DST_DB = dict(dbname="mcb_test", user="postgres", host="localhost", port=5432)
RUN_ID = 51  # LK - MCB.pdf OCR run in lk_catalog_meta


def create_schema(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS mcb_products (
                id SERIAL PRIMARY KEY,
                catalog_no TEXT NOT NULL UNIQUE,
                brand TEXT DEFAULT 'L&K',
                product_type TEXT DEFAULT 'MCB',
                poles TEXT,
                rating TEXT,
                curve_type TEXT,
                modules INTEGER,
                voltage TEXT,
                breaking_capacity TEXT,
                description TEXT,
                product_image TEXT,
                source_page INTEGER,
                created_at TIMESTAMPTZ DEFAULT now()
            );

            CREATE TABLE IF NOT EXISTS mcb_specs (
                id SERIAL PRIMARY KEY,
                product_id INTEGER NOT NULL REFERENCES mcb_products(id) ON DELETE CASCADE,
                spec_key TEXT NOT NULL,
                spec_value TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_mcb_products_catalog ON mcb_products(catalog_no);
            CREATE INDEX IF NOT EXISTS idx_mcb_specs_product ON mcb_specs(product_id);
            CREATE INDEX IF NOT EXISTS idx_mcb_products_rating ON mcb_products(rating);
            CREATE INDEX IF NOT EXISTS idx_mcb_products_curve ON mcb_products(curve_type);
        """)
    conn.commit()
    print("[DB] Schema ready")


def parse_and_insert(src_conn, dst_conn):
    """Parse OCR tables from lk_catalog_meta and insert into mcb_test."""
    with src_conn.cursor() as cur:
        cur.execute("SELECT table_index, headers, rows FROM ocr_tables WHERE run_id = %s ORDER BY table_index", (RUN_ID,))
        tables = cur.fetchall()

    products = {}

    # Image mapping: poles+voltage → product image
    IMAGE_MAP = {
        ("Single Pole (SP)", "240/415V AC"): "images/products/page1_img1.png",
        ("Double Pole (DP)", "240/415V AC"): "images/products/page1_img2.png",
        ("Three Pole (TP)", "240/415V AC"): "images/products/page1_img0.png",
        ("Four Pole (FP)", "240/415V AC"): "images/products/page2_img1.png",
        ("Double Pole (DP)", "500V DC"): "images/products/page4_img0.png",
        ("Double Pole (DP)", "1000V DC"): "images/products/page4_img0.png",
    }

    PAGE_MAP = {
        "240/415V AC": {0: 1, 1: 2},
        "500V DC": 4,
        "1000V DC": 4,
    }

    for tidx, headers, rows in tables:
        if tidx in (0, 1):
            current_poles = None
            for row in rows:
                if row[0] and 'Pole' in row[0]:
                    current_poles = row[0].strip()
                    continue
                rating = row[0].strip() if row[0] else ''
                if not rating or not any(c.isdigit() for c in rating):
                    continue
                modules = row[1].strip() if len(row) > 1 and row[1] else ''
                voltage = '240/415V AC'
                bc = '10kA'
                page = 1 if tidx == 0 else 2

                for curve_idx, curve in [(2, 'B'), (4, 'C'), (6, 'D')]:
                    if len(row) > curve_idx and row[curve_idx] and row[curve_idx].strip() != '-':
                        cat = row[curve_idx].strip()
                        mrp = row[curve_idx + 1].strip() if len(row) > curve_idx + 1 else ''
                        img = IMAGE_MAP.get((current_poles, voltage), '')
                        products[cat] = {
                            'catalog_no': cat, 'poles': current_poles, 'rating': rating,
                            'curve_type': curve, 'modules': modules, 'mrp': mrp,
                            'voltage': voltage, 'breaking_capacity': bc,
                            'description': f'{current_poles or ""} {rating} {curve}-Curve MCB',
                            'image': img, 'page': page,
                        }

        elif tidx == 2:
            current_poles = None
            for row in rows:
                if row[0] and 'Pole' in row[0]:
                    current_poles = row[0].strip()
                    continue
                rating = row[1].strip() if len(row) > 1 and row[1] else ''
                if not rating or not any(c.isdigit() for c in rating):
                    continue
                modules = row[2].strip() if len(row) > 2 else ''
                for curve_idx, curve in [(3, 'C'), (5, 'D')]:
                    if len(row) > curve_idx and row[curve_idx] and row[curve_idx].strip() != '-':
                        cat = row[curve_idx].strip()
                        mrp = row[curve_idx + 1].strip() if len(row) > curve_idx + 1 else ''
                        img = IMAGE_MAP.get((current_poles, '240/415V AC'), '')
                        products[cat] = {
                            'catalog_no': cat, 'poles': current_poles, 'rating': rating,
                            'curve_type': curve, 'modules': modules, 'mrp': mrp,
                            'voltage': '240/415V AC', 'breaking_capacity': '15kA',
                            'description': f'{current_poles or ""} {rating} {curve}-Curve 15kA MCB',
                            'image': img, 'page': 2,
                        }

        elif tidx in (6, 7):
            curve = 'B' if tidx == 6 else 'C'
            for row in rows:
                rating = row[0].strip() if row[0] else ''
                if not rating or not any(c.isdigit() for c in rating):
                    continue
                modules = row[1].strip() if len(row) > 1 else ''
                cat = row[2].strip() if len(row) > 2 else ''
                mrp = row[3].strip() if len(row) > 3 else ''
                if cat:
                    products[cat] = {
                        'catalog_no': cat, 'poles': 'Double Pole (DP)', 'rating': rating,
                        'curve_type': curve, 'modules': modules, 'mrp': mrp,
                        'voltage': '500V DC', 'breaking_capacity': '6kA',
                        'description': f'DP {rating} 500V DC {curve}-Curve MCB',
                        'image': IMAGE_MAP[('Double Pole (DP)', '500V DC')], 'page': 4,
                    }

        elif tidx == 8:
            for row in rows:
                desc = row[0].strip() if row[0] else ''
                if not desc or 'MCB' not in desc:
                    continue
                modules = row[1].strip() if len(row) > 1 else ''
                cat = row[2].strip() if len(row) > 2 else ''
                mrp = row[3].strip() if len(row) > 3 else ''
                m = re.search(r'(\d+A)', desc)
                rating = m.group(1) if m else ''
                if cat:
                    products[cat] = {
                        'catalog_no': cat, 'poles': 'Double Pole (DP)', 'rating': rating,
                        'curve_type': 'C', 'modules': modules, 'mrp': mrp,
                        'voltage': '1000V DC', 'breaking_capacity': '6kA',
                        'description': desc,
                        'image': IMAGE_MAP[('Double Pole (DP)', '1000V DC')], 'page': 4,
                    }

    # Insert
    with dst_conn.cursor() as cur:
        for cat, p in products.items():
            mod = int(p['modules']) if p['modules'].isdigit() else None
            cur.execute("""INSERT INTO mcb_products
                (catalog_no, poles, rating, curve_type, modules, voltage, breaking_capacity, description, product_image, source_page)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (catalog_no) DO NOTHING RETURNING id""",
                (p['catalog_no'], p['poles'], p['rating'], p['curve_type'], mod,
                 p['voltage'], p['breaking_capacity'], p['description'], p['image'], p['page']))
            row = cur.fetchone()
            if not row:
                continue
            pid = row[0]

            specs = [
                ('catalog_number', p['catalog_no']),
                ('rating', p['rating']),
                ('curve_type', p['curve_type']),
                ('poles', p['poles'] or ''),
                ('modules', p['modules']),
                ('voltage', p['voltage']),
                ('breaking_capacity', p['breaking_capacity']),
                ('mrp', p['mrp']),
                ('brand', 'L&K'),
                ('standard', 'IS/IEC 60898 / IS/IEC 60947-2'),
            ]
            for key, val in specs:
                if val and val.strip() and val.strip() != '-':
                    cur.execute("INSERT INTO mcb_specs (product_id, spec_key, spec_value) VALUES (%s,%s,%s)",
                                (pid, key, val.strip()))

    dst_conn.commit()
    print(f"[DB] Inserted {len(products)} products")


if __name__ == "__main__":
    src = psycopg2.connect(**SRC_DB)
    dst = psycopg2.connect(**DST_DB)
    create_schema(dst)
    parse_and_insert(src, dst)
    src.close()
    dst.close()
