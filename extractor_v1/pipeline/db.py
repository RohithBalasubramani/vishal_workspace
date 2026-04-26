"""Database connection and schema management — mitsubishi_test format.

Single database with two tables:
  - products (id, product_name, product_model, description, image_url, category,
              subcategory, brand, hsn_code, mrp, alternate_image1, alternate_image2,
              created_at, catalogue_name)
  - product_specs (id, product_id, spec_key, spec_value, spec_unit, product_model,
                   category, spec_group)

Plus a processed_files table for file-level deduplication.
"""

from __future__ import annotations

import hashlib

import psycopg2
from psycopg2 import sql

BASE_DB_PARAMS = dict(user="postgres", host="localhost", port=5432)
DB_NAME = "extractor_v1"

DB_PARAMS = dict(dbname=DB_NAME, **BASE_DB_PARAMS)


def get_db():
    return psycopg2.connect(**DB_PARAMS)


def _get_admin_db():
    return psycopg2.connect(dbname="postgres", **BASE_DB_PARAMS)


def _ensure_database(dbname: str):
    conn = _get_admin_db()
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (dbname,))
            if cur.fetchone():
                return
            cur.execute(
                sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname))
            )
    finally:
        conn.close()


def _init_schema():
    """Create tables matching the mitsubishi_test format."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    product_name TEXT NOT NULL,
                    product_model TEXT NOT NULL UNIQUE,
                    description TEXT,
                    image_url TEXT,
                    category TEXT,
                    subcategory TEXT,
                    brand TEXT,
                    hsn_code TEXT,
                    mrp TEXT,
                    alternate_image1 TEXT,
                    alternate_image2 TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    catalogue_name TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_products_name ON products(product_name);
                CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
                CREATE INDEX IF NOT EXISTS idx_products_subcategory ON products(subcategory);
                CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
                CREATE INDEX IF NOT EXISTS idx_products_mrp ON products(mrp);

                CREATE TABLE IF NOT EXISTS product_specs (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
                    spec_key TEXT NOT NULL,
                    spec_value TEXT NOT NULL,
                    spec_unit TEXT,
                    product_model TEXT,
                    category TEXT,
                    spec_group TEXT,
                    CONSTRAINT product_specs_product_key_unique UNIQUE (product_id, spec_key)
                );
                CREATE INDEX IF NOT EXISTS idx_specs_product ON product_specs(product_id);
                CREATE INDEX IF NOT EXISTS idx_specs_key ON product_specs(spec_key);
                CREATE INDEX IF NOT EXISTS idx_specs_model ON product_specs(product_model);
                CREATE INDEX IF NOT EXISTS idx_specs_category ON product_specs(category);
                CREATE INDEX IF NOT EXISTS idx_specs_group ON product_specs(spec_group);
                CREATE INDEX IF NOT EXISTS idx_specs_key_value ON product_specs(spec_key, spec_value);

                CREATE TABLE IF NOT EXISTS processed_files (
                    id SERIAL PRIMARY KEY,
                    filename TEXT NOT NULL,
                    file_hash TEXT NOT NULL UNIQUE,
                    products_inserted INTEGER DEFAULT 0,
                    products_skipped INTEGER DEFAULT 0,
                    images_linked INTEGER DEFAULT 0,
                    processed_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_processed_files_hash ON processed_files(file_hash);
                CREATE INDEX IF NOT EXISTS idx_processed_files_name ON processed_files(filename);
                """
            )
        conn.commit()
        print("[DB] Schema ready")
    finally:
        conn.close()


def init_db():
    """Create database and schema."""
    _ensure_database(DB_NAME)
    _init_schema()


def compute_file_hash(file_path):
    """Compute SHA256 hash of a file."""
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def is_file_processed(file_path):
    """Check if a file has already been processed (by SHA256 hash)."""
    file_hash = compute_file_hash(file_path)
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT filename, products_inserted, products_skipped, images_linked, processed_at "
                "FROM processed_files WHERE file_hash = %s",
                (file_hash,),
            )
            row = cur.fetchone()
            if row:
                return True, {
                    "filename": row[0],
                    "products_inserted": row[1],
                    "products_skipped": row[2],
                    "images_linked": row[3],
                    "processed_at": str(row[4]),
                }
        return False, None
    finally:
        conn.close()


def mark_file_processed(file_path, filename, products_inserted=None, products_skipped=None, images_linked=None):
    """Record that a file has been successfully processed."""
    file_hash = compute_file_hash(file_path)
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT products_inserted, products_skipped, images_linked "
                "FROM processed_files WHERE file_hash = %s",
                (file_hash,),
            )
            existing = cur.fetchone()
            current_inserted = existing[0] if existing else 0
            current_skipped = existing[1] if existing else 0
            current_images = existing[2] if existing else 0

            products_inserted = current_inserted if products_inserted is None else products_inserted
            products_skipped = current_skipped if products_skipped is None else products_skipped
            images_linked = current_images if images_linked is None else images_linked

            cur.execute(
                """
                INSERT INTO processed_files (filename, file_hash, products_inserted, products_skipped, images_linked)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (file_hash) DO UPDATE SET
                    products_inserted = EXCLUDED.products_inserted,
                    products_skipped = EXCLUDED.products_skipped,
                    images_linked = EXCLUDED.images_linked,
                    processed_at = now()
                """,
                (filename, file_hash, products_inserted, products_skipped, images_linked),
            )
        conn.commit()
    finally:
        conn.close()


def upsert_product(product: dict, catalogue_name: str = None) -> tuple[int, bool]:
    """Insert or update a product in the mitsubishi_test format.

    Returns (product_id, is_new).
    """
    conn = get_db()
    try:
        with conn.cursor() as cur:
            model = product.get("product_model", "").strip()
            if not model:
                return None, False

            name = product.get("product_name", model)
            desc = product.get("description")
            category = product.get("category")
            subcategory = product.get("subcategory")
            brand = product.get("brand")
            hsn_code = product.get("hsn_code")
            mrp = product.get("mrp")
            image_url = product.get("image_url")
            cat_name = catalogue_name or product.get("catalogue_name")

            cur.execute(
                """
                INSERT INTO products (product_name, product_model, description, image_url,
                                      category, subcategory, brand, hsn_code, mrp, catalogue_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (product_model) DO UPDATE SET
                    product_name = COALESCE(NULLIF(EXCLUDED.product_name, ''), products.product_name),
                    description = COALESCE(EXCLUDED.description, products.description),
                    category = COALESCE(EXCLUDED.category, products.category),
                    subcategory = COALESCE(EXCLUDED.subcategory, products.subcategory),
                    brand = COALESCE(EXCLUDED.brand, products.brand),
                    hsn_code = COALESCE(EXCLUDED.hsn_code, products.hsn_code),
                    mrp = COALESCE(EXCLUDED.mrp, products.mrp),
                    catalogue_name = COALESCE(EXCLUDED.catalogue_name, products.catalogue_name)
                RETURNING id, (xmax = 0) AS is_new
                """,
                (name, model, desc, image_url, category, subcategory, brand, hsn_code, mrp, cat_name),
            )
            row = cur.fetchone()
            product_id, is_new = row[0], row[1]

            # Upsert specs
            specs = product.get("specs", {})
            spec_group = product.get("spec_group")
            for key, value in specs.items():
                if not key or not value:
                    continue
                unit = None
                if isinstance(value, dict):
                    unit = value.get("unit")
                    value = value.get("value", str(value))
                cur.execute(
                    """
                    INSERT INTO product_specs (product_id, spec_key, spec_value, spec_unit,
                                               product_model, category, spec_group)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (product_id, spec_key) DO UPDATE SET
                        spec_value = EXCLUDED.spec_value,
                        spec_unit = COALESCE(EXCLUDED.spec_unit, product_specs.spec_unit),
                        product_model = COALESCE(EXCLUDED.product_model, product_specs.product_model),
                        category = COALESCE(EXCLUDED.category, product_specs.category),
                        spec_group = COALESCE(EXCLUDED.spec_group, product_specs.spec_group)
                    """,
                    (product_id, str(key), str(value), unit, model, category, spec_group),
                )

        conn.commit()
        return product_id, is_new
    finally:
        conn.close()


def bulk_upsert_products(products: list[dict], catalogue_name: str = None) -> tuple[int, int]:
    """Upsert a list of products. Returns (inserted_count, skipped_count)."""
    inserted = 0
    skipped = 0
    for product in products:
        try:
            pid, is_new = upsert_product(product, catalogue_name)
            if pid is None:
                skipped += 1
            elif is_new:
                inserted += 1
        except Exception as e:
            print(f"  [DB] Error upserting {product.get('product_model', '?')}: {e}")
            skipped += 1
    return inserted, skipped
