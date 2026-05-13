"""Database schema and helpers for the Accessories Extract pipeline.

Databases:
- accessories_extract        — Accessories catalog + specs + product mappings
- accessories_extract_meta   — Processing metadata (file dedup)
- accessories_user_data      — User edits with change tracking

Also connects to mitsubishi_test (read-only) for product mapping lookups.
"""

from __future__ import annotations

import hashlib
import psycopg2
from psycopg2 import sql

BASE_DB_PARAMS = dict(user="postgres", host="localhost", port=5432)

MAIN_DB = "accessories_extract"
META_DB = "accessories_extract_meta"
USER_DB = "accessories_user_data"
PRODUCT_DB = "mitsubishi_test"  # Read-only lookups for product mapping

MAIN_DB_PARAMS = dict(dbname=MAIN_DB, **BASE_DB_PARAMS)
META_DB_PARAMS = dict(dbname=META_DB, **BASE_DB_PARAMS)
USER_DB_PARAMS = dict(dbname=USER_DB, **BASE_DB_PARAMS)
PRODUCT_DB_PARAMS = dict(dbname=PRODUCT_DB, **BASE_DB_PARAMS)


def get_db():
    return psycopg2.connect(**MAIN_DB_PARAMS)


def get_meta_db():
    return psycopg2.connect(**META_DB_PARAMS)


def get_user_db():
    return psycopg2.connect(**USER_DB_PARAMS)


def get_product_db():
    """Read-only connection to mitsubishi_test for product lookups."""
    return psycopg2.connect(**PRODUCT_DB_PARAMS)


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
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
    finally:
        conn.close()


def _init_main_schema():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accessories (
                    id SERIAL PRIMARY KEY,
                    accessory_name TEXT NOT NULL,
                    accessory_model TEXT NOT NULL UNIQUE,
                    category TEXT,
                    sub_category TEXT,
                    brand TEXT DEFAULT 'Mitsubishi Electric',
                    mrp TEXT,
                    description TEXT,
                    image_url TEXT,
                    catalogue_name TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_acc_name ON accessories(accessory_name);
                CREATE INDEX IF NOT EXISTS idx_acc_category ON accessories(category);
                CREATE INDEX IF NOT EXISTS idx_acc_sub_category ON accessories(sub_category);
                CREATE INDEX IF NOT EXISTS idx_acc_brand ON accessories(brand);

                CREATE TABLE IF NOT EXISTS accessory_specs (
                    id SERIAL PRIMARY KEY,
                    accessory_id INTEGER REFERENCES accessories(id) ON DELETE CASCADE,
                    spec_key TEXT NOT NULL,
                    spec_value TEXT NOT NULL,
                    spec_unit TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(accessory_id, spec_key)
                );
                CREATE INDEX IF NOT EXISTS idx_aspec_acc ON accessory_specs(accessory_id);
                CREATE INDEX IF NOT EXISTS idx_aspec_key ON accessory_specs(spec_key);

                CREATE TABLE IF NOT EXISTS accessory_product_map (
                    id SERIAL PRIMARY KEY,
                    accessory_id INTEGER REFERENCES accessories(id) ON DELETE CASCADE,
                    product_model TEXT NOT NULL,
                    applies_to_frame_size TEXT,
                    compatibility_notes TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(accessory_id, product_model)
                );
                CREATE INDEX IF NOT EXISTS idx_apm_acc ON accessory_product_map(accessory_id);
                CREATE INDEX IF NOT EXISTS idx_apm_product ON accessory_product_map(product_model);
                CREATE INDEX IF NOT EXISTS idx_apm_frame ON accessory_product_map(applies_to_frame_size);
            """)
        conn.commit()
    finally:
        conn.close()


def _init_meta_schema():
    conn = get_meta_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS processed_files (
                    id SERIAL PRIMARY KEY,
                    filename TEXT NOT NULL,
                    file_hash TEXT NOT NULL UNIQUE,
                    accessories_inserted INTEGER DEFAULT 0,
                    accessories_skipped INTEGER DEFAULT 0,
                    mappings_created INTEGER DEFAULT 0,
                    processed_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_pf_hash ON processed_files(file_hash);
            """)
        conn.commit()
    finally:
        conn.close()


def _init_user_schema():
    conn = get_user_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS accessories (
                    id SERIAL PRIMARY KEY,
                    original_accessory_id INTEGER,
                    accessory_name TEXT NOT NULL,
                    accessory_model TEXT NOT NULL,
                    category TEXT,
                    sub_category TEXT,
                    brand TEXT,
                    mrp TEXT,
                    description TEXT,
                    change_type TEXT DEFAULT 'user_edit',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(accessory_model)
                );

                CREATE TABLE IF NOT EXISTS accessory_specs (
                    id SERIAL PRIMARY KEY,
                    accessory_id INTEGER REFERENCES accessories(id) ON DELETE CASCADE,
                    original_accessory_id INTEGER,
                    spec_key TEXT NOT NULL,
                    spec_value TEXT NOT NULL,
                    change_type TEXT DEFAULT 'user_added',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(accessory_id, spec_key)
                );
            """)
        conn.commit()
    finally:
        conn.close()


def init_db():
    _ensure_database(MAIN_DB)
    _ensure_database(META_DB)
    _ensure_database(USER_DB)
    _init_main_schema()
    _init_meta_schema()
    _init_user_schema()
    print("[DB] Accessories schema ready")


# ── File processing tracking ──

def compute_file_hash(file_path: str) -> str:
    h = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def is_file_processed(file_path: str):
    file_hash = compute_file_hash(file_path)
    conn = get_meta_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT filename, accessories_inserted, accessories_skipped, mappings_created, processed_at "
                "FROM processed_files WHERE file_hash = %s",
                (file_hash,),
            )
            row = cur.fetchone()
            if row:
                return True, {
                    "filename": row[0], "accessories_inserted": row[1],
                    "accessories_skipped": row[2], "mappings_created": row[3],
                    "processed_at": str(row[4]),
                }
        return False, None
    finally:
        conn.close()


def mark_file_processed(file_path, filename, accessories_inserted=0, accessories_skipped=0, mappings_created=0):
    file_hash = compute_file_hash(file_path)
    conn = get_meta_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO processed_files (filename, file_hash, accessories_inserted, accessories_skipped, mappings_created)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (file_hash) DO UPDATE SET
                    accessories_inserted = EXCLUDED.accessories_inserted,
                    accessories_skipped = EXCLUDED.accessories_skipped,
                    mappings_created = EXCLUDED.mappings_created,
                    processed_at = now()
            """, (filename, file_hash, accessories_inserted, accessories_skipped, mappings_created))
        conn.commit()
    finally:
        conn.close()


# ── Save helpers ──

def save_accessory(accessory: dict) -> int | None:
    """Insert or update an accessory. Returns accessory id."""
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO accessories (accessory_name, accessory_model, category, sub_category, brand, mrp, description, catalogue_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (accessory_model) DO UPDATE SET
                    accessory_name = COALESCE(NULLIF(EXCLUDED.accessory_name, ''), accessories.accessory_name),
                    category = COALESCE(NULLIF(EXCLUDED.category, ''), accessories.category),
                    sub_category = COALESCE(NULLIF(EXCLUDED.sub_category, ''), accessories.sub_category),
                    mrp = COALESCE(NULLIF(EXCLUDED.mrp, ''), accessories.mrp)
                RETURNING id
            """, (
                accessory.get("accessory_name", ""),
                accessory.get("accessory_model", ""),
                accessory.get("category"),
                accessory.get("sub_category"),
                accessory.get("brand", "Mitsubishi Electric"),
                accessory.get("mrp"),
                accessory.get("description"),
                accessory.get("catalogue_name"),
            ))
            return cur.fetchone()[0]
    except Exception as e:
        conn.rollback()
        print(f"[DB] Save accessory failed: {e}")
        return None
    finally:
        conn.commit()
        conn.close()


def save_accessory_specs(accessory_id: int, specs: dict):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            for key, value in specs.items():
                if not key or value is None:
                    continue
                value = str(value).strip()
                if not value:
                    continue
                cur.execute("""
                    INSERT INTO accessory_specs (accessory_id, spec_key, spec_value)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (accessory_id, spec_key) DO NOTHING
                """, (accessory_id, key, value))
        conn.commit()
    finally:
        conn.close()


def save_product_mapping(accessory_id: int, product_model: str, frame_size: str = None, notes: str = None):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO accessory_product_map (accessory_id, product_model, applies_to_frame_size, compatibility_notes)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (accessory_id, product_model) DO NOTHING
            """, (accessory_id, product_model, frame_size, notes))
        conn.commit()
    finally:
        conn.close()


# ── Query helpers ──

def get_all_accessories():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.id, a.accessory_name, a.accessory_model, a.category, a.sub_category, a.brand, a.mrp,
                       (SELECT count(*) FROM accessory_specs s WHERE s.accessory_id = a.id) as spec_count,
                       (SELECT count(*) FROM accessory_product_map m WHERE m.accessory_id = a.id) as mapping_count
                FROM accessories a ORDER BY a.id
            """)
            return cur.fetchall()
    finally:
        conn.close()


def get_accessory_detail(accessory_id: int):
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM accessories WHERE id = %s", (accessory_id,))
            cols = [desc[0] for desc in cur.description]
            row = cur.fetchone()
            if not row:
                return None, [], []
            accessory = dict(zip(cols, row))

            cur.execute("SELECT spec_key, spec_value FROM accessory_specs WHERE accessory_id = %s ORDER BY spec_key", (accessory_id,))
            specs = cur.fetchall()

            cur.execute("""
                SELECT product_model, applies_to_frame_size, compatibility_notes
                FROM accessory_product_map WHERE accessory_id = %s ORDER BY product_model
            """, (accessory_id,))
            mappings = cur.fetchall()

            return accessory, specs, mappings
    finally:
        conn.close()


def get_mappings():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT a.accessory_name, a.accessory_model, a.sub_category,
                       m.product_model, m.applies_to_frame_size, a.mrp
                FROM accessory_product_map m
                JOIN accessories a ON a.id = m.accessory_id
                ORDER BY a.accessory_model, m.product_model
            """)
            return cur.fetchall()
    finally:
        conn.close()


def lookup_products_by_frame(frame_sizes: list[str]) -> list[str]:
    """Look up product models in mitsubishi_test that match given frame sizes."""
    conn = get_product_db()
    try:
        with conn.cursor() as cur:
            conditions = []
            params = []
            for fs in frame_sizes:
                conditions.append("p.product_model ILIKE %s")
                params.append(f"%{fs}%")
            if not conditions:
                return []
            where = " OR ".join(conditions)
            cur.execute(f"""
                SELECT DISTINCT p.product_model FROM products p
                WHERE ({where}) AND p.category = 'MCCB'
                ORDER BY p.product_model
            """, params)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()
