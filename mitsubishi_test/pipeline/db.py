"""Database connection and schema management for the catalog pipeline."""

from __future__ import annotations

import hashlib
import io
import subprocess

import psycopg2
from psycopg2 import sql

BASE_DB_PARAMS = dict(user="postgres", host="localhost", port=5432)
PRODUCT_DB_NAME = "mitsubishi_test"
META_DB_NAME = "mitsubishi_test_meta"
USER_DB_NAME = "mitsubishi_user_data"

PRODUCT_DB_PARAMS = dict(dbname=PRODUCT_DB_NAME, **BASE_DB_PARAMS)
META_DB_PARAMS = dict(dbname=META_DB_NAME, **BASE_DB_PARAMS)
USER_DB_PARAMS = dict(dbname=USER_DB_NAME, **BASE_DB_PARAMS)

# Backward-compat import for modules that treat DB_PARAMS as the product DB.
DB_PARAMS = PRODUCT_DB_PARAMS

PRODUCT_TABLES = {"products", "product_specs"}
CORE_META_TABLES = {
    "processed_files",
    "ocr_runs",
    "ocr_tables",
    "ocr_search_log",
    "rows",
    "row_versions",
    "catalog_documents",
    "catalog_sources",
    "product_images",
    "product_attributes",
    "electrical_products",
    "electrical_product_images",
}
META_TABLE_COPY_ORDER = [
    "processed_files",
    "catalog_sources",
    "ocr_runs",
    "catalog_documents",
    "ocr_search_log",
    "ocr_tables",
    "rows",
    "row_versions",
    "product_images",
    "product_attributes",
    "electrical_products",
    "electrical_product_images",
]
META_TABLE_DROP_ORDER = [
    "electrical_product_images",
    "electrical_products",
    "product_attributes",
    "product_images",
    "row_versions",
    "rows",
    "ocr_tables",
    "ocr_search_log",
    "catalog_documents",
    "ocr_runs",
    "catalog_sources",
    "processed_files",
]


def get_db():
    return psycopg2.connect(**PRODUCT_DB_PARAMS)


def get_meta_db():
    return psycopg2.connect(**META_DB_PARAMS)


def get_user_db():
    return psycopg2.connect(**USER_DB_PARAMS)


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


def _table_exists(conn, table_name: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_name = %s
            """,
            (table_name,),
        )
        return cur.fetchone() is not None


def _list_public_tables(conn) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = 'public'
            ORDER BY tablename
            """
        )
        return [row[0] for row in cur.fetchall()]


def _table_columns(conn, table_name: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = %s
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def _serial_columns(conn, table_name: str) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
              AND column_default LIKE 'nextval(%%'
            ORDER BY ordinal_position
            """,
            (table_name,),
        )
        return [row[0] for row in cur.fetchall()]


def _quoted_columns(columns: list[str]) -> str:
    return ", ".join(f'"{col}"' for col in columns)


def _clone_table_schema_to_meta(table_name: str):
    dump = subprocess.run(
        [
            "pg_dump",
            "-U",
            BASE_DB_PARAMS["user"],
            "-h",
            BASE_DB_PARAMS["host"],
            "-p",
            str(BASE_DB_PARAMS["port"]),
            "-d",
            PRODUCT_DB_NAME,
            "--schema-only",
            "--no-owner",
            "--no-privileges",
            "--table",
            f"public.{table_name}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(
        [
            "psql",
            "-U",
            BASE_DB_PARAMS["user"],
            "-h",
            BASE_DB_PARAMS["host"],
            "-p",
            str(BASE_DB_PARAMS["port"]),
            "-d",
            META_DB_NAME,
            "-v",
            "ON_ERROR_STOP=1",
        ],
        input=dump.stdout,
        capture_output=True,
        text=True,
        check=True,
    )


def _copy_table_data(source_conn, target_conn, table_name: str):
    columns = _table_columns(source_conn, table_name)
    if not columns:
        return

    column_sql = _quoted_columns(columns)
    buffer = io.StringIO()
    with source_conn.cursor() as src_cur:
        src_cur.copy_expert(
            f'COPY public."{table_name}" ({column_sql}) TO STDOUT WITH CSV',
            buffer,
        )
    buffer.seek(0)

    with target_conn.cursor() as tgt_cur:
        tgt_cur.execute(
            sql.SQL("TRUNCATE TABLE {} CASCADE").format(sql.Identifier(table_name))
        )
        tgt_cur.copy_expert(
            f'COPY public."{table_name}" ({column_sql}) FROM STDIN WITH CSV',
            buffer,
        )

        for serial_column in _serial_columns(target_conn, table_name):
            tgt_cur.execute(
                sql.SQL(
                    """
                    SELECT setval(
                        pg_get_serial_sequence(%s, %s),
                        COALESCE((SELECT MAX({col}) FROM {table}), 1),
                        EXISTS(SELECT 1 FROM {table})
                    )
                    """
                ).format(
                    col=sql.Identifier(serial_column),
                    table=sql.Identifier(table_name),
                ),
                (f"public.{table_name}", serial_column),
            )


def _table_count(conn, table_name: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("SELECT COUNT(*) FROM {}").format(sql.Identifier(table_name))
        )
        return cur.fetchone()[0]


def _drop_tables(conn, table_names: list[str]):
    existing = set(_list_public_tables(conn))
    with conn.cursor() as cur:
        for table_name in table_names:
            if table_name in existing:
                cur.execute(
                    sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(
                        sql.Identifier(table_name)
                    )
                )


def _init_product_schema():
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
                    catalogue_name TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_products_name ON products(product_name);
                CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
                CREATE INDEX IF NOT EXISTS idx_products_subcategory ON products(subcategory);
                CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
                CREATE INDEX IF NOT EXISTS idx_products_mrp ON products(mrp);

                CREATE TABLE IF NOT EXISTS product_specs (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
                    spec_group TEXT,
                    spec_key TEXT NOT NULL,
                    spec_value TEXT NOT NULL,
                    spec_unit TEXT,
                    product_model TEXT,
                    category TEXT,
                    CONSTRAINT product_specs_product_key_unique UNIQUE (product_id, spec_key)
                );
                CREATE INDEX IF NOT EXISTS idx_specs_product ON product_specs(product_id);
                CREATE INDEX IF NOT EXISTS idx_specs_key ON product_specs(spec_key);
                CREATE INDEX IF NOT EXISTS idx_specs_group ON product_specs(spec_group);
                CREATE INDEX IF NOT EXISTS idx_specs_model ON product_specs(product_model);
                CREATE INDEX IF NOT EXISTS idx_specs_category ON product_specs(category);
                CREATE INDEX IF NOT EXISTS idx_specs_key_value ON product_specs(spec_key, spec_value);
                """
            )
        conn.commit()
    finally:
        conn.close()


def _init_meta_schema():
    conn = get_meta_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
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

                CREATE TABLE IF NOT EXISTS ocr_runs (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    image_path TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    processing_time_s REAL,
                    image_width INTEGER,
                    image_height INTEGER,
                    raw_text TEXT,
                    filename TEXT,
                    num_pages INTEGER DEFAULT 1,
                    file_type TEXT DEFAULT 'image',
                    method TEXT DEFAULT 'deepseek-ocr',
                    text_search tsvector
                );
                CREATE INDEX IF NOT EXISTS idx_ocr_runs_created ON ocr_runs(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_ocr_runs_filename ON ocr_runs(filename);
                CREATE INDEX IF NOT EXISTS idx_ocr_runs_fts ON ocr_runs USING gin(text_search);

                CREATE TABLE IF NOT EXISTS ocr_tables (
                    id SERIAL PRIMARY KEY,
                    run_id INTEGER REFERENCES ocr_runs(id) ON DELETE CASCADE,
                    table_index INTEGER NOT NULL,
                    headers JSONB NOT NULL,
                    rows JSONB NOT NULL,
                    header_sig TEXT DEFAULT '',
                    content_search tsvector
                );
                CREATE INDEX IF NOT EXISTS idx_ocr_tables_run ON ocr_tables(run_id);
                CREATE INDEX IF NOT EXISTS idx_ocr_tables_header_sig ON ocr_tables(header_sig);
                CREATE INDEX IF NOT EXISTS idx_ocr_tables_fts ON ocr_tables USING gin(content_search);

                CREATE TABLE IF NOT EXISTS ocr_search_log (
                    id SERIAL PRIMARY KEY,
                    run_id INTEGER REFERENCES ocr_runs(id) ON DELETE CASCADE,
                    queried_at TIMESTAMPTZ DEFAULT now(),
                    query TEXT NOT NULL,
                    results JSONB,
                    search_time_ms REAL
                );
                CREATE INDEX IF NOT EXISTS idx_ocr_search_run ON ocr_search_log(run_id);

                CREATE TABLE IF NOT EXISTS rows (
                    id SERIAL PRIMARY KEY,
                    doc_id INTEGER NOT NULL REFERENCES ocr_runs(id) ON DELETE CASCADE,
                    page INTEGER,
                    table_index INTEGER,
                    row_index INTEGER,
                    raw_text TEXT,
                    table_html TEXT,
                    raw_cells JSONB,
                    headers JSONB,
                    canonical JSONB DEFAULT '{}',
                    attributes JSONB DEFAULT '{}',
                    confidence_score REAL DEFAULT 0.0,
                    source_method TEXT NOT NULL DEFAULT 'table_parser',
                    extraction_version INTEGER DEFAULT 1,
                    text_search tsvector,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_rows_doc_page ON rows(doc_id, page, table_index);
                CREATE INDEX IF NOT EXISTS idx_rows_canonical_gin ON rows USING gin(canonical jsonb_path_ops);
                CREATE INDEX IF NOT EXISTS idx_rows_attributes_gin ON rows USING gin(attributes jsonb_path_ops);
                CREATE INDEX IF NOT EXISTS idx_rows_fts ON rows USING gin(text_search);
                CREATE INDEX IF NOT EXISTS idx_rows_canonical_brand_model
                    ON rows ((lower(canonical->>'brand')), (lower(canonical->>'model')))
                    WHERE canonical->>'brand' IS NOT NULL AND canonical->>'model' IS NOT NULL;

                CREATE TABLE IF NOT EXISTS row_versions (
                    id SERIAL PRIMARY KEY,
                    row_id INTEGER NOT NULL REFERENCES rows(id) ON DELETE CASCADE,
                    version INTEGER NOT NULL,
                    canonical JSONB,
                    attributes JSONB,
                    confidence_score REAL,
                    source_method TEXT,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_row_versions_row ON row_versions(row_id, version DESC);

                CREATE TABLE IF NOT EXISTS catalog_documents (
                    doc_id SERIAL PRIMARY KEY,
                    ocr_run_id INTEGER REFERENCES ocr_runs(id),
                    brand TEXT,
                    source_url TEXT,
                    file_path TEXT,
                    file_name TEXT,
                    year INTEGER,
                    doc_type TEXT,
                    is_active BOOLEAN DEFAULT TRUE,
                    page_count INTEGER,
                    sha256 TEXT UNIQUE,
                    source_score REAL DEFAULT 0.0,
                    ingested_at TIMESTAMPTZ DEFAULT now(),
                    ocr_status TEXT DEFAULT 'pending',
                    extraction_status TEXT DEFAULT 'pending'
                );
                CREATE INDEX IF NOT EXISTS idx_catdoc_brand ON catalog_documents(brand);
                CREATE INDEX IF NOT EXISTS idx_catdoc_status ON catalog_documents(ocr_status);

                CREATE TABLE IF NOT EXISTS catalog_sources (
                    id SERIAL PRIMARY KEY,
                    brand TEXT NOT NULL,
                    source_url TEXT,
                    file_path TEXT,
                    file_name TEXT,
                    file_type TEXT DEFAULT 'pdf',
                    page_count INTEGER,
                    scrape_status TEXT DEFAULT 'pending',
                    ocr_status TEXT DEFAULT 'pending',
                    extraction_status TEXT DEFAULT 'pending',
                    products_extracted INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_catalog_brand ON catalog_sources(brand);
                CREATE INDEX IF NOT EXISTS idx_catalog_status ON catalog_sources(scrape_status);

                CREATE TABLE IF NOT EXISTS product_images (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER NOT NULL,
                    image_path TEXT NOT NULL,
                    image_type TEXT DEFAULT 'product',
                    page_number INTEGER,
                    width INTEGER,
                    height INTEGER,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_images_product ON product_images(product_id);

                CREATE TABLE IF NOT EXISTS product_attributes (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER NOT NULL,
                    attr_key TEXT NOT NULL,
                    attr_value TEXT NOT NULL,
                    normalized_value TEXT,
                    unit TEXT,
                    confidence REAL DEFAULT 1.0,
                    source TEXT,
                    UNIQUE(product_id, attr_key)
                );
                CREATE INDEX IF NOT EXISTS idx_attrs_product ON product_attributes(product_id);
                CREATE INDEX IF NOT EXISTS idx_attrs_key ON product_attributes(attr_key);
                CREATE INDEX IF NOT EXISTS idx_attrs_key_value ON product_attributes(attr_key, normalized_value);

                CREATE TABLE IF NOT EXISTS electrical_products (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER UNIQUE,
                    brand TEXT,
                    series TEXT,
                    model TEXT,
                    category TEXT,
                    category_confidence REAL,
                    category_alternatives JSONB DEFAULT '[]',
                    subcategory TEXT,
                    current_rating_a REAL,
                    breaking_capacity_ka REAL,
                    poles INTEGER,
                    curve_type TEXT,
                    voltage_v REAL,
                    frequency_hz REAL,
                    trip_type TEXT,
                    mounting_type TEXT,
                    compliance TEXT,
                    source_doc TEXT,
                    source_page INTEGER,
                    confidence_score REAL,
                    is_complete BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_ep_brand ON electrical_products(brand);
                CREATE INDEX IF NOT EXISTS idx_ep_category ON electrical_products(category);
                CREATE INDEX IF NOT EXISTS idx_ep_current ON electrical_products(current_rating_a);
                CREATE INDEX IF NOT EXISTS idx_ep_poles ON electrical_products(poles);
                CREATE INDEX IF NOT EXISTS idx_ep_curve ON electrical_products(curve_type);
                CREATE INDEX IF NOT EXISTS idx_ep_breaking ON electrical_products(breaking_capacity_ka);

                CREATE TABLE IF NOT EXISTS electrical_product_images (
                    id SERIAL PRIMARY KEY,
                    electrical_product_id INTEGER NOT NULL
                        REFERENCES electrical_products(id) ON DELETE CASCADE,
                    image_path TEXT NOT NULL,
                    image_type TEXT DEFAULT 'product',
                    page_number INTEGER,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_epi_product
                    ON electrical_product_images(electrical_product_id);
                """
            )

            cur.execute(
                """
                CREATE OR REPLACE FUNCTION rows_text_search_trigger() RETURNS trigger AS $$
                BEGIN
                    NEW.text_search := to_tsvector('english', COALESCE(NEW.raw_text, ''));
                    RETURN NEW;
                END; $$ LANGUAGE plpgsql;
                """
            )
            cur.execute(
                """
                DO $$ BEGIN
                    CREATE TRIGGER rows_fts_update BEFORE INSERT OR UPDATE OF raw_text
                        ON rows FOR EACH ROW EXECUTE FUNCTION rows_text_search_trigger();
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$;
                """
            )
        conn.commit()
    finally:
        conn.close()


def _init_user_schema():
    """Create the user-data DB with the same products + product_specs schema."""
    conn = get_user_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS products (
                    id SERIAL PRIMARY KEY,
                    original_product_id INTEGER,
                    product_name TEXT NOT NULL,
                    product_model TEXT NOT NULL,
                    description TEXT,
                    image_url TEXT,
                    category TEXT,
                    subcategory TEXT,
                    brand TEXT,
                    hsn_code TEXT,
                    mrp TEXT,
                    alternate_image1 TEXT,
                    alternate_image2 TEXT,
                    catalogue_name TEXT,
                    change_type TEXT DEFAULT 'user_edit',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    UNIQUE(product_model)
                );
                CREATE INDEX IF NOT EXISTS idx_user_products_model ON products(product_model);
                CREATE INDEX IF NOT EXISTS idx_user_products_original ON products(original_product_id);

                CREATE TABLE IF NOT EXISTS product_specs (
                    id SERIAL PRIMARY KEY,
                    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
                    original_product_id INTEGER,
                    spec_group TEXT,
                    spec_key TEXT NOT NULL,
                    spec_value TEXT NOT NULL,
                    spec_unit TEXT,
                    change_type TEXT DEFAULT 'user_added',
                    created_at TIMESTAMPTZ DEFAULT now(),
                    CONSTRAINT user_specs_product_key_unique UNIQUE (product_id, spec_key)
                );
                CREATE INDEX IF NOT EXISTS idx_user_specs_product ON product_specs(product_id);
                CREATE INDEX IF NOT EXISTS idx_user_specs_key ON product_specs(spec_key);
                CREATE INDEX IF NOT EXISTS idx_user_specs_original ON product_specs(original_product_id);
                """
            )
        conn.commit()
    finally:
        conn.close()


def init_db():
    """Create product, metadata, and user-data databases and their base schemas."""
    _ensure_database(PRODUCT_DB_NAME)
    _ensure_database(META_DB_NAME)
    _ensure_database(USER_DB_NAME)
    _init_product_schema()
    _init_meta_schema()
    _init_user_schema()
    print("[DB] Schema ready")


def _migrate_product_schema():
    conn = get_db()
    try:
        with conn.cursor() as cur:
            for col, coltype in [
                ("alternate_image1", "TEXT"),
                ("alternate_image2", "TEXT"),
                ("catalogue_name", "TEXT"),
            ]:
                cur.execute(
                    f"""
                    DO $$ BEGIN
                        ALTER TABLE products ADD COLUMN {col} {coltype};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                    """
                )

            cur.execute(
                """
                DO $$ BEGIN
                    ALTER TABLE product_specs ADD COLUMN spec_group TEXT;
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
                """
            )
            cur.execute(
                """
                DO $$ BEGIN
                    ALTER TABLE product_specs ADD COLUMN spec_unit TEXT;
                EXCEPTION WHEN duplicate_column THEN NULL;
                END $$;
                """
            )
            cur.execute(
                """
                DO $$ BEGIN
                    ALTER TABLE product_specs ADD CONSTRAINT product_specs_product_key_unique
                        UNIQUE (product_id, spec_key);
                EXCEPTION WHEN duplicate_table THEN NULL;
                          WHEN duplicate_object THEN NULL;
                END $$;
                """
            )
        conn.commit()
    finally:
        conn.close()


def _migrate_meta_schema():
    conn = get_meta_db()
    try:
        with conn.cursor() as cur:
            for table_name, col, coltype in [
                ("ocr_runs", "text_search", "tsvector"),
                ("ocr_tables", "header_sig", "TEXT DEFAULT ''"),
                ("ocr_tables", "content_search", "tsvector"),
                ("rows", "text_search", "tsvector"),
            ]:
                cur.execute(
                    f"""
                    DO $$ BEGIN
                        ALTER TABLE {table_name} ADD COLUMN {col} {coltype};
                    EXCEPTION WHEN duplicate_column THEN NULL;
                    END $$;
                    """
                )

            cur.execute(
                """
                CREATE OR REPLACE FUNCTION rows_text_search_trigger() RETURNS trigger AS $$
                BEGIN
                    NEW.text_search := to_tsvector('english', COALESCE(NEW.raw_text, ''));
                    RETURN NEW;
                END; $$ LANGUAGE plpgsql;
                """
            )
            cur.execute(
                """
                DO $$ BEGIN
                    CREATE TRIGGER rows_fts_update BEFORE INSERT OR UPDATE OF raw_text
                        ON rows FOR EACH ROW EXECUTE FUNCTION rows_text_search_trigger();
                EXCEPTION WHEN duplicate_object THEN NULL;
                END $$;
                """
            )

            if _table_exists(conn, "ocr_runs"):
                cur.execute(
                    """
                    UPDATE ocr_runs
                    SET text_search = to_tsvector('english', COALESCE(raw_text, ''))
                    WHERE text_search IS NULL;
                    """
                )
            if _table_exists(conn, "rows"):
                cur.execute(
                    """
                    UPDATE rows
                    SET text_search = to_tsvector('english', COALESCE(raw_text, ''))
                    WHERE text_search IS NULL;
                    """
                )
            if _table_exists(conn, "ocr_tables"):
                cur.execute(
                    """
                    UPDATE ocr_tables
                    SET header_sig = ''
                    WHERE header_sig IS NULL;
                    """
                )
                cur.execute(
                    """
                    UPDATE ocr_tables
                    SET content_search = to_tsvector(
                        'english',
                        COALESCE(
                            array_to_string(ARRAY(SELECT jsonb_array_elements_text(headers)), ' ') || ' ' ||
                            array_to_string(
                                ARRAY(
                                    SELECT cell_value
                                    FROM jsonb_array_elements(rows) AS row_elem,
                                         jsonb_array_elements_text(row_elem) AS cell_value
                                ),
                                ' '
                            ),
                            ''
                        )
                    )
                    WHERE content_search IS NULL;
                    """
                )
        conn.commit()
    finally:
        conn.close()


def _split_existing_tables():
    source_conn = get_db()
    target_conn = get_meta_db()
    try:
        existing_source_tables = set(_list_public_tables(source_conn))
        source_meta_tables = sorted(existing_source_tables - PRODUCT_TABLES)
        if not source_meta_tables:
            return

        optional_tables = [
            table_name
            for table_name in source_meta_tables
            if table_name not in CORE_META_TABLES
        ]
        for table_name in optional_tables:
            if not _table_exists(target_conn, table_name):
                _clone_table_schema_to_meta(table_name)

        copy_order = [
            table_name
            for table_name in META_TABLE_COPY_ORDER
            if table_name in source_meta_tables
        ]
        copy_order.extend(
            table_name for table_name in source_meta_tables if table_name not in copy_order
        )

        for table_name in copy_order:
            if not _table_exists(target_conn, table_name):
                raise RuntimeError(
                    f"Target metadata table {table_name} does not exist in {META_DB_NAME}"
                )
            _copy_table_data(source_conn, target_conn, table_name)
            source_count = _table_count(source_conn, table_name)
            target_count = _table_count(target_conn, table_name)
            if source_count != target_count:
                raise RuntimeError(
                    f"Row count mismatch for {table_name}: {source_count} in {PRODUCT_DB_NAME}, "
                    f"{target_count} in {META_DB_NAME}"
                )

        source_conn.commit()
        target_conn.commit()

        drop_order = [
            table_name
            for table_name in META_TABLE_DROP_ORDER
            if table_name in source_meta_tables
        ]
        drop_order.extend(
            table_name for table_name in source_meta_tables if table_name not in drop_order
        )
        _drop_tables(source_conn, drop_order)
        source_conn.commit()
    finally:
        target_conn.close()
        source_conn.close()


def migrate_db():
    """Apply schema migrations and split non-product tables into lk_catalog_meta."""
    _ensure_database(PRODUCT_DB_NAME)
    _ensure_database(META_DB_NAME)
    _init_product_schema()
    _init_meta_schema()
    _migrate_product_schema()
    _migrate_meta_schema()
    _split_existing_tables()
    _migrate_product_schema()
    _migrate_meta_schema()
    print("[DB] Migration complete")


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
    conn = get_meta_db()
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
    conn = get_meta_db()
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


# ══════════════════════════════════════════════════════
# User Data DB helpers
# ══════════════════════════════════════════════════════

def save_user_product(product: dict, original_product_id: int | None = None,
                      change_type: str = "user_edit") -> int:
    """Insert or update a product in mitsubishi_user_data. Returns user product id."""
    conn = get_user_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO products (
                    original_product_id, product_name, product_model, description,
                    image_url, category, subcategory, brand, hsn_code, mrp,
                    alternate_image1, alternate_image2, catalogue_name, change_type
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (product_model) DO UPDATE SET
                    product_name = EXCLUDED.product_name,
                    description = EXCLUDED.description,
                    image_url = EXCLUDED.image_url,
                    category = EXCLUDED.category,
                    subcategory = EXCLUDED.subcategory,
                    brand = EXCLUDED.brand,
                    hsn_code = EXCLUDED.hsn_code,
                    mrp = EXCLUDED.mrp,
                    alternate_image1 = EXCLUDED.alternate_image1,
                    alternate_image2 = EXCLUDED.alternate_image2,
                    catalogue_name = EXCLUDED.catalogue_name,
                    change_type = EXCLUDED.change_type,
                    created_at = now()
                RETURNING id
                """,
                (
                    original_product_id,
                    product.get("product_name", ""),
                    product.get("product_model", ""),
                    product.get("description"),
                    product.get("image_url"),
                    product.get("category"),
                    product.get("subcategory"),
                    product.get("brand"),
                    product.get("hsn_code"),
                    product.get("mrp"),
                    product.get("alternate_image1"),
                    product.get("alternate_image2"),
                    product.get("catalogue_name"),
                    change_type,
                ),
            )
            user_pid = cur.fetchone()[0]
        conn.commit()
        return user_pid
    finally:
        conn.close()


def save_user_specs(user_product_id: int, specs: dict,
                    original_product_id: int | None = None,
                    change_type: str = "user_added"):
    """Insert or update specs in mitsubishi_user_data."""
    conn = get_user_db()
    try:
        with conn.cursor() as cur:
            for key, value in specs.items():
                if not key or not value:
                    continue
                cur.execute(
                    """
                    INSERT INTO product_specs (
                        product_id, original_product_id, spec_key, spec_value, change_type
                    ) VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (product_id, spec_key) DO UPDATE SET
                        spec_value = EXCLUDED.spec_value,
                        change_type = EXCLUDED.change_type,
                        created_at = now()
                    """,
                    (user_product_id, original_product_id, str(key), str(value), change_type),
                )
        conn.commit()
    finally:
        conn.close()


def get_user_products():
    """Fetch all products from mitsubishi_user_data."""
    conn = get_user_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.id, p.original_product_id, p.product_name, p.product_model,
                       p.category, p.brand, p.mrp, p.change_type, p.created_at
                FROM products p ORDER BY p.created_at DESC
                """
            )
            return cur.fetchall()
    finally:
        conn.close()


def get_user_product_detail(user_product_id: int):
    """Fetch a single user product with its specs."""
    conn = get_user_db()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT * FROM products WHERE id = %s", (user_product_id,)
            )
            product = cur.fetchone()
            if not product:
                return None, []
            cols = [desc[0] for desc in cur.description]
            product_dict = dict(zip(cols, product))

            cur.execute(
                "SELECT spec_key, spec_value FROM product_specs WHERE product_id = %s ORDER BY spec_key",
                (user_product_id,),
            )
            specs = cur.fetchall()
            return product_dict, specs
    finally:
        conn.close()
