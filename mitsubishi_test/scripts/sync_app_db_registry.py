#!/usr/bin/env python3
"""Build lk_catalog-only metadata in app_db_registry.

Creates a lightweight metadata registry for the two-table product catalog:
  - app_db_targets: only the lk_catalog target
  - app_meta: top-level registry metadata for the product catalog
  - app_schemas: table-level roles and purposes
  - app_schema_fields: column-level descriptions
  - app_schema_links: parent/child and denormalized relationships
"""

from __future__ import annotations

import psycopg2

SOURCE_DB = dict(dbname="lk_catalog", user="postgres", host="localhost", port=5432)
META_DB = dict(dbname="app_db_registry", user="postgres", host="localhost", port=5432)
SOURCE_DB_TARGET_ID = "db_lk_catalog"
SOURCE_DB_URI = "postgresql://postgres@localhost/lk_catalog"
EXPECTED_TABLES = ("product_specs", "products")

SCHEMA_METADATA = {
    "products": {
        "table_role": "primary_catalog",
        "purpose_text": (
            "Master product identity and classification table; holds name, model, type, "
            "category, brand, and promoted searchable attributes."
        ),
        "description": (
            "Primary product table containing name, type, category, brand, model, and "
            "other product-level attributes."
        ),
    },
    "product_specs": {
        "table_role": "secondary_characteristics",
        "purpose_text": (
            "Child characteristics table; holds product features and specifications as "
            "key-value rows linked to a parent product."
        ),
        "description": (
            "Secondary product characteristics table containing feature/specification "
            "rows for each parent product."
        ),
    },
}

PRODUCT_FIELD_DESCRIPTIONS = {
    "id": "Primary key for the master product record.",
    "product_name": "Human-readable product name used for browsing and search.",
    "product_model": "Primary model or catalog model identifier for the product.",
    "description": "Free-text description summarizing the product.",
    "image_url": "Primary product image URL when one is available.",
    "category": "Top-level product category used for classification and filtering.",
    "created_at": "Timestamp when the product row was created.",
    "brand": "Brand or manufacturer name for the product.",
    "rating": "Promoted current or rating value used for search and deduplication.",
    "poles": "Promoted pole configuration used for search and deduplication.",
    "hsn_code": "HSN code associated with the product.",
    "modules": "Promoted module count or width when available.",
    "mrp": "Promoted list price or MRP value.",
    "standard": "Promoted standards or certifications text.",
    "product_type": "Higher-level product type or family label.",
    "catalog_number": "Ordering or catalog number exposed at the product level.",
    "subcategory": "Secondary classification beneath the main category.",
    "voltage": "Promoted rated voltage value for filtering and search.",
    "breaking_capacity": "Promoted breaking capacity value for filtering and search.",
    "curve_type": "Promoted trip curve or characteristic type.",
    "frame_size": "Promoted frame size for larger devices such as MCCBs.",
    "series": "Product series or range name.",
    "source_page": "Page number in the source catalog where the product was extracted.",
    "catalog_source_id": "Reference to the originating catalog source record stored outside lk_catalog.",
    "updated_at": "Timestamp of the latest upsert or refresh for the product row.",
    "data_sheet_url": "Link to the product data sheet when available.",
    "raw_ocr_text": "Raw OCR text retained for traceability or re-extraction.",
}

SPEC_FIELD_DESCRIPTIONS = {
    "id": "Primary key for an individual product specification row.",
    "product_id": "Foreign key to the parent products row that owns this characteristic.",
    "spec_key": "Canonical specification name, such as voltage or breaking_capacity.",
    "spec_value": "Stored value for the specification key.",
    "product_model": "Denormalized helper copy of products.product_model for lookup convenience; products remains the source of truth.",
    "category": "Denormalized helper copy of products.category for lookup convenience; products remains the source of truth.",
    "spec_unit": "Optional unit associated with spec_value, such as A, V, or kA.",
}

MANUAL_LINKS = []


def ensure_meta_schema(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS app_db_targets (
                id TEXT PRIMARY KEY,
                provider TEXT,
                conn TEXT,
                status TEXT,
                last_msg TEXT
            );
            CREATE TABLE IF NOT EXISTS app_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS app_schemas (
                id TEXT PRIMARY KEY,
                name TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS app_schema_fields (
                schema_id TEXT NOT NULL,
                key TEXT NOT NULL,
                type TEXT,
                unit TEXT,
                scale REAL,
                desc_text TEXT,
                PRIMARY KEY (schema_id, key)
            );
            CREATE TABLE IF NOT EXISTS app_schema_links (
                from_schema_id TEXT NOT NULL,
                from_field TEXT NOT NULL,
                to_schema_id TEXT NOT NULL,
                to_field TEXT NOT NULL,
                link_type TEXT,
                constraint_name TEXT,
                confidence REAL,
                desc_text TEXT,
                PRIMARY KEY (from_schema_id, from_field, to_schema_id, to_field)
            );
            """
        )
        cur.execute("ALTER TABLE app_schemas ADD COLUMN IF NOT EXISTS db_target_id TEXT")
        cur.execute("ALTER TABLE app_schemas ADD COLUMN IF NOT EXISTS table_role TEXT")
        cur.execute("ALTER TABLE app_schemas ADD COLUMN IF NOT EXISTS purpose_text TEXT")
        cur.execute("ALTER TABLE app_schemas ADD COLUMN IF NOT EXISTS description TEXT")
    conn.commit()


def fetch_tables_and_fields(conn):
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
        )
        tables = [row[0] for row in cur.fetchall()]

        cur.execute(
            """
            SELECT table_name, column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public'
            ORDER BY table_name, ordinal_position
            """
        )
        fields = cur.fetchall()

        cur.execute(
            """
            SELECT
                tc.table_name,
                kcu.column_name,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_schema = 'public'
            ORDER BY tc.table_name, kcu.column_name
            """
        )
        fk_links = cur.fetchall()

    return tables, fields, fk_links


def validate_source_tables(tables) -> None:
    table_set = set(tables)
    expected_set = set(EXPECTED_TABLES)
    missing = sorted(expected_set - table_set)
    extra = sorted(table_set - expected_set)
    if missing or extra:
        raise RuntimeError(
            "lk_catalog must contain only products and product_specs for this registry sync. "
            f"Missing: {missing or 'none'}. Extra: {extra or 'none'}."
        )


def describe_field(table_name: str, column_name: str, data_type: str, is_nullable: str) -> str:
    suffix = f" Data type: {data_type}. Nullable: {is_nullable}."
    if table_name == "products":
        description = PRODUCT_FIELD_DESCRIPTIONS.get(
            column_name,
            "Promoted product-level attribute used for identity, classification, search, or traceability.",
        )
    else:
        description = SPEC_FIELD_DESCRIPTIONS.get(
            column_name,
            "Characteristic-level field stored on a child specification row for a parent product.",
        )
    return description + suffix


def refresh_targets(meta_conn) -> None:
    with meta_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE app_db_targets")
        cur.execute(
            """
            INSERT INTO app_db_targets (id, provider, conn, status, last_msg)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                SOURCE_DB_TARGET_ID,
                "postgresql",
                SOURCE_DB_URI,
                "active",
                "Primary product catalog database containing the master products table and the secondary product_specs table.",
            ),
        )
    meta_conn.commit()


def refresh_meta(meta_conn, table_count: int, explicit_links: int, inferred_links: int) -> None:
    items = {
        "default_db_target": SOURCE_DB_TARGET_ID,
        "primary_schema": "products",
        "secondary_schema": "product_specs",
        "source_database": SOURCE_DB_URI,
        "source_table_count": str(table_count),
        "explicit_link_count": str(explicit_links),
        "inferred_link_count": str(inferred_links),
        "metadata_model": "lk_catalog-only two-table product registry",
        "schema_relationship": (
            "products is the primary product table; product_specs is the secondary characteristics table"
        ),
    }
    with meta_conn.cursor() as cur:
        for key, value in items.items():
            cur.execute(
                """
                INSERT INTO app_meta (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
                """,
                (key, value),
            )
        cur.execute("DELETE FROM app_meta WHERE NOT (key = ANY(%s))", (list(items.keys()),))
    meta_conn.commit()


def refresh_schemas(meta_conn, tables, fields, fk_links) -> int:
    field_index = {(table_name, column_name) for table_name, column_name, _, _ in fields}
    inserted_manual_links = 0

    with meta_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE app_schema_links")
        cur.execute("TRUNCATE TABLE app_schema_fields")
        cur.execute("TRUNCATE TABLE app_schemas")

        for table_name in tables:
            table_meta = SCHEMA_METADATA[table_name]
            cur.execute(
                """
                INSERT INTO app_schemas (id, name, db_target_id, table_role, purpose_text)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (
                    table_name,
                    table_name,
                    SOURCE_DB_TARGET_ID,
                    table_meta["table_role"],
                    table_meta["purpose_text"],
                ),
            )
            cur.execute(
                "UPDATE app_schemas SET description = %s WHERE id = %s",
                (table_meta["description"], table_name),
            )

        for table_name, column_name, data_type, is_nullable in fields:
            cur.execute(
                """
                INSERT INTO app_schema_fields (schema_id, key, type, unit, scale, desc_text)
                VALUES (%s, %s, %s, NULL, NULL, %s)
                """,
                (
                    table_name,
                    column_name,
                    data_type,
                    describe_field(table_name, column_name, data_type, is_nullable),
                ),
            )

        for table_name, column_name, foreign_table_name, foreign_column_name, constraint_name in fk_links:
            desc_text = f"Each product_specs row belongs to one parent products row via {constraint_name}."
            cur.execute(
                """
                INSERT INTO app_schema_links (
                    from_schema_id, from_field, to_schema_id, to_field,
                    link_type, constraint_name, confidence, desc_text
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    table_name,
                    column_name,
                    foreign_table_name,
                    foreign_column_name,
                    "foreign_key",
                    constraint_name,
                    1.0,
                    desc_text,
                ),
            )

        for row in MANUAL_LINKS:
            from_schema_id, from_field, to_schema_id, to_field, *_ = row
            if (
                (from_schema_id, from_field) not in field_index
                or (to_schema_id, to_field) not in field_index
            ):
                continue
            cur.execute(
                """
                INSERT INTO app_schema_links (
                    from_schema_id, from_field, to_schema_id, to_field,
                    link_type, constraint_name, confidence, desc_text
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (from_schema_id, from_field, to_schema_id, to_field) DO UPDATE SET
                    link_type = EXCLUDED.link_type,
                    constraint_name = EXCLUDED.constraint_name,
                    confidence = EXCLUDED.confidence,
                    desc_text = EXCLUDED.desc_text
                """,
                row,
            )
            inserted_manual_links += 1

        cur.execute("ALTER TABLE app_schemas ALTER COLUMN db_target_id SET NOT NULL")
        cur.execute("ALTER TABLE app_schemas ALTER COLUMN table_role SET NOT NULL")
    meta_conn.commit()
    return inserted_manual_links


def main() -> None:
    source_conn = psycopg2.connect(**SOURCE_DB)
    meta_conn = psycopg2.connect(**META_DB)
    try:
        ensure_meta_schema(meta_conn)
        refresh_targets(meta_conn)
        tables, fields, fk_links = fetch_tables_and_fields(source_conn)
        validate_source_tables(tables)
        manual_link_count = refresh_schemas(meta_conn, tables, fields, fk_links)
        refresh_meta(
            meta_conn,
            table_count=len(tables),
            explicit_links=len(fk_links),
            inferred_links=manual_link_count,
        )
        print(
            "[OK] Synced app_db_registry from lk_catalog only: "
            "1 db target, 2 schemas, parent/child product relationship recorded "
            f"({len(fields)} fields, {len(fk_links)} explicit links, {manual_link_count} inferred links)"
        )
    finally:
        source_conn.close()
        meta_conn.close()


if __name__ == "__main__":
    main()
