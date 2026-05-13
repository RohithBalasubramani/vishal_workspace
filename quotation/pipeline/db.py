"""Database schema and helpers for the Quotation Builder.

Databases:
- quotations          — owns quotations + quote_items (this pipeline writes)
- mitsubishi_test     — products catalog (read-only)
- accessories_extract — accessories catalog (read-only)
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal

import psycopg2
from psycopg2 import sql
from psycopg2.extras import RealDictCursor

BASE_DB_PARAMS = dict(user="postgres", host="localhost", port=5432)

QUOTES_DB = "quotations"
PRODUCT_DB = "mitsubishi_test"
ACCESSORIES_DB = "accessories_extract"

QUOTES_DB_PARAMS = dict(dbname=QUOTES_DB, **BASE_DB_PARAMS)
PRODUCT_DB_PARAMS = dict(dbname=PRODUCT_DB, **BASE_DB_PARAMS)
ACCESSORIES_DB_PARAMS = dict(dbname=ACCESSORIES_DB, **BASE_DB_PARAMS)


def get_quotes_db():
    return psycopg2.connect(**QUOTES_DB_PARAMS)


def get_product_db():
    return psycopg2.connect(**PRODUCT_DB_PARAMS)


def get_accessories_db():
    return psycopg2.connect(**ACCESSORIES_DB_PARAMS)


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


def _init_schema():
    conn = get_quotes_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS quotations (
                    id SERIAL PRIMARY KEY,
                    quote_number TEXT NOT NULL UNIQUE,
                    customer_name TEXT,
                    customer_address TEXT,
                    project_name TEXT,
                    quote_date DATE DEFAULT CURRENT_DATE,
                    valid_until DATE,
                    gst_rate NUMERIC(5,2) DEFAULT 18.00,
                    status TEXT DEFAULT 'draft',
                    notes TEXT,
                    created_at TIMESTAMPTZ DEFAULT now(),
                    updated_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_q_status ON quotations(status);
                CREATE INDEX IF NOT EXISTS idx_q_date ON quotations(quote_date);

                CREATE TABLE IF NOT EXISTS quote_items (
                    id SERIAL PRIMARY KEY,
                    quotation_id INTEGER NOT NULL REFERENCES quotations(id) ON DELETE CASCADE,
                    item_type TEXT NOT NULL,
                    source_model TEXT NOT NULL,
                    item_name TEXT NOT NULL,
                    brand TEXT,
                    category TEXT,
                    unit_price NUMERIC(14,2) DEFAULT 0,
                    quantity NUMERIC(12,3) DEFAULT 1,
                    discount_pct NUMERIC(5,2) DEFAULT 0,
                    position INTEGER DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT now()
                );
                CREATE INDEX IF NOT EXISTS idx_qi_quote ON quote_items(quotation_id);
                CREATE INDEX IF NOT EXISTS idx_qi_pos ON quote_items(quotation_id, position);
            """)
        conn.commit()
    finally:
        conn.close()


def init_db():
    _ensure_database(QUOTES_DB)
    _init_schema()
    print(f"[DB] Quotations schema ready in '{QUOTES_DB}'")


# ── Catalog read helpers ──

def _to_price(raw) -> Decimal:
    if raw is None:
        return Decimal("0")
    if isinstance(raw, (int, float, Decimal)):
        return Decimal(str(raw))
    s = str(raw).strip().replace(",", "")
    if not s:
        return Decimal("0")
    try:
        return Decimal(s)
    except Exception:
        cleaned = "".join(ch for ch in s if ch.isdigit() or ch == ".")
        if not cleaned or cleaned == ".":
            return Decimal("0")
        try:
            return Decimal(cleaned)
        except Exception:
            return Decimal("0")


def list_product_categories() -> list[str]:
    conn = get_product_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT category FROM products
                WHERE category IS NOT NULL AND category != ''
                ORDER BY category
            """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def list_product_brands() -> list[str]:
    conn = get_product_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT brand FROM products
                WHERE brand IS NOT NULL AND brand != ''
                ORDER BY brand
            """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def list_accessory_categories() -> list[str]:
    conn = get_accessories_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT DISTINCT category FROM accessories
                WHERE category IS NOT NULL AND category != ''
                ORDER BY category
            """)
            return [r[0] for r in cur.fetchall()]
    finally:
        conn.close()


def search_products(query: str = "", category: str = "", brand: str = "", limit: int = 200):
    """Return product rows as dicts: source_model, item_name, brand, category, unit_price, image_url, item_type."""
    where = ["1=1"]
    params: list = []
    q = (query or "").strip()
    if q:
        where.append("(product_model ILIKE %s OR product_name ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])
    if category:
        where.append("category = %s")
        params.append(category)
    if brand:
        where.append("brand = %s")
        params.append(brand)
    where_sql = " AND ".join(where)
    conn = get_product_db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT product_model, product_name, brand, category, mrp, image_url
                FROM products
                WHERE {where_sql}
                ORDER BY product_model
                LIMIT %s
            """, params + [limit])
            rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "item_type": "product",
            "source_model": r[0],
            "item_name": r[1],
            "brand": r[2] or "",
            "category": r[3] or "",
            "unit_price": float(_to_price(r[4])),
            "image_url": r[5] or "",
        }
        for r in rows
    ]


def search_accessories(query: str = "", category: str = "", limit: int = 200):
    where = ["1=1"]
    params: list = []
    q = (query or "").strip()
    if q:
        where.append("(accessory_model ILIKE %s OR accessory_name ILIKE %s)")
        params.extend([f"%{q}%", f"%{q}%"])
    if category:
        where.append("category = %s")
        params.append(category)
    where_sql = " AND ".join(where)
    conn = get_accessories_db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT accessory_model, accessory_name, brand, category, sub_category, mrp, image_url
                FROM accessories
                WHERE {where_sql}
                ORDER BY accessory_model
                LIMIT %s
            """, params + [limit])
            rows = cur.fetchall()
    finally:
        conn.close()
    return [
        {
            "item_type": "accessory",
            "source_model": r[0],
            "item_name": r[1],
            "brand": r[2] or "",
            "category": (r[3] or "") + ("/" + r[4] if r[4] else ""),
            "unit_price": float(_to_price(r[5])),
            "image_url": r[6] or "",
        }
        for r in rows
    ]


def lookup_item(item_type: str, source_model: str):
    """Fetch a single catalog row for refreshing prices/details."""
    if item_type == "product":
        results = search_products(query=source_model, limit=5)
        for r in results:
            if r["source_model"] == source_model:
                return r
    elif item_type == "accessory":
        results = search_accessories(query=source_model, limit=5)
        for r in results:
            if r["source_model"] == source_model:
                return r
    return None


# ── Quote CRUD ──

def _next_quote_number() -> str:
    conn = get_quotes_db()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(MAX(id), 0) + 1 FROM quotations")
            seq = cur.fetchone()[0]
        return f"Q-{datetime.now().strftime('%Y%m%d')}-{seq:04d}"
    finally:
        conn.close()


def create_quotation(header: dict, items: list[dict]) -> tuple[int, str]:
    """Create a quote. Returns (quotation_id, quote_number)."""
    quote_number = (header.get("quote_number") or "").strip() or _next_quote_number()
    conn = get_quotes_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO quotations
                    (quote_number, customer_name, customer_address, project_name,
                     quote_date, valid_until, gst_rate, status, notes)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id
            """, (
                quote_number,
                header.get("customer_name") or None,
                header.get("customer_address") or None,
                header.get("project_name") or None,
                header.get("quote_date") or date.today(),
                header.get("valid_until") or None,
                header.get("gst_rate", 18),
                header.get("status", "draft"),
                header.get("notes") or None,
            ))
            quotation_id = cur.fetchone()[0]
            for pos, it in enumerate(items):
                cur.execute("""
                    INSERT INTO quote_items
                        (quotation_id, item_type, source_model, item_name, brand, category,
                         unit_price, quantity, discount_pct, position)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    quotation_id,
                    it.get("item_type", "product"),
                    it.get("source_model", ""),
                    it.get("item_name", ""),
                    it.get("brand") or "",
                    it.get("category") or "",
                    it.get("unit_price", 0),
                    it.get("quantity", 1),
                    it.get("discount_pct", 0),
                    pos,
                ))
        conn.commit()
        return quotation_id, quote_number
    finally:
        conn.close()


def update_quotation(quotation_id: int, header: dict, items: list[dict]):
    conn = get_quotes_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE quotations SET
                    customer_name = %s,
                    customer_address = %s,
                    project_name = %s,
                    quote_date = %s,
                    valid_until = %s,
                    gst_rate = %s,
                    status = %s,
                    notes = %s,
                    updated_at = now()
                WHERE id = %s
            """, (
                header.get("customer_name") or None,
                header.get("customer_address") or None,
                header.get("project_name") or None,
                header.get("quote_date") or date.today(),
                header.get("valid_until") or None,
                header.get("gst_rate", 18),
                header.get("status", "draft"),
                header.get("notes") or None,
                quotation_id,
            ))
            cur.execute("DELETE FROM quote_items WHERE quotation_id = %s", (quotation_id,))
            for pos, it in enumerate(items):
                cur.execute("""
                    INSERT INTO quote_items
                        (quotation_id, item_type, source_model, item_name, brand, category,
                         unit_price, quantity, discount_pct, position)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    quotation_id,
                    it.get("item_type", "product"),
                    it.get("source_model", ""),
                    it.get("item_name", ""),
                    it.get("brand") or "",
                    it.get("category") or "",
                    it.get("unit_price", 0),
                    it.get("quantity", 1),
                    it.get("discount_pct", 0),
                    pos,
                ))
        conn.commit()
    finally:
        conn.close()


def delete_quotation(quotation_id: int):
    conn = get_quotes_db()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM quotations WHERE id = %s", (quotation_id,))
        conn.commit()
    finally:
        conn.close()


def get_quotation(quotation_id: int):
    conn = get_quotes_db()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM quotations WHERE id = %s", (quotation_id,))
            header = cur.fetchone()
            if not header:
                return None, []
            cur.execute("""
                SELECT id, item_type, source_model, item_name, brand, category,
                       unit_price, quantity, discount_pct, position
                FROM quote_items
                WHERE quotation_id = %s
                ORDER BY position, id
            """, (quotation_id,))
            items = [dict(r) for r in cur.fetchall()]
        # Normalise numeric/decimal values
        for it in items:
            it["unit_price"] = float(it["unit_price"] or 0)
            it["quantity"] = float(it["quantity"] or 0)
            it["discount_pct"] = float(it["discount_pct"] or 0)
        return dict(header), items
    finally:
        conn.close()


def list_quotations(query: str = "", limit: int = 200):
    where = ["1=1"]
    params: list = []
    q = (query or "").strip()
    if q:
        where.append("(quote_number ILIKE %s OR customer_name ILIKE %s OR project_name ILIKE %s)")
        params.extend([f"%{q}%"] * 3)
    where_sql = " AND ".join(where)
    conn = get_quotes_db()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""
                SELECT
                    q.id, q.quote_number, q.customer_name, q.project_name,
                    q.quote_date, q.status,
                    (SELECT COUNT(*) FROM quote_items qi WHERE qi.quotation_id = q.id) AS line_count,
                    q.updated_at
                FROM quotations q
                WHERE {where_sql}
                ORDER BY q.updated_at DESC
                LIMIT %s
            """, params + [limit])
            return cur.fetchall()
    finally:
        conn.close()
