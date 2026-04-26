"""Query the mcb_test database — useful examples."""

import psycopg2

DB = dict(dbname="mcb_test", user="postgres", host="localhost", port=5432)


def get_all_products():
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("SELECT id, catalog_no, poles, rating, curve_type, voltage, breaking_capacity, product_image FROM mcb_products ORDER BY id")
        return cur.fetchall()


def get_product_specs(catalog_no):
    conn = psycopg2.connect(**DB)
    with conn.cursor() as cur:
        cur.execute("""
            SELECT p.catalog_no, p.description, p.product_image, s.spec_key, s.spec_value
            FROM mcb_products p JOIN mcb_specs s ON p.id = s.product_id
            WHERE p.catalog_no = %s ORDER BY s.id
        """, (catalog_no,))
        return cur.fetchall()


def search_products(rating=None, curve=None, poles=None, voltage=None):
    conn = psycopg2.connect(**DB)
    conditions = []
    params = []
    if rating:
        conditions.append("rating = %s")
        params.append(rating)
    if curve:
        conditions.append("curve_type = %s")
        params.append(curve)
    if poles:
        conditions.append("poles ILIKE %s")
        params.append(f"%{poles}%")
    if voltage:
        conditions.append("voltage ILIKE %s")
        params.append(f"%{voltage}%")

    where = " AND ".join(conditions) if conditions else "1=1"
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT catalog_no, poles, rating, curve_type, voltage, breaking_capacity, product_image
            FROM mcb_products WHERE {where} ORDER BY rating, curve_type
        """, params)
        return cur.fetchall()


if __name__ == "__main__":
    print("=== All 16A C-Curve MCBs ===")
    for row in search_products(rating="16A", curve="C"):
        print(f"  {row[0]} | {row[1]} | {row[3]}-Curve | {row[4]} | {row[5]}")

    print("\n=== Specs for BB10160C ===")
    for row in get_product_specs("BB10160C"):
        print(f"  {row[3]}: {row[4]}")

    print(f"\n=== Total products: {len(get_all_products())} ===")
