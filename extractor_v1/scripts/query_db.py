#!/usr/bin/env python3
"""Example queries for the extractor_v1 database."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline.db import get_db


def get_all_products():
    conn = get_db()
    cur = conn.cursor()
    cur.execute("""
        SELECT id, product_name, product_model, category, subcategory, brand, mrp
        FROM products
        ORDER BY id
    """)
    rows = cur.fetchall()
    conn.close()

    print(f"Total products: {len(rows)}\n")
    for r in rows:
        print(f"  [{r[0]}] {r[1]} | Model: {r[2]} | Cat: {r[3]} | Brand: {r[5]} | MRP: {r[6]}")
    return rows


def get_product_specs(product_model):
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT id, product_name FROM products WHERE product_model = %s", (product_model,))
    product = cur.fetchone()
    if not product:
        print(f"Product '{product_model}' not found.")
        conn.close()
        return

    pid, name = product
    print(f"\n{name} (Model: {product_model}, ID: {pid})")

    cur.execute("""
        SELECT spec_key, spec_value, spec_group
        FROM product_specs
        WHERE product_id = %s
        ORDER BY spec_group, spec_key
    """, (pid,))
    specs = cur.fetchall()
    conn.close()

    current_group = None
    for key, value, group in specs:
        if group != current_group:
            current_group = group
            print(f"\n  [{group or 'General'}]")
        print(f"    {key}: {value}")


def search_products(category=None, brand=None, search=None):
    conn = get_db()
    cur = conn.cursor()

    conditions = []
    params = []
    if category:
        conditions.append("category = %s")
        params.append(category)
    if brand:
        conditions.append("brand = %s")
        params.append(brand)
    if search:
        conditions.append("(product_name ILIKE %s OR product_model ILIKE %s)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    cur.execute(f"SELECT id, product_name, product_model, category, brand FROM products {where} ORDER BY id", params)
    rows = cur.fetchall()
    conn.close()

    print(f"Found {len(rows)} products:")
    for r in rows:
        print(f"  [{r[0]}] {r[1]} | {r[2]} | {r[3]} | {r[4]}")
    return rows


if __name__ == "__main__":
    if len(sys.argv) > 1:
        get_product_specs(sys.argv[1])
    else:
        get_all_products()
