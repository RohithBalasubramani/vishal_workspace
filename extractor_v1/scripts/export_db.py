#!/usr/bin/env python3
"""Export extractor_v1 database to CSV or Excel."""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from pipeline.db import get_db

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
EXPORT_DIR = os.path.join(PROJECT_DIR, "data", "exports")
os.makedirs(EXPORT_DIR, exist_ok=True)


def export_excel(output_path=None):
    if output_path is None:
        output_path = os.path.join(EXPORT_DIR, "catalog_products.xlsx")

    conn = get_db()
    df_products = pd.read_sql("SELECT * FROM products ORDER BY id", conn)
    df_specs = pd.read_sql("""
        SELECT p.product_model, s.spec_key, s.spec_value, s.spec_group
        FROM product_specs s JOIN products p ON s.product_id = p.id
        ORDER BY p.id, s.spec_group, s.spec_key
    """, conn)
    conn.close()

    df_wide = df_specs.pivot_table(
        index="product_model", columns="spec_key",
        values="spec_value", aggfunc="first"
    ).reset_index()

    # Convert timezone-aware datetimes for Excel
    for col in df_products.select_dtypes(include=['datetimetz']).columns:
        df_products[col] = df_products[col].astype(str)

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df_products.to_excel(writer, sheet_name="Products", index=False)
        df_wide.to_excel(writer, sheet_name="Specs (Pivot)", index=False)
        df_specs.to_excel(writer, sheet_name="Specs (Raw)", index=False)

    print(f"Exported {len(df_products)} products to {output_path}")
    return output_path


def export_csv(output_dir=None):
    if output_dir is None:
        output_dir = EXPORT_DIR

    conn = get_db()
    df_products = pd.read_sql("SELECT * FROM products ORDER BY id", conn)
    df_specs = pd.read_sql("""
        SELECT p.product_model, s.spec_key, s.spec_value, s.spec_group
        FROM product_specs s JOIN products p ON s.product_id = p.id
        ORDER BY p.id
    """, conn)
    conn.close()

    products_path = os.path.join(output_dir, "catalog_products.csv")
    specs_path = os.path.join(output_dir, "catalog_specs.csv")

    df_products.to_csv(products_path, index=False)
    df_specs.to_csv(specs_path, index=False)

    print(f"Exported {len(df_products)} products to {products_path}")
    print(f"Exported {len(df_specs)} specs to {specs_path}")
    return products_path, specs_path


if __name__ == "__main__":
    fmt = sys.argv[1] if len(sys.argv) > 1 else "excel"
    if fmt == "csv":
        export_csv()
    else:
        export_excel()
