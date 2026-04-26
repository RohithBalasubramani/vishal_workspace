"""Export mcb_test database to CSV/Excel."""

import os
import psycopg2
import pandas as pd

DB = dict(dbname="mcb_test", user="postgres", host="localhost", port=5432)
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "exports")


def export(fmt="xlsx"):
    os.makedirs(OUT_DIR, exist_ok=True)
    conn = psycopg2.connect(**DB)

    # Products table
    df_products = pd.read_sql("""
        SELECT id, catalog_no, brand, product_type, poles, rating, curve_type,
               modules, voltage, breaking_capacity, description, product_image, source_page
        FROM mcb_products ORDER BY id
    """, conn)

    # Specs as pivot table
    df_specs = pd.read_sql("""
        SELECT p.catalog_no, s.spec_key, s.spec_value
        FROM mcb_specs s JOIN mcb_products p ON s.product_id = p.id
        ORDER BY p.id, s.id
    """, conn)
    conn.close()

    # Pivot specs into wide format
    df_specs_wide = df_specs.pivot_table(index="catalog_no", columns="spec_key", values="spec_value", aggfunc="first")
    df_specs_wide = df_specs_wide.reset_index()

    if fmt == "xlsx":
        path = os.path.join(OUT_DIR, "mcb_products.xlsx")
        with pd.ExcelWriter(path, engine="openpyxl") as writer:
            df_products.to_excel(writer, sheet_name="Products", index=False)
            df_specs_wide.to_excel(writer, sheet_name="Specs (Wide)", index=False)
            df_specs.to_excel(writer, sheet_name="Specs (Key-Value)", index=False)
        print(f"Exported to {path}")
    else:
        path = os.path.join(OUT_DIR, "mcb_products.csv")
        df_products.to_csv(path, index=False)
        path2 = os.path.join(OUT_DIR, "mcb_specs.csv")
        df_specs.to_csv(path2, index=False)
        print(f"Exported to {path} and {path2}")


if __name__ == "__main__":
    export("xlsx")
