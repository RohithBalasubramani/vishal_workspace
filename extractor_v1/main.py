#!/usr/bin/env python3
"""Extractor V1 — Catalog Manager (mitsubishi_test DB format).

Usage:
    python main.py                              # Launch Gradio UI
    python main.py --extract /path/to/pdf       # Extract products from a catalog PDF
    python main.py --brand "ABB"                # With brand hint
    python main.py --init-db                    # Initialize database schema
"""

import argparse
import os
import sys

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(PROJECT_DIR, "ocr_complete"))
sys.path.insert(0, PROJECT_DIR)


def run_ui(port=7863):
    from app.ui import create_app
    demo = create_app()
    demo.launch(server_name="0.0.0.0", server_port=port, share=False)


def run_extract(pdf_path, brand=None):
    from pipeline.catalog_extractor import process_catalog
    from pipeline.image_extractor import process_pdf_images

    print(f"Processing: {pdf_path}")
    stats = process_catalog(pdf_path, brand_hint=brand)
    print(f"Extraction: {stats}")

    img_stats = process_pdf_images(pdf_path)
    print(f"Images: {img_stats}")


def run_init_db():
    from pipeline.db import init_db
    init_db()


def main():
    parser = argparse.ArgumentParser(description="Extractor V1 — Catalog Manager")
    parser.add_argument("--extract", metavar="PDF", help="Extract products from a catalog PDF")
    parser.add_argument("--brand", help="Brand hint for extraction (e.g. ABB, Mitsubishi)")
    parser.add_argument("--init-db", action="store_true", help="Initialize database schema")
    parser.add_argument("--port", type=int, default=7863, help="Gradio port (default: 7863)")
    args = parser.parse_args()

    if args.init_db:
        run_init_db()
    elif args.extract:
        run_extract(args.extract, args.brand)
    else:
        run_ui(args.port)


if __name__ == "__main__":
    main()
