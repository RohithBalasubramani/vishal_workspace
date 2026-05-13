#!/usr/bin/env python3
"""MCB Catalog Manager — Main entry point.

Usage:
    python main.py                              # Launch Gradio UI
    python main.py --extract /path/to/pdf       # Extract products from a catalog PDF
    python main.py --seed                       # Seed DB with LK MCB data
    python main.py --init-db                    # Initialize database schema
"""

import argparse
import os
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_ui(port=7862):
    from app.ui import create_app
    demo = create_app()
    image_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "images")
    demo.launch(server_name="0.0.0.0", server_port=port, share=False, allowed_paths=[image_dir])


def run_extract(pdf_path, brand=None):
    from pipeline.catalog_extractor import process_catalog
    from pipeline.image_extractor import process_pdf_images

    print(f"Processing: {pdf_path}")
    stats = process_catalog(pdf_path, brand_hint=brand)
    print(f"Extraction: {stats}")

    img_stats = process_pdf_images(pdf_path)
    print(f"Images: {img_stats}")


def run_seed():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))
    import seed_lk_catalog
    seed_lk_catalog.seed()


def run_init_db():
    from pipeline.db import init_db, migrate_db
    init_db()
    migrate_db()


def main():
    parser = argparse.ArgumentParser(description="MCB Catalog Manager")
    parser.add_argument("--extract", metavar="PDF", help="Extract products from a catalog PDF")
    parser.add_argument("--brand", help="Brand hint for extraction (e.g. Schneider, ABB)")
    parser.add_argument("--seed", action="store_true", help="Seed DB with LK MCB data")
    parser.add_argument("--init-db", action="store_true", help="Initialize database schema")
    parser.add_argument("--port", type=int, default=7862, help="Gradio port (default: 7862)")
    args = parser.parse_args()

    if args.init_db:
        run_init_db()
    elif args.seed:
        run_seed()
    elif args.extract:
        run_extract(args.extract, args.brand)
    else:
        run_ui(args.port)


if __name__ == "__main__":
    main()
