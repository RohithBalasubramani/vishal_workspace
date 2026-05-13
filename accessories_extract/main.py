#!/usr/bin/env python3
"""Accessories Extract — Main entry point.

Usage:
    python main.py                       # Launch Gradio UI on port 7863
    python main.py --init-db             # Initialize database schema
    python main.py --port 7864           # Custom port
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def run_ui(port=7863):
    from app.ui import create_app
    demo = create_app()
    demo.launch(server_name="0.0.0.0", server_port=port, share=False)


def run_init_db():
    from pipeline.db import init_db
    init_db()


def main():
    parser = argparse.ArgumentParser(description="Accessories Extract Pipeline")
    parser.add_argument("--init-db", action="store_true", help="Initialize database schema")
    parser.add_argument("--port", type=int, default=7863, help="Gradio port (default: 7863)")
    args = parser.parse_args()

    if args.init_db:
        run_init_db()
    else:
        run_ui(args.port)


if __name__ == "__main__":
    main()
