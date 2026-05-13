#!/usr/bin/env python3
"""Quotation Builder — Main entry point.

Reads catalog data from `mitsubishi_test` (products) and `accessories_extract`
(accessories). Persists quotations and line items to its own `quotations` DB.

Usage:
    python main.py                       # Launch Gradio UI on port 7864
    python main.py --init-db             # Initialize quotations DB schema
    python main.py --port 7865           # Custom port
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


INTER_FONT_STACK = (
    '"Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, '
    '"Helvetica Neue", Arial, sans-serif'
)

INTER_CSS = f"""
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800&display=swap');

body,
.gradio-container,
.gradio-container *,
button, input, textarea, select,
label, table, td, th, h1, h2, h3, h4, h5, h6, p, span, div {{
    font-family: {INTER_FONT_STACK} !important;
}}

code, pre, kbd, samp,
.gradio-container .cm-editor,
.gradio-container [class*="font-mono"] {{
    font-family: "JetBrains Mono", "SF Mono", "Menlo", "Monaco", "Consolas", monospace !important;
}}
"""


def run_ui(port=7864):
    import gradio as gr
    from app.ui import create_app
    demo = create_app()
    exports_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "exports")
    images_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                              "mitsubishi_test", "data", "images")
    demo.launch(
        server_name="0.0.0.0",
        server_port=port,
        share=False,
        theme=gr.themes.Soft(font=[gr.themes.GoogleFont("Inter"), "system-ui", "sans-serif"]),
        css=INTER_CSS,
        allowed_paths=[exports_dir, images_dir],
    )


def run_init_db():
    from pipeline.db import init_db
    init_db()


def main():
    parser = argparse.ArgumentParser(description="Quotation Builder")
    parser.add_argument("--init-db", action="store_true", help="Initialize quotations DB schema")
    parser.add_argument("--port", type=int, default=7864, help="Gradio port (default: 7864)")
    args = parser.parse_args()

    if args.init_db:
        run_init_db()
    else:
        run_ui(args.port)


if __name__ == "__main__":
    main()
