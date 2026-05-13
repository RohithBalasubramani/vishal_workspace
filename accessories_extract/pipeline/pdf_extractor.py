"""PDF text & table extraction using GLM-OCR (GPU).

GLM-OCR (2.7GB) runs alongside vLLM without GPU memory conflicts.
Uses AutoModelForImageTextToText + Processor with chat template.
"""

import os
import re
import time
import gc
import tempfile

# Preload CUDA nvrtc libs for GPU inference
import ctypes
for _nvrtc_path in [
    os.path.expanduser("~/.pyenv/versions/3.11.9/lib/python3.11/site-packages/nvidia/cu13/lib/libnvrtc-builtins.so.13.0"),
    os.path.expanduser("~/.pyenv/versions/3.11.9/lib/python3.11/site-packages/nvidia/cu13/lib/libnvrtc.so"),
    "/opt/cuda/targets/x86_64-linux/lib/libnvrtc-builtins.so.13.1",
    "/opt/cuda/targets/x86_64-linux/lib/libnvrtc-builtins.so",
]:
    if os.path.exists(_nvrtc_path):
        try:
            ctypes.CDLL(_nvrtc_path)
        except Exception:
            pass

import fitz
from PIL import Image


def get_page_count(pdf_path: str) -> int:
    doc = fitz.open(pdf_path)
    count = len(doc)
    doc.close()
    return count


# ── GLM-OCR model (lazy loaded) ──

_model = None
_proc = None
_on_gpu = False
MODEL_ID = "zai-org/GLM-OCR"


def _gpu_free_gb() -> float:
    """Return free GPU memory in GB, 0 if no CUDA."""
    try:
        import torch
        if torch.cuda.is_available():
            return torch.cuda.mem_get_info()[0] / (1024**3)
    except Exception:
        pass
    return 0.0


def _load_model(prefer_gpu: bool = True):
    global _model, _proc, _on_gpu
    if _model is not None:
        return _model, _proc

    import torch
    from transformers import AutoProcessor, AutoModelForImageTextToText

    print("[OCR] Loading GLM-OCR (2.7GB)...")
    _proc = AutoProcessor.from_pretrained(MODEL_ID, trust_remote_code=True)

    free = _gpu_free_gb()
    if prefer_gpu and free >= 3.5:
        try:
            _model = AutoModelForImageTextToText.from_pretrained(
                MODEL_ID, trust_remote_code=True,
                dtype=torch.float16, device_map="auto",
            ).eval()
            # Test inference to catch CUDA kernel compile errors early
            test_img = Image.new("RGB", (64, 64), (255, 255, 255))
            test_msgs = [{"role": "user", "content": [
                {"type": "image", "image": test_img},
                {"type": "text", "text": "test"},
            ]}]
            test_inputs = _proc.apply_chat_template(
                test_msgs, add_generation_prompt=True,
                tokenize=True, return_tensors="pt", return_dict=True,
            )
            test_inputs = {k: v.to(_model.device) if hasattr(v, 'to') else v for k, v in test_inputs.items()}
            with torch.inference_mode():
                _model.generate(**test_inputs, max_new_tokens=5, do_sample=False)
            _on_gpu = True
            print(f"[OCR] GLM-OCR loaded on GPU ({free:.1f} GB was free)")
            return _model, _proc
        except Exception as e:
            print(f"[OCR] GPU inference failed ({type(e).__name__}: {e}), falling back to CPU...")
            del _model
            _model = None
            gc.collect()
            torch.cuda.empty_cache()

    _model = AutoModelForImageTextToText.from_pretrained(
        MODEL_ID, trust_remote_code=True,
        dtype=torch.float32,
    ).eval()
    _on_gpu = False
    print(f"[OCR] GLM-OCR loaded on CPU (GPU had {free:.1f} GB free, need 3.5)")

    return _model, _proc


def _try_switch_to_gpu():
    """If currently on CPU and GPU has enough free memory, reload on GPU."""
    global _model, _proc, _on_gpu
    if _on_gpu or _model is None:
        return
    free = _gpu_free_gb()
    if free >= 3.5:
        print(f"[OCR] GPU freed up ({free:.1f} GB), switching from CPU to GPU...")
        _unload_model()
        _load_model(prefer_gpu=True)


def _unload_model():
    global _model, _proc, _on_gpu
    if _model is None:
        return
    import torch
    del _model, _proc
    _model = None
    _proc = None
    _on_gpu = False
    gc.collect()
    torch.cuda.empty_cache()
    print("[OCR] Model unloaded, memory freed")


def ocr_single_page(img: Image.Image) -> str:
    """Run GLM-OCR on a single PIL Image. Returns extracted text/HTML."""
    import torch
    model, proc = _load_model()

    if max(img.size) > 1500:
        s = 1500 / max(img.size)
        img = img.resize((int(img.width * s), int(img.height * s)), Image.LANCZOS)

    messages = [{"role": "user", "content": [
        {"type": "image", "image": img},
        {"type": "text", "text": "Table Recognition:"},
    ]}]

    try:
        inputs = proc.apply_chat_template(
            messages, add_generation_prompt=True,
            tokenize=True, return_tensors="pt", return_dict=True,
        )
        inputs = {k: v.to(model.device) if hasattr(v, 'to') else v for k, v in inputs.items()}

        with torch.inference_mode():
            outputs = model.generate(**inputs, max_new_tokens=4096, do_sample=False)

        text = proc.decode(outputs[0][inputs["input_ids"].shape[1]:], skip_special_tokens=True)
        return text.strip()

    except Exception as e:
        import traceback
        print(f"[OCR] Error: {type(e).__name__}: {e}")
        traceback.print_exc()
        return ""


def ocr_pdf(pdf_path: str, start_page: int = 1, end_page: int | None = None) -> list[dict]:
    """Run GLM-OCR on PDF pages.

    Returns list of {"page": int, "text": str} dicts.
    """
    import torch

    doc = fitz.open(os.path.abspath(pdf_path))
    total_pages = len(doc)
    start_idx = max(0, start_page - 1)
    end_idx = min(total_pages, end_page) if end_page else total_pages

    print(f"[OCR] Processing {os.path.basename(pdf_path)} pages {start_page}-{end_idx}...")

    results = []
    zoom = 200 / 72  # 200 DPI

    for page_idx in range(start_idx, end_idx):
        # Every 5 pages, check if GPU freed up and switch from CPU
        if (page_idx - start_idx) % 5 == 0:
            _try_switch_to_gpu()

        t0 = time.time()
        page = doc[page_idx]
        page_rect = page.rect

        # Split page into segments (GLM-OCR handles one table per image best)
        segments = [
            ("full", page_rect),
            ("top", fitz.Rect(page_rect.x0, page_rect.y0, page_rect.x1, page_rect.y0 + page_rect.height * 0.55)),
            ("bottom", fitz.Rect(page_rect.x0, page_rect.y0 + page_rect.height * 0.45, page_rect.x1, page_rect.y1)),
        ]

        page_text_parts = []
        for seg_name, clip_rect in segments:
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, clip=clip_rect)

            if max(pix.width, pix.height) > 1500:
                scale = 1500 / max(pix.width, pix.height)
                pix = page.get_pixmap(matrix=fitz.Matrix(zoom * scale, zoom * scale), clip=clip_rect)

            img = Image.frombytes("RGB", (pix.width, pix.height), pix.samples)
            text = ocr_single_page(img)
            if text:
                page_text_parts.append(text)

        # Combine and deduplicate
        combined = "\n\n".join(page_text_parts)
        dt = time.time() - t0

        results.append({"page": page_idx + 1, "text": combined})

        if (page_idx + 1 - start_idx) % 3 == 0 or page_idx == end_idx - 1:
            print(f"[OCR] Page {page_idx + 1}/{end_idx} ({len(combined)} chars, {dt:.1f}s)")

        gc.collect()
        torch.cuda.empty_cache()

    doc.close()
    print(f"[OCR] Done: {len(results)} pages")
    return results


def parse_tables_from_ocr(ocr_pages: list[dict]) -> list[dict]:
    """Parse HTML tables from GLM-OCR output.

    GLM-OCR returns HTML tables with <table>, <thead>, <tbody>, <tr>, <td>, <th> tags.
    """
    tables = []
    for page in ocr_pages:
        text = page.get("text", "")
        page_num = page.get("page", 0)

        # Extract non-table context
        clean = re.sub(r'<table[^>]*>.*?</table>', '', text, flags=re.DOTALL)
        lines = [l.strip() for l in clean.split('\n') if l.strip() and len(l.strip()) > 3
                 and not l.strip().startswith('<')]
        page_context = " | ".join(lines[:5])

        # Parse HTML tables — GLM-OCR uses <table class="table table-bordered">
        for tidx, table_html in enumerate(re.findall(r'<table[^>]*>(.*?)</table>', text, re.DOTALL)):
            # Extract header rows from <thead>
            headers = []
            thead = re.search(r'<thead>(.*?)</thead>', table_html, re.DOTALL)
            if thead:
                header_cells = re.findall(r'<th[^>]*>(.*?)</th>', thead.group(1), re.DOTALL)
                headers = [re.sub(r'<[^>]+>', ' ', c).strip() for c in header_cells]

            # Extract body rows from <tbody> or full table
            rows = []
            tbody = re.search(r'<tbody>(.*?)</tbody>', table_html, re.DOTALL)
            body_html = tbody.group(1) if tbody else table_html

            for row_html in re.findall(r'<tr[^>]*>(.*?)</tr>', body_html, re.DOTALL):
                cells = re.findall(r'<t[dh][^>]*>(.*?)</t[dh]>', row_html, re.DOTALL)
                cells = [re.sub(r'<[^>]+>', ' ', c).strip() for c in cells]
                if cells:
                    rows.append(cells)

            # If no thead, use first row as headers
            if not headers and rows:
                headers = rows[0]
                rows = rows[1:]

            if headers and rows:
                tables.append({
                    "headers": headers,
                    "rows": rows,
                    "page": page_num,
                    "table_index": tidx,
                    "page_context": page_context,
                })

        # Fallback: parse markdown tables if no HTML tables found
        if not any(t["page"] == page_num for t in tables):
            md_tables = re.findall(r'(\|.+\|(?:\n\|.+\|)+)', text)
            for tidx, md_table in enumerate(md_tables):
                md_rows = [r.strip() for r in md_table.strip().split('\n') if r.strip()]
                if len(md_rows) < 2:
                    continue
                parsed = []
                for row in md_rows:
                    cells = [c.strip() for c in row.split('|') if c.strip()]
                    if cells and not all(c.replace('-', '').replace(':', '').strip() == '' for c in cells):
                        parsed.append(cells)
                if len(parsed) >= 2:
                    tables.append({
                        "headers": parsed[0],
                        "rows": parsed[1:],
                        "page": page_num,
                        "table_index": tidx,
                        "page_context": page_context,
                    })

    return tables


def process(pdf_path: str, start_page: int = 1, end_page: int | None = None) -> tuple:
    """Full pipeline: OCR + parse tables.

    Returns: (ocr_pages, tables, dt, num_pages)
    """
    t0 = time.time()
    ocr_pages = ocr_pdf(pdf_path, start_page, end_page)
    tables = parse_tables_from_ocr(ocr_pages)
    dt = time.time() - t0
    num_pages = get_page_count(pdf_path)
    return ocr_pages, tables, dt, num_pages


def render_pdf_previews(pdf_path: str, max_pages: int = 8, dpi: int = 120) -> list[tuple]:
    """Render PDF pages as preview images for the gallery."""
    doc = fitz.open(pdf_path)
    pages_to_render = min(len(doc), max_pages)
    zoom = dpi / 72
    mat = fitz.Matrix(zoom, zoom)
    previews = []

    for i in range(pages_to_render):
        pix = doc[i].get_pixmap(matrix=mat)
        img_path = os.path.join(
            tempfile.gettempdir(),
            f"acc_preview_{os.path.basename(pdf_path)}_{i + 1}.png"
        )
        pix.save(img_path)
        previews.append((img_path, f"Page {i + 1}"))

    doc.close()
    return previews
