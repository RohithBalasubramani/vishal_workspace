# Electrical Catalog Manager — Project Report

## Overview

The Electrical Catalog Manager is a web-based application that automates the extraction, structuring, and management of product data from electrical equipment catalog PDFs. It converts unstructured PDF catalogs into a searchable, structured database of products with specifications, pricing, and linked product images.

**Tech Stack:** Python, Gradio (Web UI), PostgreSQL, DeepSeek-OCR-2 (GPU OCR), Qwen 3.5-27B via vLLM (LLM extraction), PyMuPDF, pdfplumber

**Database:** mitsubishi_test (main catalog), mitsubishi_test_meta (processing metadata), mitsubishi_user_data (user edits)

---

## Pipeline Architecture

```
PDF Upload → DeepSeek-OCR-2 (GPU) → Table Parsing → Table Merging → Qwen 3.5-27B (vLLM) → DB Save → Image Extraction & Linking
```

### Stage 1: Document Ingestion
- Accepts single PDF or batch folder uploads
- File-level deduplication via SHA256 hash — prevents re-processing identical files
- Supports page range selection for large catalogs (e.g., pages 1-50 of a 360-page ABB catalog)

### Stage 2: Text & Table Extraction
- **Primary (CPU):** PyMuPDF + pdfplumber for PDFs with embedded text — instant, zero GPU
- **Fallback (GPU):** DeepSeek-OCR-2 for scanned PDFs or PDFs with custom font encoding (e.g., L&T catalogs with CID-encoded fonts where pdfplumber produces garbled text)
- DeepSeek-OCR-2 renders each page at 400 DPI and extracts text with full table structure (HTML tables with rowspan/colspan)
- Output: structured tables with headers and rows, plus page context (headings, descriptions)

### Stage 3: Intelligent Table Merging
- Catalogs often split product data across multiple tables on the same page (e.g., ABB has a specs table + 3 ordering code tables for N/S/H variants)
- The merger identifies related tables (specs table + ordering code tables) and combines them into unified tables
- Page context (headings like "Tmax power distribution circuit breakers — TMD/TMA — 3 Pole") is attached to each table for the LLM

### Stage 4: LLM-Based Product Extraction
- Tables are batched and sent to Qwen 3.5-27B via vLLM API
- Three extraction levels: **Basic** (name, model, category), **Standard** (+ core specs), **Detailed** (all specifications)
- Category filtering at the LLM level — extract only MCBs, or only Contactors, etc.
- Handles both product listing tables (rows = individual products) and comparison tables (columns = products, rows = specs)
- Automatic MRP mapping — recognizes "L.P.", "Price", "Rate", "Unit Price", "M.R.P." and normalizes to "mrp"
- Product model priority: Cat. No. > Order Code > Type Code > Product Name
- Concurrent processing: up to 3 parallel vLLM calls per batch

### Stage 5: Database Storage (No Duplicates)
- Products table: product_name, product_model (unique), category, brand, mrp, image_url, alternate images
- Product specs table: key-value pairs (spec_key, spec_value) per product — stores all technical specifications
- Dedup logic: existing products are never overwritten; only empty fields are filled; only new spec keys are added
- User edits saved separately to mitsubishi_user_data with change tracking (original_product_id, change_type)

### Stage 6: Image Extraction & Linking
- Extracts embedded images from PDFs; small images (<150px) are re-rendered at 300 DPI for HD quality
- Logo/watermark detection: images appearing on 3+ pages are excluded
- Image-to-product matching uses two strategies:
  - **Label matching:** finds text near images (e.g., "Type MOG-S1 (Rocker Type)") and matches to products with that label in their name/specs
  - **Positional matching:** assigns the nearest image above the product text on the same page
- No cross-contamination: ABB products only get ABB images, L&T products only get L&T images

---

## Web Application Features

### Tab 1: Upload & Extract
- Single PDF upload with extraction level, category filter, and page range selection
- Real-time PDF page preview (rendered thumbnails in a gallery)
- Editable preview table — users can correct data before saving
- Auto-save to main database on extraction; user edits save to mitsubishi_user_data
- Batch mode for processing multiple PDFs with progress tracking

### Tab 2: Browse Products
- Brand dropdown filter (ABB, L&T, Mitsubishi Electric, Tridium)
- Category dropdown filter (MCB, MCCB, Contactor, UPS, Meter, etc.)
- Full-text search across product name, model, AND product specs — type a Cat. No. (CS90232), order code (1SDA067000R1), or generic name (MNX 9-2P) and find the product
- Product detail view with specifications table and up to 3 images (primary + 2 alternates)

### Tab 3: Review & Edit
- View all products with spec counts
- Load, edit, and save product details — edits go to user data DB with audit trail
- Add new specifications to products
- User Data sub-tab showing all modifications made by users
- Delete products from main catalog

### Tab 4: Ask AI
- Chat interface powered by Qwen 3.5-27B
- Searches product database for context before answering
- Answers questions using only the catalog data (grounded responses)

### Tab 5: Export
- Export to Excel (.xlsx) with 4 sheets: Products, Specs (Pivot), Specs (Raw), User Edits
- Export to CSV (pivoted specs format)

---

## Results

| Metric | Value |
|--------|-------|
| Total Products | 3,787 |
| Products with Images | 1,566 |
| Products with MRP | 1,999 |
| Brands | ABB (1,965), L&T (1,811), Mitsubishi (1), Tridium (1) |
| Categories | 20 (MCB, MCCB, ACB, Contactor, UPS, Meter, Drive, etc.) |
| Source PDFs | 9 files (360p ABB, 121p L&T, others) |
| OCR Method | DeepSeek-OCR-2 (GPU, 400 DPI) |
| Extraction Model | Qwen 3.5-27B-FP8 via vLLM |
| GPU Usage | A100 94GB — OCR and LLM share GPU sequentially |

## Key Technical Decisions

1. **DeepSeek-OCR-2 over pdfplumber:** L&T PDFs use CID-encoded fonts that produce garbled text with text-based extractors. DeepSeek-OCR-2 reads from rendered images, giving 100% accurate text including MRP values.

2. **Table merging before LLM:** ABB catalogs split specs and ordering codes across 3-5 tables per page. Merging them gives the LLM full context, increasing product extraction from 307 to 1,965.

3. **Cat. No. as product_model:** Using the most specific identifier (catalog number/order code) as the unique key enables precise lookup. Generic names are preserved in product_name and searchable.

4. **Image label matching:** Reading text near images ("Type MOG-S1") and matching to products ensures correct image assignment across tables on the same page, preventing wrong product photos.

5. **Three-database architecture:** Separating main catalog (mitsubishi_test), processing metadata (mitsubishi_test_meta), and user edits (mitsubishi_user_data) ensures extraction data is never mixed with user modifications.
