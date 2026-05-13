"""Generic product designation expander for catalog extraction.

Detects lookup/designation tables (e.g. "Coil Ratings & Ordering Designation")
and expands base products with voltage placeholders into all variants.

Example:
    Base product:  S-T12 AC__V 1A1B  (price 1410)
    Lookup table:  AC24V↔DC12V, AC48V↔DC24V, AC100V↔DC48V, ...

    Expanded:
        S-T12 AC24V 1A1B  (1410)  ↔  SD-T12 DC12V 1A1B  (2320)
        S-T12 AC48V 1A1B  (1410)  ↔  SD-T12 DC24V 1A1B  (2320)
        ...

Handles:
- Blank/empty DC or AC columns (only expands the side that exists)
- Dash (-) or empty values in any field (preserved as-is)
- Multiple lookup tables per catalog
- Generic placeholder patterns (not hardcoded to Mitsubishi)
"""

from __future__ import annotations

import re
from copy import deepcopy


# ── Lookup Table Detection ────────────────────────────

# Headers that indicate a designation/coil-rating lookup table
_LOOKUP_HEADER_PATTERNS = [
    r"ordering\s*designation",
    r"coil\s*rating",
    r"rated\s*voltage",
    r"ac\s*rated",
    r"dc\s*rated",
    r"designation.*ac",
    r"designation.*dc",
]

# Headers that indicate a product table with expandable references
_REFERENCE_HEADER_PATTERNS = [
    r"reference\s*with\s*ac",
    r"reference\s*with\s*dc",
    r"ac\s*coil",
    r"dc\s*coil",
    r"part\s*code",
    r"ordering\s*ref",
]


def _header_matches(headers: list[str], patterns: list[str]) -> bool:
    """Check if any header matches any pattern (case-insensitive)."""
    header_text = " ".join(str(h).lower() for h in headers)
    return any(re.search(p, header_text) for p in patterns)


def _is_lookup_table(table: dict) -> bool:
    """Detect if a table is a voltage/designation lookup table.

    Detection strategies:
    1. Header text matches known patterns (e.g., "Ordering Designation", "Coil Rating")
    2. Content-based: multiple cells contain AC/DC voltage designations (e.g., AC24V, DC12V)
       but NO cells contain voltage placeholders (AC__V, AC...V) — distinguishes lookup from product tables
    """
    headers = table.get("headers", [])
    rows = table.get("rows", [])
    if not headers:
        return False

    # Strategy 1: header pattern match
    if _header_matches(headers, _LOOKUP_HEADER_PATTERNS):
        return True

    # Strategy 2: content-based detection
    # A lookup table has cells like "AC24V", "DC12V", "AC100V" etc. as data values
    # but does NOT have placeholder patterns like "AC__V", "AC...V"
    voltage_desig_re = re.compile(r'\b(AC|DC)\d+V', re.IGNORECASE)
    all_cells = [str(h) for h in headers]
    for row in rows:
        all_cells.extend(str(c) for c in row)
    all_text = " ".join(all_cells)

    desig_count = len(voltage_desig_re.findall(all_text))
    has_placeholder = bool(_VOLTAGE_PLACEHOLDER_RE.search(all_text))

    # If there are many voltage designations and no placeholders, it's a lookup
    if desig_count >= 4 and not has_placeholder:
        return True

    return False


def _is_expandable_product_table(table: dict) -> bool:
    """Detect if a product table has references that need voltage expansion."""
    headers = table.get("headers", [])
    if not headers:
        return False
    return _header_matches(headers, _REFERENCE_HEADER_PATTERNS)


# ── Lookup Table Parsing ──────────────────────────────

_VOLTAGE_DESIG_RE = re.compile(r'^(AC|DC)\d+V', re.IGNORECASE)
_VOLTAGE_RANGE_RE = re.compile(r'^\d[\d.\-–\s]*$')


def parse_lookup_table(table: dict) -> list[dict]:
    """Parse a designation lookup table into a list of voltage mappings.

    Handles two formats:
    1. Named columns: headers like "Ordering Designation - AC", "AC Rated Voltage", etc.
    2. Content-based (OCR output): headers contain the first data row, data is in cells.
       e.g., Headers: ['For S(D)...', 'AC24V*', '24', '12', 'DC12V***']

    Returns list of dicts like:
        [
            {"ac_designation": "AC24V", "ac_voltage": "24", "dc_designation": "DC12V", "dc_voltage": "12"},
            {"ac_designation": "AC48V", "ac_voltage": "48-50", "dc_designation": "DC24V", "dc_voltage": "24"},
            ...
        ]
    """
    raw_headers = [str(h).strip() for h in table.get("headers", [])]
    headers_lower = [h.lower() for h in raw_headers]
    rows = table.get("rows", [])

    if not raw_headers:
        return []

    # ── Strategy 1: Named column headers ──
    ac_desig_idx = dc_desig_idx = ac_volt_idx = dc_volt_idx = None

    for i, h in enumerate(headers_lower):
        if re.search(r"ordering.*ac|designation.*ac|ac.*designation|ac.*ordering", h):
            ac_desig_idx = i
        elif re.search(r"ordering.*dc|designation.*dc|dc.*designation|dc.*ordering", h):
            dc_desig_idx = i
        elif re.search(r"ac.*rated.*voltage|ac.*voltage|rated.*voltage.*ac", h):
            ac_volt_idx = i
        elif re.search(r"dc.*rated.*voltage|dc.*voltage|rated.*voltage.*dc", h):
            dc_volt_idx = i

    if ac_desig_idx is not None or dc_desig_idx is not None:
        return _parse_named_columns(rows, ac_desig_idx, dc_desig_idx, ac_volt_idx, dc_volt_idx)

    # ── Strategy 2: Content-based detection ──
    # Scan headers + rows for cells matching AC/DC designation patterns
    # Build column roles from content analysis
    all_rows = [raw_headers] + [list(r) for r in rows]
    return _parse_content_based(all_rows)


def _parse_named_columns(rows, ac_desig_idx, dc_desig_idx, ac_volt_idx, dc_volt_idx):
    """Parse using known column indices."""
    mappings = []
    for row in rows:
        if not any(str(c).strip() for c in row if c):
            continue

        def _cell(idx):
            if idx is not None and idx < len(row):
                val = str(row[idx]).strip()
                return val if val and val != "-" else ""
            return ""

        mapping = {
            "ac_designation": _clean_designation(_cell(ac_desig_idx)),
            "ac_voltage": _cell(ac_volt_idx),
            "dc_designation": _clean_designation(_cell(dc_desig_idx)),
            "dc_voltage": _cell(dc_volt_idx),
        }
        if mapping["ac_designation"] or mapping["dc_designation"]:
            mappings.append(mapping)
    return mappings


def _clean_designation(val: str) -> str:
    """Remove asterisks and footnote markers from designations (e.g., 'AC24V*' → 'AC24V')."""
    return re.sub(r'[*]+$', '', val).strip()


def _parse_content_based(all_rows: list[list]) -> list[dict]:
    """Parse lookup table by detecting column roles from cell content.

    Identifies which columns contain AC designations, DC designations,
    AC voltages, and DC voltages by pattern matching cell values.

    Handles OCR quirks where the header row has a different structure
    from data rows (e.g., col 0 = label in header but AC designation in data).
    """
    if not all_rows:
        return []

    num_cols = max(len(r) for r in all_rows)

    # Score each column for each role, weighting data rows more than the header row
    col_scores = {i: {"ac_desig": 0, "dc_desig": 0, "voltage": 0, "text": 0} for i in range(num_cols)}

    for row_idx, row in enumerate(all_rows):
        # Weight data rows (idx>0) more heavily since the header row may have mixed content
        weight = 1 if row_idx == 0 else 2
        for i, cell in enumerate(row):
            val = str(cell).strip()
            if not val:
                continue
            val_clean = re.sub(r'[*]+', '', val)

            if re.match(r'^AC\d+V$', val_clean, re.IGNORECASE):
                col_scores[i]["ac_desig"] += weight
            elif re.match(r'^DC\d+V$', val_clean, re.IGNORECASE):
                col_scores[i]["dc_desig"] += weight
            elif _VOLTAGE_RANGE_RE.match(val_clean):
                col_scores[i]["voltage"] += weight
            else:
                col_scores[i]["text"] += weight

    # Assign roles based on highest score
    ac_desig_col = dc_desig_col = None
    voltage_cols = []

    for i in range(num_cols):
        scores = col_scores[i]
        max_role = max(scores, key=scores.get)
        max_score = scores[max_role]
        if max_score == 0:
            continue

        if max_role == "ac_desig" and ac_desig_col is None:
            ac_desig_col = i
        elif max_role == "dc_desig" and dc_desig_col is None:
            dc_desig_col = i
        elif max_role == "voltage":
            voltage_cols.append(i)

    if ac_desig_col is None and dc_desig_col is None:
        return []

    # Assign voltage columns based on proximity to designation columns
    ac_volt_col = dc_volt_col = None

    if ac_desig_col is not None and dc_desig_col is not None:
        # Both AC and DC designation columns found — voltage cols are between them
        low = min(ac_desig_col, dc_desig_col)
        high = max(ac_desig_col, dc_desig_col)
        between = [vc for vc in voltage_cols if low < vc < high]

        if len(between) >= 2:
            # Two voltage columns between designations
            if ac_desig_col < dc_desig_col:
                ac_volt_col = between[0]
                dc_volt_col = between[1]
            else:
                dc_volt_col = between[0]
                ac_volt_col = between[1]
        elif len(between) == 1:
            # One voltage column — assign to the nearer designation
            if ac_desig_col < dc_desig_col:
                ac_volt_col = between[0]
            else:
                dc_volt_col = between[0]
        else:
            # No voltage columns between — use nearest ones
            for vc in voltage_cols:
                if vc > ac_desig_col and ac_volt_col is None:
                    ac_volt_col = vc
                elif vc > dc_desig_col and dc_volt_col is None:
                    dc_volt_col = vc
    else:
        # Only one designation column — assign voltage cols sequentially
        desig_col = ac_desig_col if ac_desig_col is not None else dc_desig_col
        for vc in voltage_cols:
            if vc > desig_col:
                if ac_desig_col is not None and ac_volt_col is None:
                    ac_volt_col = vc
                elif dc_desig_col is not None and dc_volt_col is None:
                    dc_volt_col = vc

    # Build mappings from all rows
    mappings = []

    # For the header row (row 0), the AC/DC designations may be in different columns
    # than the data rows (e.g., header has AC24V in col 1, data has AC48V in col 0).
    # Scan the header row for any AC/DC designations regardless of column assignment.
    if all_rows:
        header_row = all_rows[0]
        header_ac = header_dc = header_ac_v = header_dc_v = ""
        ac_v_candidates = []
        dc_v_candidates = []

        for i, cell in enumerate(header_row):
            val = _clean_designation(str(cell).strip())
            if re.match(r'^AC\d+V$', val, re.IGNORECASE):
                header_ac = val
            elif re.match(r'^DC\d+V$', val, re.IGNORECASE):
                header_dc = val
            elif _VOLTAGE_RANGE_RE.match(val):
                if not header_ac_v and header_ac:
                    header_ac_v = val
                else:
                    header_dc_v = val

        if header_ac or header_dc:
            mappings.append({
                "ac_designation": header_ac,
                "ac_voltage": header_ac_v,
                "dc_designation": header_dc,
                "dc_voltage": header_dc_v,
            })

    # Parse data rows using the detected column assignments
    for row in all_rows[1:]:
        def _cell(idx):
            if idx is not None and idx < len(row):
                val = str(row[idx]).strip()
                return val if val and val != "-" else ""
            return ""

        ac_d = _clean_designation(_cell(ac_desig_col))
        dc_d = _clean_designation(_cell(dc_desig_col))
        ac_v = _cell(ac_volt_col)
        dc_v = _cell(dc_volt_col)

        # Skip rows that are clearly not data
        if not ac_d and not dc_d:
            continue
        # Skip if the "designation" is actually a long text description
        if ac_d and len(ac_d) > 20:
            continue

        # Validate designations — must match AC/DC + number + V pattern
        # If a value doesn't match (e.g., "220" in the AC column due to OCR column shift),
        # it's likely a voltage value, not a designation
        ac_desig_valid = bool(re.match(r'^(AC|DC)\d+V$', ac_d, re.IGNORECASE)) if ac_d else False
        dc_desig_valid = bool(re.match(r'^(AC|DC)\d+V$', dc_d, re.IGNORECASE)) if dc_d else False

        # Try to recover designations from misaligned columns
        if not ac_desig_valid and not dc_desig_valid:
            # Check all cells in this row for valid designations
            found_ac = found_dc = ""
            found_voltages = []
            for cell in row:
                val = _clean_designation(str(cell).strip())
                if re.match(r'^AC\d+V$', val, re.IGNORECASE):
                    found_ac = val
                elif re.match(r'^DC\d+V$', val, re.IGNORECASE):
                    found_dc = val
                elif _VOLTAGE_RANGE_RE.match(val):
                    found_voltages.append(val)

            ac_d = found_ac
            dc_d = found_dc
            # Assign voltages if found
            if found_voltages:
                if not ac_v and found_voltages:
                    ac_v = found_voltages[0]
                if not dc_v and len(found_voltages) > 1:
                    dc_v = found_voltages[1]
                elif not dc_v and found_voltages:
                    dc_v = found_voltages[-1]

        # Handle AC500V → DC200V + DC220V case: when AC designation is empty
        # but DC designation exists, pair it with the previous AC designation
        if not ac_d and dc_d and mappings:
            prev_ac = mappings[-1].get("ac_designation", "")
            ac_d = prev_ac  # Reuse previous AC designation

        mapping = {
            "ac_designation": ac_d,
            "ac_voltage": ac_v,
            "dc_designation": dc_d,
            "dc_voltage": dc_v,
        }
        if mapping["ac_designation"] or mapping["dc_designation"]:
            mappings.append(mapping)

    return mappings


# ── Placeholder Detection & Substitution ──────────────

# Common voltage placeholder patterns in product references
# e.g., "S-T12 AC__V 1A1B", "SD-T12 DC__V 1A1B", "S-T10 AC...V 1A"
# Handles underscores, dots, spaces, ellipsis as placeholders
_VOLTAGE_PLACEHOLDER_RE = re.compile(
    r'(AC|DC)([_.\s…]*V\b)',
    re.IGNORECASE
)

# Pattern to match the voltage part for substitution
# Matches: "AC__V", "AC___V", "AC_V", "AC...V", "AC…V", "DC__V", etc.
_AC_PLACEHOLDER_RE = re.compile(r'AC[_.\s…]*V', re.IGNORECASE)
_DC_PLACEHOLDER_RE = re.compile(r'DC[_.\s…]*V', re.IGNORECASE)


def _has_voltage_placeholder(text: str) -> bool:
    """Check if a string contains a voltage placeholder like AC__V or DC__V."""
    if not text:
        return False
    return bool(_VOLTAGE_PLACEHOLDER_RE.search(text))


def _substitute_voltage(reference: str, designation: str, is_dc: bool = False) -> str:
    """Replace the voltage placeholder in a reference with a specific designation.

    Example:
        _substitute_voltage("S-T12 AC__V 1A1B", "AC24V", is_dc=False)
        → "S-T12 AC24V 1A1B"

        _substitute_voltage("SD-T12 DC__V 1A1B", "DC12V", is_dc=True)
        → "SD-T12 DC12V 1A1B"
    """
    if not reference or not designation:
        return reference

    pattern = _DC_PLACEHOLDER_RE if is_dc else _AC_PLACEHOLDER_RE
    result = pattern.sub(designation, reference, count=1)
    return result


# ── Cell Blank/Symbol Detection ───────────────────────

def _is_blank_or_symbol(value: str | None) -> bool:
    """Check if a cell value is blank, dash, or other empty-equivalent symbol."""
    if value is None:
        return True
    v = str(value).strip()
    return v in ("", "-", "–", "—", "N/A", "n/a", "NA", "na", "None", "null", "nil", "*")


# ── Product Expansion ─────────────────────────────────

def expand_products(products: list[dict], lookup_mappings: list[dict]) -> list[dict]:
    """Expand products with voltage placeholders using lookup mappings.

    For each product whose product_model contains a voltage placeholder (AC__V / DC__V):
    - Generate one variant per mapping row in the lookup table
    - Substitute the placeholder with the actual designation
    - Pair AC and DC designations from the same lookup row
    - If the original product has no DC reference (blank), skip DC expansion

    Products without placeholders are passed through unchanged.

    Args:
        products: list of product dicts from LLM extraction
        lookup_mappings: parsed lookup table from parse_lookup_table()

    Returns:
        Expanded list of product dicts (original non-expandable products + expanded variants)
    """
    if not lookup_mappings:
        return products

    expanded = []

    for product in products:
        model = product.get("product_model", "")
        specs = product.get("specs", {})

        # Check if this product has a voltage placeholder
        if not _has_voltage_placeholder(model):
            # Also check spec values for references with placeholders
            has_expandable_spec = False
            for key, val in specs.items():
                if isinstance(val, str) and _has_voltage_placeholder(val):
                    has_expandable_spec = True
                    break

            if not has_expandable_spec:
                expanded.append(product)
                continue

        # Determine if this is an AC or DC base product
        model_upper = model.upper()
        is_ac_product = bool(_AC_PLACEHOLDER_RE.search(model))
        is_dc_product = bool(_DC_PLACEHOLDER_RE.search(model))

        for mapping in lookup_mappings:
            variant = deepcopy(product)

            # Expand the product_model
            new_model = model
            if is_ac_product and mapping.get("ac_designation"):
                new_model = _substitute_voltage(new_model, mapping["ac_designation"], is_dc=False)
            if is_dc_product and mapping.get("dc_designation"):
                new_model = _substitute_voltage(new_model, mapping["dc_designation"], is_dc=True)

            variant["product_model"] = new_model

            # Expand any spec values that have placeholders
            new_specs = {}
            for key, val in variant.get("specs", {}).items():
                if isinstance(val, str) and _has_voltage_placeholder(val):
                    if _AC_PLACEHOLDER_RE.search(val) and mapping.get("ac_designation"):
                        val = _substitute_voltage(val, mapping["ac_designation"], is_dc=False)
                    if _DC_PLACEHOLDER_RE.search(val) and mapping.get("dc_designation"):
                        val = _substitute_voltage(val, mapping["dc_designation"], is_dc=True)
                new_specs[key] = val

            # Add voltage info to specs
            if mapping.get("ac_voltage"):
                new_specs["ac_rated_voltage"] = mapping["ac_voltage"]
            if mapping.get("dc_voltage"):
                new_specs["dc_rated_voltage"] = mapping["dc_voltage"]
            if mapping.get("ac_designation"):
                new_specs["ac_ordering_designation"] = mapping["ac_designation"]
            if mapping.get("dc_designation"):
                new_specs["dc_ordering_designation"] = mapping["dc_designation"]

            variant["specs"] = new_specs

            # Update product_name to include voltage
            name = variant.get("product_name", "")
            desig = mapping.get("ac_designation") or mapping.get("dc_designation") or ""
            if desig and desig not in name:
                variant["product_name"] = f"{name} {desig}".strip()

            expanded.append(variant)

    return expanded


def expand_paired_products(
    products: list[dict],
    lookup_mappings: list[dict],
    ac_ref_key: str = "reference_with_ac_coils",
    dc_ref_key: str = "reference_with_dc_coils",
    ac_price_key: str | None = None,
    dc_price_key: str | None = None,
) -> list[dict]:
    """Expand products that have paired AC/DC reference columns.

    This handles the common catalog pattern where a single row has:
    - An AC reference column (e.g., "S-T12 AC__V 1A1B")
    - A DC reference column (e.g., "SD-T12 DC__V 1A1B")
    - Separate prices for AC and DC

    The AC and DC references are expanded in lockstep using the lookup table,
    pairing AC24V↔DC12V, AC48V↔DC24V, etc.

    Blank DC references → only AC variants are generated.
    Blank AC references → only DC variants are generated.

    Args:
        products: list of product dicts where specs contain ac_ref_key/dc_ref_key
        lookup_mappings: parsed lookup table
        ac_ref_key: spec key for the AC reference column
        dc_ref_key: spec key for the DC reference column
        ac_price_key: spec key for AC price (optional)
        dc_price_key: spec key for DC price (optional)

    Returns:
        Expanded product list
    """
    if not lookup_mappings:
        return products

    # Auto-detect AC/DC reference keys from specs
    if products:
        sample_specs = products[0].get("specs", {})
        spec_keys_lower = {k.lower().replace(" ", "_"): k for k in sample_specs.keys()}

        for k_lower, k_orig in spec_keys_lower.items():
            if "reference" in k_lower and "ac" in k_lower:
                ac_ref_key = k_orig
            elif "reference" in k_lower and "dc" in k_lower:
                dc_ref_key = k_orig

    expanded = []

    for product in products:
        specs = product.get("specs", {})
        ac_ref = str(specs.get(ac_ref_key, "")).strip()
        dc_ref = str(specs.get(dc_ref_key, "")).strip()

        has_ac_placeholder = _has_voltage_placeholder(ac_ref)
        has_dc_placeholder = _has_voltage_placeholder(dc_ref)
        ac_is_blank = _is_blank_or_symbol(ac_ref)
        dc_is_blank = _is_blank_or_symbol(dc_ref)

        # If neither side has a placeholder, pass through
        if not has_ac_placeholder and not has_dc_placeholder:
            expanded.append(product)
            continue

        for mapping in lookup_mappings:
            variant = deepcopy(product)
            new_specs = variant.get("specs", {})

            # Expand AC reference
            if has_ac_placeholder and mapping.get("ac_designation"):
                new_ac = _substitute_voltage(ac_ref, mapping["ac_designation"], is_dc=False)
                new_specs[ac_ref_key] = new_ac
                # Use AC reference as product_model
                variant["product_model"] = new_ac

            # Expand DC reference (only if not blank)
            if has_dc_placeholder and not dc_is_blank and mapping.get("dc_designation"):
                new_dc = _substitute_voltage(dc_ref, mapping["dc_designation"], is_dc=True)
                new_specs[dc_ref_key] = new_dc

            # Add voltage metadata
            if mapping.get("ac_voltage"):
                new_specs["ac_rated_voltage"] = mapping["ac_voltage"]
            if mapping.get("dc_voltage"):
                new_specs["dc_rated_voltage"] = mapping["dc_voltage"]
            if mapping.get("ac_designation"):
                new_specs["ac_ordering_designation"] = mapping["ac_designation"]
            if mapping.get("dc_designation"):
                new_specs["dc_ordering_designation"] = mapping["dc_designation"]

            variant["specs"] = new_specs

            # Update product name
            name = variant.get("product_name", "")
            desig = mapping.get("ac_designation") or mapping.get("dc_designation") or ""
            if desig and desig not in name:
                variant["product_name"] = f"{name} {desig}".strip()

            expanded.append(variant)

    return expanded


# ── Table-Level Expansion (pre-LLM) ──────────────────

def expand_table_rows(
    product_table: dict,
    lookup_mappings: list[dict],
) -> dict:
    """Expand a raw table's rows using lookup mappings (before LLM extraction).

    Detects columns with voltage placeholders and expands each row into
    N rows (one per lookup mapping entry). Preserves blank columns.

    Args:
        product_table: {"headers": [...], "rows": [[...], ...]}
        lookup_mappings: from parse_lookup_table()

    Returns:
        New table dict with expanded rows
    """
    headers = product_table.get("headers", [])
    rows = product_table.get("rows", [])

    if not headers or not rows or not lookup_mappings:
        return product_table

    # Find columns that contain voltage placeholders
    ac_col_indices = []
    dc_col_indices = []

    # Check headers first
    for i, h in enumerate(headers):
        h_lower = str(h).lower()
        if re.search(r"reference.*ac|ac.*coil|ac.*ref", h_lower):
            ac_col_indices.append(i)
        elif re.search(r"reference.*dc|dc.*coil|dc.*ref", h_lower):
            dc_col_indices.append(i)

    # If no header match, scan first few rows for placeholders
    if not ac_col_indices and not dc_col_indices:
        for row in rows[:5]:
            for i, cell in enumerate(row):
                cell_str = str(cell).strip()
                if _AC_PLACEHOLDER_RE.search(cell_str) and i not in ac_col_indices:
                    ac_col_indices.append(i)
                elif _DC_PLACEHOLDER_RE.search(cell_str) and i not in dc_col_indices:
                    dc_col_indices.append(i)

    if not ac_col_indices and not dc_col_indices:
        return product_table

    expanded_rows = []

    for row in rows:
        # Check if this row has any expandable cells
        has_expandable = False
        for idx in ac_col_indices + dc_col_indices:
            if idx < len(row) and _has_voltage_placeholder(str(row[idx])):
                has_expandable = True
                break

        if not has_expandable:
            expanded_rows.append(row)
            continue

        for mapping in lookup_mappings:
            new_row = list(row)  # shallow copy

            for idx in ac_col_indices:
                if idx < len(new_row):
                    cell = str(new_row[idx]).strip()
                    if _has_voltage_placeholder(cell) and mapping.get("ac_designation"):
                        new_row[idx] = _substitute_voltage(cell, mapping["ac_designation"], is_dc=False)

            for idx in dc_col_indices:
                if idx < len(new_row):
                    cell = str(new_row[idx]).strip()
                    if _is_blank_or_symbol(cell):
                        # Blank DC → keep blank (no DC variant for this product)
                        pass
                    elif _has_voltage_placeholder(cell) and mapping.get("dc_designation"):
                        new_row[idx] = _substitute_voltage(cell, mapping["dc_designation"], is_dc=True)

            expanded_rows.append(new_row)

    result = {**product_table, "rows": expanded_rows}
    return result


# ── High-Level Pipeline Integration ───────────────────

def detect_and_extract_lookups(tables: list[dict]) -> tuple[list[dict], list[dict]]:
    """Separate lookup tables from product tables and parse the lookups.

    Args:
        tables: all extracted tables from the PDF

    Returns:
        (lookup_mappings, remaining_tables)
        - lookup_mappings: combined list of all voltage mappings found
        - remaining_tables: tables that are NOT lookup tables (product tables)
    """
    all_mappings = []
    remaining = []

    for table in tables:
        if _is_lookup_table(table):
            mappings = parse_lookup_table(table)
            if mappings:
                print(f"  [Expander] Found lookup table with {len(mappings)} voltage mappings")
                all_mappings.extend(mappings)
            else:
                remaining.append(table)
        else:
            remaining.append(table)

    return all_mappings, remaining


def expand_tables(tables: list[dict]) -> list[dict]:
    """Auto-detect lookup tables and expand product tables.

    This is the main entry point for table-level expansion (pre-LLM).
    Call this on the raw tables before sending them to the LLM for extraction.

    Args:
        tables: all tables extracted from the PDF

    Returns:
        Expanded tables (lookup tables removed, product tables expanded)
    """
    lookup_mappings, product_tables = detect_and_extract_lookups(tables)

    if not lookup_mappings:
        return tables

    expanded = []
    total_original = 0
    total_expanded = 0

    for table in product_tables:
        original_rows = len(table.get("rows", []))
        total_original += original_rows

        expanded_table = expand_table_rows(table, lookup_mappings)
        expanded_rows = len(expanded_table.get("rows", []))
        total_expanded += expanded_rows

        expanded.append(expanded_table)

    if total_expanded > total_original:
        print(f"  [Expander] Expanded {total_original} rows → {total_expanded} rows "
              f"({total_expanded - total_original} new variants)")

    return expanded


def expand_extracted_products(products: list[dict], tables: list[dict]) -> list[dict]:
    """Post-LLM expansion: expand already-extracted products using lookup tables.

    Use this when products have already been extracted by the LLM but still
    contain voltage placeholders in their product_model or specs.

    Args:
        products: LLM-extracted product dicts
        tables: raw tables (to find lookup tables)

    Returns:
        Expanded product list
    """
    lookup_mappings, _ = detect_and_extract_lookups(tables)

    if not lookup_mappings:
        return products

    original_count = len(products)

    # Check if products use paired AC/DC reference pattern
    has_paired = False
    for p in products[:5]:
        specs = p.get("specs", {})
        for key in specs:
            k_lower = key.lower().replace(" ", "_")
            if "reference" in k_lower and ("ac" in k_lower or "dc" in k_lower):
                has_paired = True
                break
        if has_paired:
            break

    if has_paired:
        expanded = expand_paired_products(products, lookup_mappings)
    else:
        expanded = expand_products(products, lookup_mappings)

    if len(expanded) > original_count:
        print(f"  [Expander] Expanded {original_count} products → {len(expanded)} products "
              f"({len(expanded) - original_count} new variants)")

    return expanded
