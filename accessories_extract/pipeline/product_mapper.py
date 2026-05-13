"""Product Mapper — Links extracted accessories to parent products in mitsubishi_test DB.

Parses the "applies_to" field from accessories and finds matching products
in the mitsubishi_test database.
"""

from __future__ import annotations

import re
from .db import get_db, get_product_db, save_product_mapping


# Frame size ranges used in Mitsubishi catalogs
FRAME_RANGES = {
    "NF63-250AF": ["NF63", "NF125", "NF160", "NF250"],
    "NF400-800AF": ["NF400", "NF630", "NF800"],
    "NF(63-250AF)": ["NF63", "NF125", "NF160", "NF250"],
    "NF(400-800AF)": ["NF400", "NF630", "NF800"],
    "NF63-NF250": ["NF63", "NF125", "NF160", "NF250"],
    "NF400-NF800": ["NF400", "NF630", "NF800"],
    "NV63-NV250": ["NV63", "NV125", "NV160", "NV250"],
    "NV400-NV800": ["NV400", "NV630", "NV800"],
    "All models": [],  # Maps to all MCCB products
}


def parse_applies_to(applies_to: str) -> list[str]:
    """Parse the 'applies_to' field into a list of frame/model prefixes.

    Examples:
        "NF63-250AF" → ["NF63", "NF125", "NF160", "NF250"]
        "NF(400-800AF)" → ["NF400", "NF630", "NF800"]
        "AE630-SW to AE4000-SWA" → ["AE630", "AE1000", "AE1250", "AE1600", "AE2000", "AE2500", "AE3200", "AE4000"]
        "For All models" → [] (maps to all)
        "S-T10 to S-T50" → ["S-T10", "S-T12", "S-T20", "S-T21", "S-T25", "S-T32", "S-T35", "S-T50"]
    """
    if not applies_to:
        return []

    text = applies_to.strip()

    # Check predefined ranges
    for pattern, frames in FRAME_RANGES.items():
        if pattern.lower() in text.lower():
            return frames

    # Parse "NF63-HV & NV63-SV" style
    if "&" in text or "," in text:
        parts = re.split(r'[&,]', text)
        result = []
        for part in parts:
            part = part.strip()
            m = re.match(r'(NF|NV|AE)\d+', part)
            if m:
                result.append(m.group(0))
        return result

    # Parse "AE630-SW 3P/4P - AE4000-SWA 3P/4P" style
    m = re.search(r'(AE\d+).*?(?:to|-)\s*(AE\d+)', text, re.IGNORECASE)
    if m:
        ae_sizes = [630, 1000, 1250, 1600, 2000, 2500, 3200, 4000, 5000, 6300]
        start = int(re.search(r'\d+', m.group(1)).group())
        end = int(re.search(r'\d+', m.group(2)).group())
        return [f"AE{s}" for s in ae_sizes if start <= s <= end]

    # Parse "S-T10 to S-T50" style
    m = re.search(r'(S-?[TN]\d+).*?(?:to|-)\s*(S-?[TN]\d+)', text, re.IGNORECASE)
    if m:
        return [m.group(1), m.group(2)]

    # Single model reference
    m = re.match(r'(NF|NV|AE|S-?[TN])\d+', text)
    if m:
        return [m.group(0)]

    # "For All models" or similar
    if "all" in text.lower():
        return []

    return [text] if text else []


def map_accessories_to_products(accessories: list[dict]) -> int:
    """Map extracted accessories to products in mitsubishi_test DB.

    For each accessory, looks up matching products and creates entries
    in accessory_product_map.

    Returns: number of mappings created.
    """
    conn = get_db()
    product_conn = get_product_db()
    total_mappings = 0

    try:
        cur = conn.cursor()
        product_cur = product_conn.cursor()

        for acc in accessories:
            applies_to = acc.get("applies_to", "")
            model = acc.get("accessory_model", "").strip()
            if not model:
                continue

            # Get accessory ID
            cur.execute("SELECT id FROM accessories WHERE accessory_model = %s", (model,))
            row = cur.fetchone()
            if not row:
                continue
            acc_id = row[0]

            # Parse frame sizes
            frame_prefixes = parse_applies_to(applies_to)

            if not frame_prefixes:
                # "All models" — map to all MCCB/ACB products
                product_cur.execute("""
                    SELECT DISTINCT product_model FROM products
                    WHERE category IN ('MCCB', 'ACB') AND brand = 'Mitsubishi'
                    ORDER BY product_model LIMIT 50
                """)
                products = [r[0] for r in product_cur.fetchall()]
            else:
                # Find matching products
                conditions = []
                params = []
                for prefix in frame_prefixes:
                    conditions.append("product_model ILIKE %s")
                    params.append(f"{prefix}%")

                if conditions:
                    product_cur.execute(f"""
                        SELECT DISTINCT product_model FROM products
                        WHERE ({' OR '.join(conditions)})
                        AND category IN ('MCCB', 'ACB')
                        ORDER BY product_model
                    """, params)
                    products = [r[0] for r in product_cur.fetchall()]
                else:
                    products = []

            # Create mappings
            for product_model in products:
                save_product_mapping(acc_id, product_model, applies_to)
                total_mappings += 1

        print(f"  [Mapper] Created {total_mappings} product mappings")
        return total_mappings

    finally:
        conn.close()
        product_conn.close()
