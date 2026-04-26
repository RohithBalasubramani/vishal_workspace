"""Seed the lk_catalog database with Lauritz Knudsen MCB catalog data."""

import psycopg2

DB_PARAMS = dict(dbname="lk_catalog", user="postgres", host="localhost", port=5432)


def get_db():
    return psycopg2.connect(**DB_PARAMS)


def insert_product(cur, name, model, description, image_url, category):
    """Insert a product and return its ID. Skip if model already exists."""
    cur.execute("SELECT id FROM products WHERE product_model = %s", (model,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        """INSERT INTO products (product_name, product_model, description, image_url, category)
           VALUES (%s, %s, %s, %s, %s) RETURNING id""",
        (name, model, description, image_url, category)
    )
    return cur.fetchone()[0]


def insert_specs(cur, product_id, specs):
    """Insert key-value specs for a product."""
    for key, value in specs.items():
        if value and str(value).strip() and str(value).strip() != "-":
            cur.execute(
                "INSERT INTO product_specs (product_id, spec_key, spec_value) VALUES (%s, %s, %s)",
                (product_id, key, str(value))
            )


def seed():
    conn = get_db()
    cur = conn.cursor()

    # ══════════════════════════════════════════════════════════════
    # MCB 0.5A-63A (BB Series) — IS/IEC 60898 10kA
    # ══════════════════════════════════════════════════════════════

    mcb_description = (
        "Miniature Circuit Breaker (MCB) [8536] by Lauritz Knudsen. "
        "Conforms to IS/IEC 60898 - 10kA and IS/IEC 60947-2. "
        "Features: True contact indication, label holder, trip free mechanism, "
        "advanced current limiting hammer trip, no line-load bias, bi-connect feature."
    )

    mcb_common_specs = {
        "Brand": "Lauritz Knudsen",
        "Product Type": "Miniature Circuit Breaker (MCB)",
        "HSN Code": "8536",
        "Standard": "IS/IEC 60898, IS/IEC 60947-2",
        "Breaking Capacity (C-curve ≤40A)": "15kA",
        "Breaking Capacity (C-curve 50A-63A)": "10kA",
        "Module Width": "17.5mm",
        "Features": "True contact indication, Label holder, Trip free mechanism, No line-load bias, Bi-connect",
        "Watt Loss": "Almost 50% of IS/IEC 60898 prescribed values",
    }

    # Ratings and prices for each pole configuration
    # Format: (rating, modules, b_cat, b_mrp, c_cat, c_mrp, d_cat, d_mrp)
    sp_data = [
        ("0.5A", 1, None, None, "BB10E50C", 540, "BB10E50D", 560),
        ("1A", 1, None, None, "BB10010C", 540, "BB10010D", 560),
        ("2A", 1, None, None, "BB10020C", 540, "BB10020D", 560),
        ("3A", 1, None, None, "BB10030C", 540, "BB10030D", 560),
        ("4A", 1, None, None, "BB10040C", 540, "BB10040D", 560),
        ("6A", 1, "BB10060B", 318, "BB10060C", 318, "BB10060D", 470),
        ("10A", 1, "BB10100B", 318, "BB10100C", 318, "BB10100D", 470),
        ("16A", 1, "BB10160B", 318, "BB10160C", 318, "BB10160D", 470),
        ("20A", 1, "BB10200B", 318, "BB10200C", 318, "BB10200D", 470),
        ("25A", 1, "BB10250B", 318, "BB10250C", 318, "BB10250D", 470),
        ("32A", 1, "BB10320B", 318, "BB10320C", 318, "BB10320D", 470),
        ("40A", 1, "BB10400B", 756, "BB10400C", 756, "BB10400D", 840),
        ("50A", 1, "BB10500B", 756, "BB10500C", 756, "BB10500D*", 840),
        ("63A", 1, "BB10630B", 756, "BB10630C", 756, "BB10630D*", 840),
    ]

    dp_data = [
        ("0.5A", 2, None, None, "BB20E50C", 1550, "BB20E50D", 1575),
        ("1A", 2, None, None, "BB20010C", 1550, "BB20010D", 1575),
        ("2A", 2, None, None, "BB20020C", 1550, "BB20020D", 1575),
        ("3A", 2, None, None, "BB20030C", 1550, "BB20030D", 1575),
        ("4A", 2, None, None, "BB20040C", 1550, "BB20040D", 1575),
        ("6A", 2, "BB20060B", 1080, "BB20060C", 1080, "BB20060D", 1145),
        ("10A", 2, "BB20100B", 1080, "BB20100C", 1080, "BB20100D", 1145),
        ("16A", 2, "BB20160B", 1080, "BB20160C", 1080, "BB20160D", 1145),
        ("20A", 2, "BB20200B", 1080, "BB20200C", 1080, "BB20200D", 1145),
        ("25A", 2, "BB20250B", 1080, "BB20250C", 1080, "BB20250D", 1145),
        ("32A", 2, "BB20320B", 1080, "BB20320C", 1080, "BB20320D", 1145),
        ("40A", 2, "BB20400B", 1760, "BB20400C", 1760, "BB20400D", 1870),
        ("50A", 2, "BB20500B", 1760, "BB20500C", 1760, "BB20500D*", 1870),
        ("63A", 2, "BB20630B", 1760, "BB20630C", 1760, "BB20630D*", 1870),
    ]

    tp_data = [
        ("0.5A", 3, None, None, "BB30E50C", 2255, "BB30E50D", 2290),
        ("1A", 3, None, None, "BB30010C", 2255, "BB30010D", 2290),
        ("2A", 3, None, None, "BB30020C", 2255, "BB30020D", 2290),
        ("3A", 3, None, None, "BB30030C", 2255, "BB30030D", 2290),
        ("4A", 3, None, None, "BB30040C", 2255, "BB30040D", 2290),
        ("6A", 3, "BB30060B", 1700, "BB30060C", 1700, "BB30060D", 1825),
        ("10A", 3, "BB30100B", 1700, "BB30100C", 1700, "BB30100D", 1825),
        ("16A", 3, "BB30160B", 1700, "BB30160C", 1700, "BB30160D", 1825),
        ("20A", 3, "BB30200B", 1700, "BB30200C", 1700, "BB30200D", 1825),
        ("25A", 3, "BB30250B", 1700, "BB30250C", 1700, "BB30250D", 1825),
        ("32A", 3, "BB30320B", 1700, "BB30320C", 1700, "BB30320D", 1825),
        ("40A", 3, "BB30400B", 2650, "BB30400C", 2650, "BB30400D", 2825),
        ("50A", 3, "BB30500B", 2650, "BB30500C", 2650, "BB30500D*", 2825),
        ("63A", 3, "BB30630B", 2650, "BB30630C", 2650, "BB30630D*", 2825),
    ]

    fp_data = [
        ("0.5A", 4, None, None, "BB40E50C", 2840, "BB40E50D", 2845),
        ("1A", 4, None, None, "BB40010C", 2840, "BB40010D", 2845),
        ("2A", 4, None, None, "BB40020C", 2840, "BB40020D", 2845),
        ("3A", 4, None, None, "BB40030C", 2840, "BB40030D", 2845),
        ("4A", 4, None, None, "BB40040C", 2840, "BB40040D", 2845),
        ("6A", 4, "BB40060B", 2355, "BB40060C", 2355, "BB40060D", 2460),
        ("10A", 4, "BB40100B", 2355, "BB40100C", 2355, "BB40100D", 2460),
        ("16A", 4, "BB40160B", 2355, "BB40160C", 2355, "BB40160D", 2460),
        ("20A", 4, "BB40200B", 2355, "BB40200C", 2355, "BB40200D", 2460),
        ("25A", 4, "BB40250B", 2355, "BB40250C", 2355, "BB40250D", 2460),
        ("32A", 4, "BB40320B", 2355, "BB40320C", 2355, "BB40320D", 2460),
        ("40A", 4, "BB40400B", 3305, "BB40400C", 3305, "BB40400D", 3640),
        ("50A", 4, "BB40500B", 3305, "BB40500C", 3305, "BB40500D*", 3640),
        ("63A", 4, "BB40630B", 3305, "BB40630C", 3305, "BB40630D*", 3640),
    ]

    pole_configs = [
        ("Single Pole (SP)", "SP", sp_data, "SP - 12 Nos."),
        ("Double Pole (DP)", "DP", dp_data, "DP - 6 Nos."),
        ("Three Pole (TP)", "TP", tp_data, "TP - 4 Nos."),
        ("Four Pole (FP)", "FP", fp_data, "FP - 3 Nos."),
    ]

    count = 0
    for pole_name, pole_code, data, pack_qty in pole_configs:
        for rating, modules, b_cat, b_mrp, c_cat, c_mrp, d_cat, d_mrp in data:
            # Insert each curve variant as a separate product
            for curve, cat_no, mrp in [("B", b_cat, b_mrp), ("C", c_cat, c_mrp), ("D", d_cat, d_mrp)]:
                if cat_no is None:
                    continue
                name = f"LK MCB {pole_code} {rating} {curve}-Curve"
                pid = insert_product(cur, name, cat_no, mcb_description, None, "MCB")
                specs = {
                    **mcb_common_specs,
                    "Rating": rating,
                    "Poles": pole_code,
                    "Pole Configuration": pole_name,
                    "Modules": str(modules),
                    "Curve Type": f"{curve}-Curve",
                    "Catalog Number": cat_no,
                    "MRP (₹)": str(mrp),
                    "Standard Pack Quantity": pack_qty,
                }
                insert_specs(cur, pid, specs)
                count += 1

    print(f"  Inserted {count} MCB products (BB Series)")

    # ══════════════════════════════════════════════════════════════
    # HR MCB 80A-125A (AU15S Series) — IS/IEC 60947-2 15kA
    # ══════════════════════════════════════════════════════════════

    hr_description = (
        "BIS-Certified HR MCB 80A-125A [8536] by Lauritz Knudsen. "
        "Short circuit Breaking Capacity 15kA (IS/IEC 60947-2, EN 60947-2). "
        "C & D Characteristics. Available in SP, DP, TP and FP. "
        "Suitable for Isolation. Protection Degree IP20."
    )

    hr_common_specs = {
        "Brand": "Lauritz Knudsen",
        "Product Type": "High Rating MCB (HR MCB)",
        "HSN Code": "8536",
        "Standard": "IS/IEC 60947-2, EN 60947-2",
        "Breaking Capacity": "15kA",
        "Protection Degree": "IP20",
        "Certification": "BIS Certified",
        "Features": "Suitable for Isolation, Wide range of site mountable accessories",
    }

    hr_data = [
        # (rating, pole, modules, c_cat, c_mrp, d_cat, d_mrp)
        ("80A", "SP", 1.5, "AU15S10803C", 3175, "AU15S10803D", 3615),
        ("100A", "SP", 1.5, "AU15S11003C", 3560, "AU15S11003D", 4045),
        ("125A", "SP", 1.5, "AU15S11253C", 3830, "AU15S11253D", 4370),
        ("80A", "DP", 3, "AU15S20803C", 7035, "AU15S20803D", 8010),
        ("100A", "DP", 3, "AU15S21003C", 7505, "AU15S21003D", 8535),
        ("125A", "DP", 3, "AU15S21253C", 8260, "AU15S21253D", 9390),
        ("80A", "TP", 4.5, "AU15S30803C", 11100, "AU15S30803D", 12640),
        ("100A", "TP", 4.5, "AU15S31003C", 11840, "AU15S31003D", 13455),
        ("125A", "TP", 4.5, "AU15S31253C", 13140, "AU15S31253D", 14930),
        ("80A", "FP", 6, "AU15S40803C", 14500, "AU15S40803D", 16465),
        ("100A", "FP", 6, "AU15S41003C", 15080, "AU15S41003D", 17160),
        ("125A", "FP", 6, "AU15S41253C", 17080, "AU15S41253D", 19415),
    ]

    hr_count = 0
    for rating, pole, modules, c_cat, c_mrp, d_cat, d_mrp in hr_data:
        for curve, cat_no, mrp in [("C", c_cat, c_mrp), ("D", d_cat, d_mrp)]:
            name = f"LK HR MCB {pole} {rating} {curve}-Curve"
            pid = insert_product(cur, name, cat_no, hr_description, None, "HR MCB")
            specs = {
                **hr_common_specs,
                "Rating": rating,
                "Poles": pole,
                "Modules": str(modules),
                "Curve Type": f"{curve}-Curve",
                "Catalog Number": cat_no,
                "MRP (₹)": str(mrp),
            }
            insert_specs(cur, pid, specs)
            hr_count += 1

    print(f"  Inserted {hr_count} HR MCB products (AU15S Series)")

    # ══════════════════════════════════════════════════════════════
    # Solar Combo MCBs (SC Series)
    # ══════════════════════════════════════════════════════════════

    solar_description = (
        "Solar Combo MCB [8536] by Lauritz Knudsen. "
        "AC MCB + DC MCB combo for complete solar rooftop protection. "
        "DC MCB: 10kA as per IEC 60947-2. AC MCB: 10kA as per IS/IEC 60898-1. "
        "AC MCB with C-Curve."
    )

    solar_common_specs = {
        "Brand": "Lauritz Knudsen",
        "Product Type": "Solar Combo MCB",
        "HSN Code": "8536",
        "DC MCB Standard": "IEC 60947-2",
        "DC MCB Breaking Capacity": "10kA",
        "AC MCB Standard": "IS/IEC 60898-1",
        "AC MCB Breaking Capacity": "10kA",
        "AC MCB Curve": "C-Curve",
        "Modules": "2+2",
        "MRP (₹)": "1900",
    }

    solar_data = [
        ("SC221616", "1 AC 16A DP MCB + 1 DC 16A DP 500V MCB"),
        ("SC222020", "1 AC 20A DP MCB + 1 DC 20A DP 500V MCB"),
        ("SC222025", "1 AC 20A DP MCB + 1 DC 25A DP 500V MCB"),
        ("SC222032", "1 AC 20A DP MCB + 1 DC 32A DP 500V MCB"),
        ("SC222516", "1 AC 25A DP MCB + 1 DC 16A DP 500V MCB"),
        ("SC222525", "1 AC 25A DP MCB + 1 DC 25A DP 500V MCB"),
        ("SC223216", "1 AC 32A DP MCB + 1 DC 16A DP 500V MCB"),
        ("SC223225", "1 AC 32A DP MCB + 1 DC 25A DP 500V MCB"),
        ("SC223232", "1 AC 32A DP MCB + 1 DC 32A DP 500V MCB"),
    ]

    solar_count = 0
    for cat_no, desc in solar_data:
        name = f"LK Solar Combo MCB {cat_no}"
        pid = insert_product(cur, name, cat_no, solar_description, None, "Solar Combo MCB")
        specs = {**solar_common_specs, "Catalog Number": cat_no, "Description": desc}
        insert_specs(cur, pid, specs)
        solar_count += 1

    print(f"  Inserted {solar_count} Solar Combo MCB products")

    # ══════════════════════════════════════════════════════════════
    # DC MCBs — BB Series (130V), BJ Series (250V/500V), BK Series (1000V)
    # ══════════════════════════════════════════════════════════════

    dc_ratings = ["0.5A","1A","2A","3A","4A","6A","10A","16A","20A","25A","32A","40A","50A","63A"]

    # 130V DC SP (BB Series)
    bb_sp_cats = ["BB10E5DC","BB1001DC","BB1002DC","BB1003DC","BB1004DC","BB1006DC",
                  "BB1010DC","BB1016DC","BB1020DC","BB1025DC","BB1032DC","BB1040DC","BB1050DC","BB1063DC"]
    bb_sp_mrps = [755,755,755,755,755,655,655,655,655,655,655,860,860,860]

    # 130V DC DP (BB Series)
    bb_dp_cats = ["BB20E5DC","BB2001DC","BB2002DC","BB2003DC","BB2004DC","BB2006DC",
                  "BB2010DC","BB2016DC","BB2020DC","BB2025DC","BB2032DC","BB2040DC","BB2050DC","BB2063DC"]
    bb_dp_mrps = [1615,1615,1615,1615,1615,1420,1420,1420,1420,1420,1420,1920,1920,1920]

    # 250V DC SP (BJ Series)
    bj_sp_cats = ["BJ10E5DC","BJ1001DC","BJ1002DC","BJ1003DC","BJ1004DC","BJ1006DC",
                  "BJ1010DC","BJ1016DC","BJ1020DC","BJ1025DC","BJ1032DC","BJ1040DC","BJ1050DC","BJ1063DC"]
    bj_sp_mrps = [895,895,895,895,895,730,730,730,730,730,730,1010,1010,1010]

    # 250V DC DP (BJ Series)  — actually 500V DC DP
    bj_dp_cats = ["BJ20E5DC","BJ2001DC","BJ2002DC","BJ2003DC","BJ2004DC","BJ2006DC",
                  "BJ2010DC","BJ2016DC","BJ2020DC","BJ2025DC","BJ2032DC","BJ2040DC","BJ2050DC","BJ2063DC"]
    bj_dp_mrps = [1865,1865,1865,1865,1865,1575,1575,1575,1575,1575,1575,2005,2005,2005]

    dc_configs = [
        ("130V DC", "SP", 1, "BB", bb_sp_cats, bb_sp_mrps, "6kA"),
        ("130V DC", "DP", 2, "BB", bb_dp_cats, bb_dp_mrps, "6kA"),
        ("250V DC", "SP", 1, "BJ", bj_sp_cats, bj_sp_mrps, "10kA (≤40A), 6kA (50A-63A)"),
        ("500V DC", "DP", 2, "BJ", bj_dp_cats, bj_dp_mrps, "10kA (≤40A), 6kA (50A-63A)"),
    ]

    dc_count = 0
    for voltage, pole, modules, series, cats, mrps, breaking_cap in dc_configs:
        for i, rating in enumerate(dc_ratings):
            cat_no = cats[i]
            mrp = mrps[i]
            name = f"LK DC MCB {pole} {rating} {voltage}"
            desc = f"DC MCB {series} Series [8536] by Lauritz Knudsen. Rated Voltage: {voltage}. As per IEC 60947-2."
            pid = insert_product(cur, name, cat_no, desc, None, "DC MCB")
            specs = {
                "Brand": "Lauritz Knudsen",
                "Product Type": "DC MCB",
                "HSN Code": "8536",
                "Series": f"{series} Series",
                "Standard": "IEC 60947-2",
                "Rated Voltage": voltage,
                "Rating": rating,
                "Poles": pole,
                "Modules": str(modules),
                "Breaking Capacity": breaking_cap,
                "Catalog Number": cat_no,
                "MRP (₹)": str(mrp),
            }
            insert_specs(cur, pid, specs)
            dc_count += 1

    # 1000V DC (BK Series)
    bk_data = [
        ("6A", "BK2006DC", 2400), ("10A", "BK2010DC", 2400), ("16A", "BK2016DC", 2400),
        ("20A", "BK2020DC", 2400), ("25A", "BK2025DC", 2400), ("32A", "BK2032DC", 2400),
        ("40A", "BK2040DC", 2915), ("50A", "BK2050DC", 2915), ("63A", "BK2063DC", 2915),
    ]

    for rating, cat_no, mrp in bk_data:
        name = f"LK DC MCB DP {rating} 1000V"
        desc = "1000V DC MCB BK Series [8536] by Lauritz Knudsen. As per IEC 60947-2."
        pid = insert_product(cur, name, cat_no, desc, None, "DC MCB")
        specs = {
            "Brand": "Lauritz Knudsen",
            "Product Type": "DC MCB",
            "HSN Code": "8536",
            "Series": "BK Series",
            "Standard": "IEC 60947-2",
            "Rated Voltage": "1000V DC",
            "Rating": rating,
            "Poles": "DP",
            "Modules": "4",
            "Breaking Capacity": "6kA",
            "Catalog Number": cat_no,
            "MRP (₹)": str(mrp),
        }
        insert_specs(cur, pid, specs)
        dc_count += 1

    print(f"  Inserted {dc_count} DC MCB products")

    conn.commit()
    cur.close()
    conn.close()

    # Print summary
    conn = get_db()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM products")
    total_products = cur.fetchone()[0]
    cur.execute("SELECT count(*) FROM product_specs")
    total_specs = cur.fetchone()[0]
    cur.execute("SELECT category, count(*) FROM products GROUP BY category ORDER BY category")
    categories = cur.fetchall()
    cur.close()
    conn.close()

    print(f"\n{'='*50}")
    print(f"Total: {total_products} products, {total_specs} spec entries")
    for cat, cnt in categories:
        print(f"  {cat}: {cnt}")


if __name__ == "__main__":
    seed()
