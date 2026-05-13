"""Accessory extraction pipeline using vLLM (Qwen 3.5-27B).

Extracts accessories from Mitsubishi Electric LVS catalogs and maps them
to parent products. Designed for MCCB/ACB accessory tables.
"""

from __future__ import annotations

import json
import os
import re

import requests

from .db import save_accessory, save_accessory_specs

VLLM_URL = os.environ.get("VLLM_URL", "http://localhost:8201/v1/chat/completions")
VLLM_MODEL = os.environ.get("VLLM_MODEL", "Qwen/Qwen3.5-27B-FP8")

ACCESSORY_PROMPT = """You are an electrical product accessory data extractor. Given a table from a Mitsubishi Electric LVS catalog PDF, extract every accessory into structured JSON.

For each accessory, extract:
- "accessory_name": clear descriptive name (e.g. "Alarm switch (AL), lead wire type", "Door Mounted Extended Rotary Handle")
- "accessory_model": the reference/part code (e.g. "AL-05SV*", "V-05SV", "SHT**-05SV_*", "AE630SWMD3PLSIG")
- "category": "Internal" for accessories mounted inside the breaker, "External" for accessories mounted outside/on panel
- "sub_category": one of: AL (Alarm Switch), AX (Auxiliary Switch), ALAX (Combined AL+AX), SHT (Shunt Trip), UVT (Undervoltage Trip), Handle (Operating Handle), Interlock (Mechanical Interlock), Spreader, Terminal, Counter, Display, VT (Voltage Transformer), Communication, Motor, Lock, Barrier, DIN (DIN Rail Adapter), Other
- "applies_to": the applicable MCCB/ACB model range this accessory works with (e.g. "NF63-250AF", "NF400-800AF", "AE630-SW to AE4000-SWA", "All models"). Extract this from "Application for Model" column or page context
- "mrp": price in INR (from LP column). Map any "LP", "LP (INR)", "UNIT LP INR" column to this
- "specs": dictionary of ALL other specifications as key-value pairs (rated voltage, mounting type, contact configuration, etc.)

Rules:
- EVERY ROW in the table is a SEPARATE accessory. Extract each individually.
- If a "Application for Model" or "Applicable ACB/MCCB" column exists, extract it as "applies_to"
- If the page context mentions which products the accessories apply to (e.g. "MCCB Internal Accessories for NF63-250AF"), use that as "applies_to" for all rows
- Price columns: "LP", "LP (INR)", "UNIT LP INR" → map to "mrp"
- Voltage codes in model names: A048=24-48V AC, A240=100-240V AC, A550=380-550V AC, D012=12V DC, D024=24V DC, D048=36-48V DC, D125=100-125V DC, D250=200-250V DC
- Installation codes: L=Left Side, R=Right Side, LS=Left Side with SLT, RS=Right Side with SLT
- If the table has accessories for ACBs (Air Circuit Breakers), use sub_category accordingly (SHT, CC for Closing Coil, AX, Counter, UVT, Display, VT, Communication, Motor, Interlock, Lock)
- Return only a JSON array
"""


def _parse_json_from_llm(content: str) -> list:
    content = content.strip()
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    match = re.search(r'```(?:json)?\s*\n?(.*?)```', content, re.DOTALL)
    if match:
        try:
            return json.loads(match.group(1).strip())
        except json.JSONDecodeError:
            pass

    start = content.find('[')
    end = content.rfind(']')
    if start != -1 and end != -1 and end > start:
        try:
            return json.loads(content[start:end + 1])
        except json.JSONDecodeError:
            pass

    return []


def _format_table(table: dict, tidx: int) -> str:
    headers = table.get("headers", [])
    rows = table.get("rows", [])
    ctx = table.get("page_context", "")

    text = f"Table {tidx + 1} (page {table.get('page', '?')}):\n"
    if ctx:
        text += f"Page context: {ctx}\n"
    text += "Columns: " + " | ".join(str(h) for h in headers) + "\n---\n"
    for row in rows[:50]:
        cells = [str(row[i]) if i < len(row) else "" for i in range(len(headers))]
        text += " | ".join(cells) + "\n"
    if len(rows) > 50:
        text += f"... ({len(rows) - 50} more rows)\n"
    return text


def _call_vllm(prompt: str) -> list:
    full_prompt = f"""{ACCESSORY_PROMPT}

{prompt}

Extract all accessories as a JSON array:"""

    try:
        resp = requests.post(
            VLLM_URL,
            json={
                "model": VLLM_MODEL,
                "messages": [
                    {"role": "system", "content": "You extract structured accessory data from electrical equipment catalogs. Output valid JSON only."},
                    {"role": "user", "content": full_prompt},
                ],
                "max_tokens": 16384,
                "temperature": 0.05,
                "chat_template_kwargs": {"enable_thinking": False},
            },
            timeout=None,
        )
        if resp.status_code != 200:
            print(f"[Extract] vLLM returned {resp.status_code}")
            return []
        content = resp.json()["choices"][0]["message"]["content"]
        return _parse_json_from_llm(content)
    except Exception as e:
        print(f"[Extract] vLLM error: {e}")
        return []


def extract_accessories(tables: list[dict], filename: str) -> list[dict]:
    """Extract accessories from parsed tables using vLLM.

    Returns list of accessory dicts with keys:
    accessory_name, accessory_model, category, sub_category, applies_to, mrp, specs
    """
    all_accessories = []
    valid_tables = [t for t in tables if t.get("headers") and t.get("rows")]

    print(f"  [Extract] {len(valid_tables)} valid tables from {filename}")

    # Send each table individually for best accuracy
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def _process_table(tidx, table):
        table_text = f"Source document: {filename}\n\n"
        table_text += _format_table(table, tidx)
        items = _call_vllm(table_text)

        accessories = []
        for item in items:
            model = item.get("accessory_model") or item.get("model") or item.get("reference")
            if not model:
                continue
            accessories.append({
                "accessory_name": item.get("accessory_name") or item.get("name", ""),
                "accessory_model": str(model).strip(),
                "category": item.get("category", "Other"),
                "sub_category": item.get("sub_category", "Other"),
                "applies_to": item.get("applies_to", ""),
                "mrp": item.get("mrp"),
                "brand": "Mitsubishi Electric",
                "specs": item.get("specs", {}),
            })
        return tidx, accessories

    max_workers = min(3, len(valid_tables))
    completed = 0

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_process_table, i, t): i for i, t in enumerate(valid_tables)}
        for future in as_completed(futures):
            tidx, accessories = future.result()
            all_accessories.extend(accessories)
            completed += 1
            if completed % 5 == 0 or completed == len(valid_tables):
                print(f"  [Extract] {completed}/{len(valid_tables)} tables done ({len(all_accessories)} accessories)")

    print(f"  [Extract] Total: {len(all_accessories)} accessories from {filename}")
    return all_accessories


def save_accessories(accessories: list[dict]) -> tuple[int, int]:
    """Save extracted accessories to DB. Returns (new_count, existing_count)."""
    new_count = 0
    existing_count = 0

    for acc in accessories:
        model = acc.get("accessory_model", "").strip()
        if not model:
            existing_count += 1
            continue

        mrp = acc.get("mrp")
        if isinstance(mrp, dict):
            mrp = mrp.get("value", str(mrp))
        mrp = str(mrp).strip() if mrp else None

        acc_data = {
            "accessory_name": acc.get("accessory_name", ""),
            "accessory_model": model,
            "category": acc.get("category"),
            "sub_category": acc.get("sub_category"),
            "brand": acc.get("brand", "Mitsubishi Electric"),
            "mrp": mrp,
            "description": acc.get("description"),
            "catalogue_name": acc.get("catalogue_name"),
        }

        acc_id = save_accessory(acc_data)
        if acc_id:
            # Save specs
            specs = acc.get("specs", {})
            if isinstance(specs, dict):
                clean_specs = {}
                for k, v in specs.items():
                    if isinstance(v, dict):
                        v = v.get("value", json.dumps(v))
                    elif isinstance(v, list):
                        v = ", ".join(str(x) for x in v)
                    clean_specs[k] = str(v).strip() if v else ""
                save_accessory_specs(acc_id, clean_specs)

            # Store applies_to as a spec too
            applies_to = acc.get("applies_to", "")
            if applies_to:
                save_accessory_specs(acc_id, {"applies_to": applies_to})

            new_count += 1
        else:
            existing_count += 1

    print(f"  [Save] {new_count} new, {existing_count} existing")
    return new_count, existing_count
