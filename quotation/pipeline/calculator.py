"""Pricing math for the quotation builder.

All amounts are in Indian Rupees. Rounding follows half-up at 2 decimals.
"""

from __future__ import annotations

from decimal import Decimal, ROUND_HALF_UP


def _q(x) -> Decimal:
    return Decimal(str(x or 0))


def _round(x: Decimal) -> Decimal:
    return x.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def line_total(unit_price, quantity, discount_pct) -> float:
    up = _q(unit_price)
    qty = _q(quantity)
    disc = _q(discount_pct)
    gross = up * qty
    net = gross * (Decimal("100") - disc) / Decimal("100")
    return float(_round(net))


def compute_totals(items: list[dict], gst_rate=18) -> dict:
    """Return subtotal, gst_amount, grand_total, and a copy of items with line_total set.

    `items` rows need: unit_price, quantity, discount_pct.
    """
    rate = _q(gst_rate)
    subtotal = Decimal("0")
    out_items = []
    for it in items:
        lt = Decimal(str(line_total(it.get("unit_price", 0),
                                    it.get("quantity", 0),
                                    it.get("discount_pct", 0))))
        out_items.append({**it, "line_total": float(_round(lt))})
        subtotal += lt

    subtotal = _round(subtotal)
    gst_amount = _round(subtotal * rate / Decimal("100"))
    grand_total = _round(subtotal + gst_amount)
    cgst = _round(gst_amount / Decimal("2"))
    sgst = _round(gst_amount - cgst)

    return {
        "items": out_items,
        "subtotal": float(subtotal),
        "gst_rate": float(rate),
        "gst_amount": float(gst_amount),
        "cgst": float(cgst),
        "sgst": float(sgst),
        "grand_total": float(grand_total),
    }


def format_inr(amount) -> str:
    """Format a number as Indian Rupee string with commas (1,23,45,678.90)."""
    try:
        n = float(amount)
    except (TypeError, ValueError):
        return "₹0.00"
    sign = "-" if n < 0 else ""
    n = abs(n)
    whole, frac = f"{n:.2f}".split(".")
    if len(whole) <= 3:
        formatted = whole
    else:
        head, tail = whole[:-3], whole[-3:]
        groups = []
        while len(head) > 2:
            groups.insert(0, head[-2:])
            head = head[:-2]
        if head:
            groups.insert(0, head)
        formatted = ",".join(groups) + "," + tail
    return f"{sign}₹{formatted}.{frac}"
