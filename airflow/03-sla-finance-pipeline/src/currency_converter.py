"""
src/currency_converter.py — Currency Normalization Service
===========================================================
Real-World Problem:
    Global enterprise reporting requires harmonizing transactions across local currencies
    (e.g. Brazilian Real BRL) into corporate reporting currency (USD) using the official
    daily closing FX exchange rate.
"""

from __future__ import annotations

from typing import Any, Dict, List


def convert(
    records: List[Dict[str, Any]],
    from_currency: str = "BRL",
    to_currency: str = "USD",
    fx_rate: float = 0.20,  # 1 BRL = 0.20 USD
) -> List[Dict[str, Any]]:
    """
    Applies FX conversion rates and calculates total_price_usd.
    """
    converted_records: List[Dict[str, Any]] = []
    for r in records:
        rec = dict(r)
        price_brl = float(rec.get("total_price_brl", 0.0))
        rec["fx_rate"] = fx_rate
        rec["from_currency"] = from_currency
        rec["to_currency"] = to_currency
        rec["total_price_usd"] = round(price_brl * fx_rate, 2)
        converted_records.append(rec)
    return converted_records
