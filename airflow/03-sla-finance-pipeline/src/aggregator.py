"""
src/aggregator.py — Financial Executive Aggregator
===================================================
Real-World Problem:
    Aggregating financial metrics for executive leadership requires multi-dimensional
    breakdowns: gross revenue, channel attribution, average order value (AOV),
    and product volume metrics.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List


def aggregate(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Computes key financial KPIs across the batch.
    """
    if not records:
        return {
            "total_transactions": 0,
            "total_revenue_brl": 0.0,
            "total_revenue_usd": 0.0,
            "average_order_value_usd": 0.0,
            "revenue_by_channel": {},
            "top_products": [],
        }

    total_brl = 0.0
    total_usd = 0.0
    channel_revenue: Dict[str, float] = defaultdict(float)
    product_revenue: Dict[str, Dict[str, Any]] = defaultdict(lambda: {"revenue_usd": 0.0, "units": 0, "name": ""})

    for r in records:
        brl = float(r.get("total_price_brl", 0.0))
        usd = float(r.get("total_price_usd", 0.0))
        qty = int(r.get("quantity", 1))
        channel = r.get("sales_channel", "unknown")
        pid = r.get("product_id", "unknown")
        pname = r.get("product_name", pid)

        total_brl += brl
        total_usd += usd
        channel_revenue[channel] += usd

        product_revenue[pid]["revenue_usd"] += usd
        product_revenue[pid]["units"] += qty
        product_revenue[pid]["name"] = pname

    aov_usd = round(total_usd / len(records), 2)

    # Sort top products by revenue
    sorted_products = sorted(
        [
            {"product_id": pid, "name": data["name"], "revenue_usd": round(data["revenue_usd"], 2), "units": data["units"]}
            for pid, data in product_revenue.items()
        ],
        key=lambda x: x["revenue_usd"],
        reverse=True,
    )

    return {
        "total_transactions": len(records),
        "total_revenue_brl": round(total_brl, 2),
        "total_revenue_usd": round(total_usd, 2),
        "average_order_value_usd": aov_usd,
        "revenue_by_channel": {k: round(v, 2) for k, v in channel_revenue.items()},
        "top_products": sorted_products[:5],
    }
