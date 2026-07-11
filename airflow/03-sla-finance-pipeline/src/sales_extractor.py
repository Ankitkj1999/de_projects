"""
src/sales_extractor.py — Daily Sales Extractor
==============================================
Real-World Problem:
    Financial reporting requires transactional consistency for the reporting date.
    Extracting raw sales must handle heavy queries gracefully and respect execution timeouts.
"""

from __future__ import annotations

import random
import time
from typing import Any, Dict, List


def extract(logical_date: str = "2024-01-15", simulate_slow: bool = False) -> Dict[str, Any]:
    """
    Extracts daily sales transactions for the target date.
    If simulate_slow=True, simulates a database under heavy lock load.
    """
    if simulate_slow:
        time.sleep(1.0)

    # Deterministic generation using logical date as seed
    seed = sum(ord(c) for c in logical_date)
    rng = random.Random(seed)

    channels = ["marketplace", "direct_web", "b2b_wholesale", "mobile_app"]
    products = [
        ("prod_elec_01", "Smartphone Pro", 850.0),
        ("prod_elec_02", "Noise-Cancelling Headphones", 320.0),
        ("prod_home_03", "Ergonomic Office Chair", 450.0),
        ("prod_home_04", "Robotic Vacuum Cleaner", 620.0),
        ("prod_fit_05", "Fitness Smartwatch", 199.0),
        ("prod_book_06", "Data Architecture Manual", 65.0),
    ]

    records: List[Dict[str, Any]] = []
    for i in range(1, 501):
        pid, pname, base_price = rng.choice(products)
        qty = rng.randint(1, 4)
        price_brl = round(base_price * qty * rng.uniform(0.90, 1.10), 2)
        channel = rng.choice(channels)

        records.append({
            "order_id": f"FIN_ORD_{logical_date.replace('-', '')}_{i:05d}",
            "product_id": pid,
            "product_name": pname,
            "quantity": qty,
            "unit_price_brl": round(price_brl / qty, 2),
            "total_price_brl": price_brl,
            "sales_channel": channel,
            "transaction_date": logical_date,
        })

    return {
        "logical_date": logical_date,
        "row_count": len(records),
        "records": records,
        "status": "EXTRACTED",
    }
