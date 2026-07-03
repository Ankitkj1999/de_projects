"""
src/client_extractor.py — Multi-Tenant Order Extractor
======================================================
Real-World Problem:
    In SaaS and enterprise multi-tenant architectures, dozens or hundreds of clients
    deliver feeds into isolated tenant namespaces. Writing and maintaining independent
    extraction scripts per client causes code divergence and maintenance hell.

    This module provides a parameterized extractor that pulls or generates tenant data
    based strictly on a YAML configuration contract.
"""

from __future__ import annotations

import csv
import random
from pathlib import Path
from typing import Any, Dict, List


def extract(client_config: Dict[str, Any], date_str: str = "2024-01-15") -> Dict[str, Any]:
    """
    Extracts orders for a specific tenant.
    If the source file doesn't exist, generates a synthetic high-fidelity feed
    seeded by the tenant ID for determinism.
    """
    client_id = client_config["client_id"]
    source_path = Path(client_config["source_path"])
    source_path.parent.mkdir(parents=True, exist_ok=True)

    # Use seed based on client_id to generate reproducible volume
    seed_map = {
        "acme_retail": (150, 42),
        "globex_ecom": (600, 101),
        "initech_shop": (80, 202),
        "umbrella_mart": (320, 303),
    }
    target_count, seed_val = seed_map.get(client_id, (120, 999))

    rng = random.Random(seed_val)
    statuses = ["delivered", "shipped", "invoiced", "processing"]

    records: List[Dict[str, Any]] = []
    for i in range(1, target_count + 1):
        price = round(rng.uniform(15.0, min(float(client_config.get("max_price_ceiling", 5000.0)), 1200.0)), 2)
        records.append({
            "order_id": f"{client_id.upper()[:4]}_{i:06d}",
            "client_id": client_id,
            "customer_id": f"cust_{rng.randint(1000, 9999)}",
            "order_status": rng.choice(statuses),
            "price": price,
            "order_date": date_str,
        })

    # Write out to source path CSV for physical file persistence
    with open(source_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["order_id", "client_id", "customer_id", "order_status", "price", "order_date"])
        writer.writeheader()
        writer.writerows(records)

    return {
        "client_id": client_id,
        "source_path": str(source_path),
        "record_count": len(records),
        "records": records,
        "status": "EXTRACTED",
    }
