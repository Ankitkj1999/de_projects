"""
src/extractor.py — Deterministic Synthetic Sales Extractor
===========================================================
Real-World Problem:
    When testing idempotency in a data pipeline, you need to prove that
    re-running a DAG for the same logical date produces *exactly the same
    source records* — not random noise that makes row-count comparisons
    meaningless.

    In production, the source system (e.g. Olist's order API) is the
    authoritative record and is itself immutable for a closed day. This
    module simulates that property: seeding Python's random number generator
    with the logical date string guarantees that every call to ``extract()``
    for the same date produces byte-identical records. Different dates produce
    different records, just as different days would have different orders.

    This determinism is the cornerstone of safe backfill testing: you can
    re-run, diff, and assert without introducing flakiness.
"""

import random
import uuid
import hashlib
from datetime import datetime, timezone
from typing import List, Dict, Any

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
RECORDS_PER_PARTITION = 200

_PRODUCT_IDS = [
    "prod_electronics_01", "prod_fashion_02", "prod_home_03", "prod_sports_04",
    "prod_books_05", "prod_beauty_06", "prod_toys_07", "prod_garden_08",
    "prod_tools_09", "prod_food_10",
]
_STATUSES = ["delivered", "shipped", "processing", "cancelled", "invoiced"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def extract(logical_date: str) -> Dict[str, Any]:
    """
    Generate a deterministic set of Olist-style sales records for *logical_date*.

    The same date **always** produces the same 200 records; different dates
    produce different records. This mirrors the behaviour of a real order API
    that returns a closed day's data immutably.

    Parameters
    ----------
    logical_date : str
        ISO date string, e.g. ``"2024-01-15"``.

    Returns
    -------
    dict
        ``{"records": list[dict], "logical_date": str, "record_count": int}``
    """
    # Seed deterministically from the date string so re-runs are identical.
    seed_value = int(hashlib.md5(logical_date.encode()).hexdigest(), 16) % (2**32)
    rng = random.Random(seed_value)

    records: List[Dict[str, Any]] = []
    for _ in range(RECORDS_PER_PARTITION):
        # Build a stable order_id that embeds the date so partitions don't
        # collide on order_id across different dates.
        order_uuid = uuid.UUID(int=rng.getrandbits(128))
        order_id = f"{logical_date}_{order_uuid.hex[:12]}"
        customer_id = f"cust_{uuid.UUID(int=rng.getrandbits(128)).hex[:8]}"
        product_id = rng.choice(_PRODUCT_IDS)
        price = round(rng.uniform(5.0, 2500.0), 2)
        status = rng.choice(_STATUSES)

        records.append({
            "order_id": order_id,
            "customer_id": customer_id,
            "product_id": product_id,
            "price": price,
            "order_status": status,
            "partition_date": logical_date,
        })

    return {
        "records": records,
        "logical_date": logical_date,
        "record_count": len(records),
        "row_count": len(records),
    }
