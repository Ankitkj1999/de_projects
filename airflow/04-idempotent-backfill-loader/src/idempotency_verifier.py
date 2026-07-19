"""
src/idempotency_verifier.py — Warehouse Partition Idempotency Verifier
======================================================================
Real-World Problem:
    In production pipelines, automated tests must verify that reloading existing
    partitions does not inflate row counts or introduce duplicates.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = str(_PROJECT_ROOT / "data" / "processed" / "airflow" / "warehouse" / "sales_warehouse.db")


def verify(logical_date: str, expected_count: int = 200, db_path: str = DEFAULT_DB_PATH) -> Dict[str, Any]:
    """
    Checks warehouse partition count and uniqueness.
    """
    with sqlite3.connect(db_path) as conn:
        cur = conn.execute(
            "SELECT COUNT(*), COUNT(DISTINCT order_id) FROM sales_partitions WHERE partition_date = ?",
            (logical_date,),
        )
        total_rows, distinct_orders = cur.fetchone()

    has_duplicates = total_rows != distinct_orders
    matches_expected = total_rows == expected_count
    is_safe = (not has_duplicates) and matches_expected

    return {
        "logical_date": logical_date,
        "actual_count": total_rows,
        "distinct_orders": distinct_orders,
        "expected_count": expected_count,
        "duplicate_detected": has_duplicates,
        "safe": is_safe,
        "status": "PASSED" if is_safe else "FAILED",
    }
