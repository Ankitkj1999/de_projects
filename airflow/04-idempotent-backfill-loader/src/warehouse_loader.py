"""
src/warehouse_loader.py — Idempotent Warehouse Loader with UPSERT
==================================================================
Real-World Problem:
    When a backfill or pipeline retry executes, plain `INSERT` statements append
    duplicate records, corrupting analytical aggregates.

    This module implements an idempotent load strategy using SQLite UPSERT
    (`INSERT OR REPLACE`) keyed by `(order_id, partition_date)`.
    Running the loader 1 time or 100 times for the same date produces the exact same
    row count.
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = str(_PROJECT_ROOT / "data" / "processed" / "airflow" / "warehouse" / "sales_warehouse.db")


class WarehouseLoader:
    """
    Idempotent loader for sales partitions into warehouse SQLite database.
    """

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._ensure_table()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_table(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sales_partitions (
                    order_id TEXT NOT NULL,
                    customer_id TEXT NOT NULL,
                    product_id TEXT NOT NULL,
                    price REAL NOT NULL,
                    order_status TEXT NOT NULL,
                    partition_date TEXT NOT NULL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    run_id TEXT NOT NULL,
                    PRIMARY KEY (order_id, partition_date)
                )
            """)
            conn.commit()

    def upsert(self, logical_date: str, records: List[Dict[str, Any]], run_id: str = "manual") -> Dict[str, Any]:
        """
        Loads records using INSERT OR REPLACE (UPSERT).
        """
        if not records:
            return {"upserted": 0, "partition_date": logical_date, "run_id": run_id}

        self._ensure_table()
        now_str = datetime.now(timezone.utc).isoformat()

        rows = [
            (
                r["order_id"],
                r["customer_id"],
                r["product_id"],
                float(r["price"]),
                r["order_status"],
                logical_date,
                now_str,
                run_id,
            )
            for r in records
        ]

        with self._connect() as conn:
            conn.executemany(
                """
                INSERT OR REPLACE INTO sales_partitions
                (order_id, customer_id, product_id, price, order_status, partition_date, loaded_at, run_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            conn.commit()

        count = self.get_partition_count(logical_date)
        return {
            "upserted": len(records),
            "partition_date": logical_date,
            "partition_row_count": count,
            "run_id": run_id,
            "status": "COMMITTED",
        }

    def get_partition_count(self, logical_date: str) -> int:
        with self._connect() as conn:
            cur = conn.execute("SELECT COUNT(*) FROM sales_partitions WHERE partition_date = ?", (logical_date,))
            return cur.fetchone()[0]

    def clear(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM sales_partitions")
            conn.commit()
