"""
src/staging_loader.py — Staging Database Loader with UPSERT Idempotency
========================================================================
Real-World Problem:
    When an ETL task retries after transient failure or when an engineer manually
    re-runs a DAG run, naive INSERT statements duplicate records in the staging table.
    
    This module implements an idempotent staging loader backed by SQLite using
    INSERT OR REPLACE (UPSERT). Re-running the pipeline 10 times with the same
    batch will produce the exact same table row count with zero duplicate records.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd


DEFAULT_STAGING_DB = Path("data/processed/airflow/staging/vendor_orders_staging.db")


class StagingLoader:
    """
    Idempotent loader for clean vendor orders into staging SQLite database.
    Uses INSERT OR REPLACE to ensure backfill/retry safety.
    """

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else DEFAULT_STAGING_DB
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(str(self.db_path))

    def _init_db(self) -> None:
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS staging_vendor_orders (
                    order_id TEXT PRIMARY KEY,
                    customer_id TEXT NOT NULL,
                    order_status TEXT NOT NULL,
                    price REAL NOT NULL,
                    order_purchase_timestamp TEXT NOT NULL,
                    dag_run_id TEXT NOT NULL,
                    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()

    def load(self, records: List[Dict[str, Any]], dag_run_id: str) -> Dict[str, Any]:
        """
        Loads records using UPSERT on order_id.
        Returns a summary dict with loaded count and table stats.
        """
        if not records:
            return {"loaded": 0, "total_in_table": self.count(), "status": "NO_RECORDS"}

        with self._connect() as conn:
            # Check how many existing keys match
            placeholders = ",".join("?" for _ in records)
            order_ids = [r["order_id"] for r in records]
            existing_count = conn.execute(
                f"SELECT COUNT(*) FROM staging_vendor_orders WHERE order_id IN ({placeholders})",
                order_ids,
            ).fetchone()[0]

            conn.executemany(
                """
                INSERT OR REPLACE INTO staging_vendor_orders
                (order_id, customer_id, order_status, price, order_purchase_timestamp, dag_run_id, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    (
                        r["order_id"],
                        r["customer_id"],
                        r["order_status"],
                        float(r["price"]),
                        str(r["order_purchase_timestamp"]),
                        dag_run_id,
                    )
                    for r in records
                ],
            )
            conn.commit()

        total = self.count()
        return {
            "loaded": len(records),
            "updated_existing": existing_count,
            "new_inserted": len(records) - existing_count,
            "total_in_table": total,
            "status": "SUCCESS",
        }

    def get_all(self) -> pd.DataFrame:
        with self._connect() as conn:
            return pd.read_sql_query("SELECT * FROM staging_vendor_orders ORDER BY loaded_at DESC", conn)

    def count(self) -> int:
        with self._connect() as conn:
            return conn.execute("SELECT COUNT(*) FROM staging_vendor_orders").fetchone()[0]

    def clear(self) -> None:
        with self._connect() as conn:
            conn.execute("DELETE FROM staging_vendor_orders")
            conn.commit()
