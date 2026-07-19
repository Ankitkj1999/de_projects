"""
src/partition_checker.py — Partition Existence Checker
=======================================================
Real-World Problem:
    In production data pipelines, re-running a DAG for a past date (backfill)
    or retrying a failed run is extremely common. Without idempotency awareness,
    the first question an operator must answer before re-running is: "Has this
    partition already been loaded?" Checking this manually is error-prone and
    doesn't scale across hundreds of daily partitions.

    This module provides a lightweight partition-existence check backed by
    SQLite. Before any data is extracted or loaded, the DAG queries whether
    today's partition (keyed by logical_date) already exists in the warehouse
    and how many rows it currently holds. This metadata drives downstream
    UPSERT behaviour and audit logging, making every run safe to retry.
"""

import sqlite3
import os
from pathlib import Path
from typing import Optional

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False


# ---------------------------------------------------------------------------
# Default DB path — relative to the project root so both demo.py and tests
# can resolve it without depending on the working directory.
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = str(_PROJECT_ROOT / "data" / "processed" / "airflow" / "warehouse" / "sales_warehouse.db")


class PartitionChecker:
    """Checks whether a date partition already exists in the sales warehouse."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._ensure_table()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.db_path)

    def _ensure_table(self) -> None:
        """Create the sales table if it does not exist yet."""
        with self._connect() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sales_partitions (
                    order_id       TEXT NOT NULL,
                    customer_id    TEXT NOT NULL,
                    product_id     TEXT NOT NULL,
                    price          REAL NOT NULL,
                    order_status   TEXT NOT NULL,
                    partition_date TEXT NOT NULL,
                    loaded_at      TEXT NOT NULL,
                    run_id         TEXT NOT NULL,
                    PRIMARY KEY (order_id, partition_date)
                )
            """)
            conn.commit()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def check(self, logical_date: str) -> dict:
        """
        Check whether *logical_date* partition already exists in the warehouse.

        Parameters
        ----------
        logical_date : str
            ISO date string, e.g. ``"2024-01-15"``.

        Returns
        -------
        dict
            ``{"partition_exists": bool, "existing_row_count": int, "partition_date": str}``
        """
        with self._connect() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) FROM sales_partitions WHERE partition_date = ?",
                (logical_date,),
            )
            row_count: int = cursor.fetchone()[0]

        exists = row_count > 0
        return {
            "partition_exists": exists,
            "existing_row_count": row_count,
            "partition_date": logical_date,
        }

    def get_partition_history(self):
        """
        Return a summary of all loaded partitions.

        Returns
        -------
        pandas.DataFrame or list[dict]
            One row per partition date with columns:
            partition_date, row_count, first_loaded_at, last_loaded_at.
        """
        with self._connect() as conn:
            cursor = conn.execute("""
                SELECT
                    partition_date,
                    COUNT(*)        AS row_count,
                    MIN(loaded_at)  AS first_loaded_at,
                    MAX(loaded_at)  AS last_loaded_at
                FROM sales_partitions
                GROUP BY partition_date
                ORDER BY partition_date
            """)
            rows = cursor.fetchall()
            columns = ["partition_date", "row_count", "first_loaded_at", "last_loaded_at"]

        if HAS_PANDAS:
            return pd.DataFrame(rows, columns=columns)
        return [dict(zip(columns, r)) for r in rows]
