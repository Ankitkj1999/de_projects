"""
src/dead_letter_queue.py — Dead Letter Queue with SQLite Backend
================================================================
Real-World Problem:
    Not every ETL failure should halt the entire pipeline. Some records are
    genuinely malformed (null IDs, negative prices from upstream bugs) and
    should be quarantined rather than blocking 10,000 valid records from
    landing in the warehouse.

    The Dead Letter Queue (DLQ) pattern — borrowed from message-queue systems
    like Apache Kafka and AWS SQS — provides a holding area for bad records with
    full audit metadata: WHY the record failed, WHEN it was quarantined, and
    WHICH DAG run produced the failure. This enables:

      • Operations: "how many records failed this week?" → one SQL query
      • Engineering: replay failed records after fixing upstream data quality
      • Compliance: full audit trail with timestamps and failure reasons

This module provides a SQLite-backed DLQ store. In production this would be
backed by a Delta Lake quarantine table or an S3 bucket with structured JSON.
"""

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd


class DeadLetterQueue:
    """
    SQLite-backed Dead Letter Queue for quarantining invalid ETL records.

    Each record stored in the DLQ retains:
      - The original raw data (JSON-serialised)
      - A human-readable failure reason
      - The DAG run ID that produced the failure
      - A timestamp and optional retry counter

    Usage
    -----
    >>> dlq = DeadLetterQueue()
    >>> dlq.push([{"order_id": None, ...}], "null_order_id", "manual__2024-01-15")
    >>> stats = dlq.get_stats()
    """

    DEFAULT_DB_PATH = Path(
        "data/processed/airflow/dlq/vendor_orders_dlq.db"
    )

    def __init__(self, db_path: str | Path | None = None):
        self.db_path = Path(db_path) if db_path else self.DEFAULT_DB_PATH
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        return conn

    def _init_db(self) -> None:
        """Create the DLQ table if it does not exist."""
        ddl = """
        CREATE TABLE IF NOT EXISTS dead_letter_queue (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id       TEXT,
            raw_data       TEXT NOT NULL,
            failure_reason TEXT NOT NULL,
            failed_at      TEXT NOT NULL,
            dag_run_id     TEXT NOT NULL,
            retry_count    INTEGER DEFAULT 0,
            resolved       INTEGER DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_dlq_order_id    ON dead_letter_queue(order_id);
        CREATE INDEX IF NOT EXISTS idx_dlq_dag_run_id  ON dead_letter_queue(dag_run_id);
        CREATE INDEX IF NOT EXISTS idx_dlq_failed_at   ON dead_letter_queue(failed_at);
        """
        with self._connect() as conn:
            conn.executescript(ddl)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push(
        self,
        records: list[Any],
        failure_reason: str = "VALIDATION_FAILURE",
        dag_run_id: str = "manual_run",
        retry_count: int = 0,
    ) -> int:
        """
        Quarantine a batch of invalid records.
        Supports both list[dict] and list[tuple[dict, reason_str]].
        """
        if not records:
            return 0

        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        rows = []
        for item in records:
            if isinstance(item, tuple) and len(item) == 2:
                rec, reason = item
            else:
                rec = item
                reason = failure_reason

            rows.append((
                rec.get("order_id") if isinstance(rec, dict) else None,
                json.dumps(rec, default=str),
                reason,
                now,
                dag_run_id,
                retry_count,
            ))

        sql = """
            INSERT INTO dead_letter_queue
                (order_id, raw_data, failure_reason, failed_at, dag_run_id, retry_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        with self._connect() as conn:
            conn.executemany(sql, rows)

        return len(rows)

    def get_all(self) -> pd.DataFrame:
        """Return all DLQ records as a DataFrame."""
        with self._connect() as conn:
            df = pd.read_sql_query(
                "SELECT * FROM dead_letter_queue ORDER BY failed_at DESC",
                conn,
            )
        return df

    def get_stats(self) -> dict:
        """
        Return aggregate statistics about the DLQ.

        Returns
        -------
        dict with keys:
            total_quarantined : int
            by_reason         : dict[reason → count]
            by_dag_run        : dict[dag_run_id → count]
            oldest_failure    : str | None
            newest_failure    : str | None
            unresolved        : int
        """
        sql_total  = "SELECT COUNT(*) FROM dead_letter_queue"
        sql_reason = "SELECT failure_reason, COUNT(*) cnt FROM dead_letter_queue GROUP BY failure_reason"
        sql_run    = "SELECT dag_run_id, COUNT(*) cnt FROM dead_letter_queue GROUP BY dag_run_id"
        sql_dates  = "SELECT MIN(failed_at) oldest, MAX(failed_at) newest FROM dead_letter_queue"
        sql_unresolved = "SELECT COUNT(*) FROM dead_letter_queue WHERE resolved = 0"

        with self._connect() as conn:
            total      = conn.execute(sql_total).fetchone()[0]
            by_reason  = {r[0]: r[1] for r in conn.execute(sql_reason).fetchall()}
            by_run     = {r[0]: r[1] for r in conn.execute(sql_run).fetchall()}
            dates_row  = conn.execute(sql_dates).fetchone()
            unresolved = conn.execute(sql_unresolved).fetchone()[0]

        return {
            "total_quarantined": total,
            "by_reason": by_reason,
            "failure_reasons": by_reason,
            "by_dag_run": by_run,
            "oldest_failure": dates_row[0] if dates_row else None,
            "newest_failure": dates_row[1] if dates_row else None,
            "unresolved": unresolved,
        }

    def mark_resolved(self, order_ids: list[str]) -> int:
        """Mark specific order_ids as resolved (e.g. after upstream fix + replay)."""
        if not order_ids:
            return 0
        placeholders = ",".join("?" * len(order_ids))
        with self._connect() as conn:
            cur = conn.execute(
                f"UPDATE dead_letter_queue SET resolved = 1 WHERE order_id IN ({placeholders})",
                order_ids,
            )
        return cur.rowcount

    def clear(self) -> None:
        """Truncate the DLQ — use only in tests."""
        with self._connect() as conn:
            conn.execute("DELETE FROM dead_letter_queue")
