"""
src/warehouse_loader.py — Warehouse Ingestion Loader for Sensor Pipeline
========================================================================
Real-World Problem:
    Loads verified orders into SQLite warehouse table `sensor_orders`.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List

DEFAULT_SENSOR_DB = Path("data/processed/airflow/sensor_warehouse/orders.db")


def load(logical_date: str, records: List[Dict[str, Any]], db_path: Path = DEFAULT_SENSOR_DB) -> Dict[str, Any]:
    """
    Loads verified order batch into SQLite warehouse.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with sqlite3.connect(str(db_path)) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sensor_orders (
                order_id TEXT PRIMARY KEY,
                customer_id TEXT NOT NULL,
                order_status TEXT NOT NULL,
                payment_value REAL NOT NULL,
                partition_date TEXT NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()

        if records:
            conn.executemany(
                """
                INSERT OR REPLACE INTO sensor_orders
                (order_id, customer_id, order_status, payment_value, partition_date, loaded_at)
                VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                """,
                [
                    (
                        r["order_id"],
                        r["customer_id"],
                        r["order_status"],
                        float(r.get("payment_value", 0.0)),
                        logical_date,
                    )
                    for r in records
                ],
            )
            conn.commit()

        count = conn.execute("SELECT COUNT(*) FROM sensor_orders WHERE partition_date = ?", (logical_date,)).fetchone()[0]

    return {
        "logical_date": logical_date,
        "rows_loaded": len(records),
        "partition_total": count,
        "status": "LOADED",
    }
