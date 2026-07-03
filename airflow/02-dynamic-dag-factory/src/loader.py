"""
src/loader.py — Multi-Tenant SQLite Database Loader with Isolation
===================================================================
Real-World Problem:
    Multi-tenant pipelines require strong data isolation — records must never leak
    across client partitions or tables. Re-runs must also be idempotent.

    This module loads validated client orders into isolated tables per tenant
    using UPSERT (INSERT OR REPLACE).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Any, Dict, List
import pandas as pd

DEFAULT_DB = Path("data/processed/airflow/warehouse/multi_tenant_warehouse.db")


def get_connection(db_path: Path = DEFAULT_DB) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return sqlite3.connect(str(db_path))


def init_client_table(client_config: Dict[str, Any], db_path: Path = DEFAULT_DB) -> None:
    table_name = client_config["target_table"]
    with get_connection(db_path) as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                order_id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                customer_id TEXT NOT NULL,
                order_status TEXT NOT NULL,
                price REAL NOT NULL,
                order_date TEXT NOT NULL,
                loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()


def load(client_config: Dict[str, Any], records: List[Dict[str, Any]], db_path: Path = DEFAULT_DB) -> Dict[str, Any]:
    """
    Idempotently loads records into the client's isolated table.
    """
    table_name = client_config["target_table"]
    init_client_table(client_config, db_path)

    if not records:
        return {"client_id": client_config["client_id"], "table": table_name, "loaded": 0, "status": "EMPTY"}

    with get_connection(db_path) as conn:
        conn.executemany(
            f"""
            INSERT OR REPLACE INTO {table_name}
            (order_id, client_id, customer_id, order_status, price, order_date, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
            """,
            [
                (
                    r["order_id"],
                    r["client_id"],
                    r["customer_id"],
                    r["order_status"],
                    float(r["price"]),
                    r["order_date"],
                )
                for r in records
            ],
        )
        conn.commit()
        count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

    return {
        "client_id": client_config["client_id"],
        "table": table_name,
        "loaded_batch": len(records),
        "total_in_table": count,
        "mode": "UPSERT",
        "status": "LOADED",
    }


def query_client_table(client_config: Dict[str, Any], db_path: Path = DEFAULT_DB) -> pd.DataFrame:
    table_name = client_config["target_table"]
    init_client_table(client_config, db_path)
    with get_connection(db_path) as conn:
        return pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
