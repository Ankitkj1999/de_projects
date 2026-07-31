"""
dags/idempotent_sales_loader_dag.py — Idempotent Daily Sales Loader & Backfill DAG
==================================================================================
Real-World Problem:
    Backfilling historical partitions or manually retrying failed runs often duplicates
    millions of rows in downstream data warehouses when jobs use naive INSERT statements.

    This DAG uses Airflow's `{{ ds }}` logical date macro with deterministic extraction,
    SQLite UPSERT (INSERT OR REPLACE), automated partition verification, and full
    audit logging.

DAG Flow:
    check_partition_exists (reads partition status)
           │
           ▼
    extract_daily_partition (deterministic generator seeded by {{ ds }})
           │
           ▼
    upsert_to_warehouse (UPSERT into sales_partitions)
           │
           ▼
    verify_idempotency (asserts exact row count & zero duplicates)
           │
           ▼
    log_audit_trail (records run_id, backfill flag, and row stats)
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict

DAG_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DAG_DIR.parent
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from partition_checker import PartitionChecker
from extractor import extract
from warehouse_loader import WarehouseLoader
from idempotency_verifier import verify
from audit_logger import log

# Compatibility layer supporting Airflow 3.x, Airflow 2.x, and standalone mock
try:
    from airflow import DAG
    try:
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        from airflow.operators.python import PythonOperator
except ImportError:
    class DAG:
        def __init__(self, dag_id: str, default_args: dict = None, schedule: str = None, **kwargs):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.schedule = schedule
            self.tasks = []
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class PythonOperator:
        def __init__(self, task_id: str, python_callable: Any = None, op_kwargs: dict = None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.op_kwargs = op_kwargs or {}
            self.kwargs = kwargs
            self.upstream_list = []
            self.downstream_list = []

        def __rshift__(self, other):
            if isinstance(other, list):
                for o in other:
                    self.downstream_list.append(o)
                    o.upstream_list.append(self)
            else:
                self.downstream_list.append(other)
                other.upstream_list.append(self)
            return other


def task_check_partition(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    checker = PartitionChecker()
    res = checker.check(logical_date)
    return res


def task_extract(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    return extract(logical_date)


def task_upsert(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    run_id = context.get("run_id", "manual")
    ext_data = extract(logical_date)
    loader = WarehouseLoader()
    return loader.upsert(logical_date, ext_data["records"], run_id=run_id)


def task_verify(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    return verify(logical_date)


def task_log_audit(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    run_id = context.get("run_id", "manual")
    checker = PartitionChecker()
    check_info = checker.check(logical_date)
    is_backfill = check_info["partition_exists"]

    loader = WarehouseLoader()
    count = loader.get_partition_count(logical_date)
    stats = {"upserted": count, "partition_row_count": count, "status": "COMMITTED"}

    return log(logical_date, stats, run_id=run_id, is_backfill=is_backfill)


default_args = {
    "owner": "data-platform-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="idempotent_sales_loader",
    default_args=default_args,
    description="Idempotent, backfill-safe daily partition loader with UPSERT and audit logging",
    schedule="@daily",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    tags=["production", "idempotent", "backfill-safe", "upsert"],
) as dag:

    t1_check = PythonOperator(
        task_id="check_partition_exists",
        python_callable=task_check_partition,
    )

    t2_extract = PythonOperator(
        task_id="extract_daily_partition",
        python_callable=task_extract,
    )

    t3_upsert = PythonOperator(
        task_id="upsert_to_warehouse",
        python_callable=task_upsert,
    )

    t4_verify = PythonOperator(
        task_id="verify_idempotency",
        python_callable=task_verify,
    )

    t5_audit = PythonOperator(
        task_id="log_audit_trail",
        python_callable=task_log_audit,
    )

    t1_check >> t2_extract >> t3_upsert >> t4_verify >> t5_audit
