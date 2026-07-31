"""
dags/upstream_producer_dag.py — Upstream File & Success Marker Producer DAG
===========================================================================
Real-World Problem:
    Demonstrates the upstream side of the Hadoop/Spark/Glue atomic write contract:
    Task 1: writes the data payload (CSV)
    Task 2: drops the `_SUCCESS_{ds}` marker file with SHA-256 checksum and row count
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

from file_producer import produce, write_marker

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
        def __init__(self, task_id: str, python_callable: Any = None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.kwargs = kwargs
            self.upstream_list = []
            self.downstream_list = []

        def __rshift__(self, other):
            self.downstream_list.append(other)
            other.upstream_list.append(self)
            return other


def task_produce_orders(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    return produce(logical_date)


def task_write_marker(**context: Any) -> str:
    logical_date = context.get("ds", "2024-01-15")
    return write_marker(logical_date)


default_args = {
    "owner": "upstream-systems",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="upstream_file_producer",
    default_args=default_args,
    description="Upstream pipeline producing orders CSV and dropping atomic _SUCCESS marker",
    schedule="@daily",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    tags=["upstream", "file-producer", "marker-pattern"],
) as dag:

    t1_produce = PythonOperator(
        task_id="produce_orders_file",
        python_callable=task_produce_orders,
    )

    t2_marker = PythonOperator(
        task_id="write_success_marker",
        python_callable=task_write_marker,
    )

    t1_produce >> t2_marker
