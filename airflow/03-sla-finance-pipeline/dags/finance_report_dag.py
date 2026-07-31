"""
dags/finance_report_dag.py — SLA-Monitored Financial Report DAG
==============================================================
Real-World Problem:
    The CFO's daily revenue report must land before 8:00 AM without fail.
    If upstream data is stale, the DAG must ShortCircuit immediately to avoid publishing
    erroneous numbers. If heavy aggregation tasks exceed their time budgets, Airflow's
    `sla_miss_callback` must trigger instant notifications to on-call data engineers.

DAG Architecture:
    freshness_check (ShortCircuitOperator, sla=5m)
           │
      [Pass / Fresh]
           ▼
    extract_raw_sales (PythonOperator, sla=15m, timeout=20m)
           │
           ▼
    apply_currency_conversion (PythonOperator, sla=10m)
           │
           ▼
    aggregate_revenue (PythonOperator, sla=10m, timeout=15m)
           │
           ▼
    generate_report (PythonOperator, sla=5m)
           │
           ▼
    notify_cfo (PythonOperator)
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

from freshness_checker import check_source_freshness
from sales_extractor import extract
from currency_converter import convert
from aggregator import aggregate
from report_generator import generate
from sla_callbacks import on_sla_miss
from notifier import notify_cfo

# Compatibility layer supporting Airflow 3.x, Airflow 2.x, and standalone mock
try:
    from airflow import DAG
    try:
        from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
    except ImportError:
        from airflow.operators.python import PythonOperator, ShortCircuitOperator
except ImportError:
    class DAG:
        def __init__(self, dag_id: str, default_args: dict = None, schedule: str = None, sla_miss_callback: Any = None, **kwargs):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.schedule = schedule
            self.sla_miss_callback = sla_miss_callback
            self.tasks = []
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class BaseOperator:
        def __init__(self, task_id: str, python_callable: Any = None, sla: timedelta = None, execution_timeout: timedelta = None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
            self.sla = sla
            self.execution_timeout = execution_timeout
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

    class PythonOperator(BaseOperator):
        pass

    class ShortCircuitOperator(BaseOperator):
        pass


def task_check_freshness(**context: Any) -> bool:
    """ShortCircuit task: skips rest of pipeline if source data is older than 24h."""
    result = check_source_freshness()
    return result["is_fresh"]


def task_extract_sales(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    return extract(logical_date=logical_date)


def task_convert_currency(**context: Any) -> Any:
    sales = extract("2024-01-15")
    return convert(sales["records"])


def task_aggregate(**context: Any) -> Dict[str, Any]:
    sales = extract("2024-01-15")
    converted = convert(sales["records"])
    return aggregate(converted)


def task_generate_report(**context: Any) -> str:
    agg = task_aggregate()
    report_path = generate(agg, "2024-01-15")
    return str(report_path)


def task_notify_cfo(**context: Any) -> Dict[str, Any]:
    agg = task_aggregate()
    report_path = task_generate_report()
    return notify_cfo(report_path, agg)


default_args = {
    "owner": "financial-data-platform",
    "depends_on_past": True,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="daily_finance_report",
    default_args=default_args,
    description="Daily 6:00 AM CFO Financial Revenue Pipeline with SLA Miss Alerting & ShortCircuit Freshness Gate",
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    sla_miss_callback=on_sla_miss,
    tags=["finance", "sla-monitored", "short-circuit", "cfo-reporting"],
) as dag:

    t1_freshness = ShortCircuitOperator(
        task_id="freshness_check",
        python_callable=task_check_freshness,
        sla=timedelta(minutes=5),
    )

    t2_extract = PythonOperator(
        task_id="extract_raw_sales",
        python_callable=task_extract_sales,
        sla=timedelta(minutes=15),
        execution_timeout=timedelta(minutes=20),
    )

    t3_fx = PythonOperator(
        task_id="apply_currency_conversion",
        python_callable=task_convert_currency,
        sla=timedelta(minutes=10),
    )

    t4_agg = PythonOperator(
        task_id="aggregate_revenue",
        python_callable=task_aggregate,
        sla=timedelta(minutes=10),
        execution_timeout=timedelta(minutes=15),
    )

    t5_report = PythonOperator(
        task_id="generate_report",
        python_callable=task_generate_report,
        sla=timedelta(minutes=5),
    )

    t6_notify = PythonOperator(
        task_id="notify_cfo",
        python_callable=task_notify_cfo,
    )

    t1_freshness >> t2_extract >> t3_fx >> t4_agg >> t5_report >> t6_notify
