"""
dags/downstream_processor_dag.py — Event-Driven Sensor & Circuit Breaker DAG
============================================================================
Real-World Problem:
    Downstream processing must only begin after the upstream _SUCCESS marker arrives.
    If the sensor times out, the Circuit Breaker pattern trips: alerting on-call
    and gracefully skipping downstream ETL tasks without failing the entire platform.

DAG Flow:
                     wait_for_success_marker (soft_fail=True)
                                │
                                ▼
                       check_sensor_result (Branch)
                                ├── [Marker Present] ──► process_orders
                                │                             │
                                │                             ▼
                                │                        load_to_warehouse
                                │                             │
                                │                             ▼
                                │                        notify_success
                                │
                                └── [Sensor Timed Out] ──► fire_circuit_breaker
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

from sensor import SuccessMarkerSensor
from circuit_breaker import CircuitBreaker
from order_processor import process
from warehouse_loader import load

# Compatibility layer supporting Airflow 3.x, Airflow 2.x, and standalone mock
try:
    from airflow import DAG
    try:
        from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
    except ImportError:
        from airflow.operators.python import PythonOperator, BranchPythonOperator
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

    class BaseOperator:
        def __init__(self, task_id: str, python_callable: Any = None, **kwargs):
            self.task_id = task_id
            self.python_callable = python_callable
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

    class BranchPythonOperator(BaseOperator):
        pass


def task_sensor(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    sensor = SuccessMarkerSensor()
    res = sensor.poll(logical_date, timeout_seconds=10)
    return res


def task_branch(**context: Any) -> str:
    logical_date = context.get("ds", "2024-01-15")
    sensor = SuccessMarkerSensor()
    marker = sensor.poke(logical_date)
    breaker = CircuitBreaker()
    return breaker.evaluate_sensor_result({"found": marker is not None, "timed_out": marker is None})


def task_process(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    return process(logical_date)


def task_load(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    data = process(logical_date)
    return load(logical_date, data["records"])


def task_notify(**context: Any) -> None:
    pass


def task_circuit_breaker(**context: Any) -> Dict[str, Any]:
    logical_date = context.get("ds", "2024-01-15")
    breaker = CircuitBreaker()
    return breaker.trip(logical_date, reason="SENSOR_TIMEOUT_EXCEEDED")


default_args = {
    "owner": "downstream-analytics",
    "retries": 0,
}

with DAG(
    dag_id="downstream_orders_processor",
    default_args=default_args,
    description="Event-driven downstream processor waiting for _SUCCESS marker with circuit breaker branch",
    schedule="@daily",
    start_date=datetime(2024, 1, 15),
    catchup=False,
    tags=["sensor", "circuit-breaker", "event-driven", "branching"],
) as dag:

    t1_sensor = PythonOperator(
        task_id="wait_for_success_marker",
        python_callable=task_sensor,
    )

    t2_branch = BranchPythonOperator(
        task_id="check_sensor_result",
        python_callable=task_branch,
    )

    t3a_process = PythonOperator(
        task_id="process_orders",
        python_callable=task_process,
    )

    t4a_load = PythonOperator(
        task_id="load_to_warehouse",
        python_callable=task_load,
    )

    t5a_notify = PythonOperator(
        task_id="notify_success",
        python_callable=task_notify,
    )

    t3b_breaker = PythonOperator(
        task_id="fire_circuit_breaker",
        python_callable=task_circuit_breaker,
    )

    t1_sensor >> t2_branch
    t2_branch >> [t3a_process, t3b_breaker]
    t3a_process >> t4a_load >> t5a_notify
