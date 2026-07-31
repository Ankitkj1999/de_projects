"""
src/dag_factory.py — DAG Factory Engine
========================================
Real-World Problem:
    In SaaS and enterprise platforms onboarding dozens of clients, copy-pasting DAG
    definitions creates configuration drift, code duplication, and maintenance nightmares.

    This module provides the DAG Factory engine that takes client configurations
    and dynamically compiles Airflow DAG objects at parse time.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List
import yaml

# Add src to path
SRC_DIR = Path(__file__).resolve().parent

# Ensure local imports resolve to this directory
for mod in ["notifier", "validator", "loader", "client_extractor"]:
    sys.modules.pop(mod, None)

if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from client_extractor import extract
from validator import validate
from loader import load
from notifier import notify

# Airflow compatibility layer supporting Airflow 3.x, Airflow 2.x, and standalone mock
try:
    from airflow import DAG
    try:
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        from airflow.operators.python import PythonOperator
except ImportError:
    class DAG:
        def __init__(self, dag_id: str, default_args: dict = None, schedule: str = None, tags: list = None, **kwargs):
            self.dag_id = dag_id
            self.default_args = default_args or {}
            self.schedule = schedule
            self.tags = tags or []
            self.tasks = []
            self.kwargs = kwargs

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            pass

    class PythonOperator:
        def __init__(self, task_id: str, python_callable: Any, op_kwargs: dict = None, **kwargs):
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


class ClientDAGFactory:
    """
    Reads configuration and programmatically generates Airflow DAGs.
    """

    def __init__(self, config_path: str | Path):
        self.config_path = Path(config_path)

    def load_configs(self) -> List[Dict[str, Any]]:
        with open(self.config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data.get("clients", [])

    def create_dag(self, client: Dict[str, Any]) -> Any:
        client_id = client["client_id"]
        dag_id = f"etl_{client_id}"
        schedule = client.get("schedule", "@daily")
        sla_mins = client.get("sla_minutes", 30)

        default_args = {
            "owner": client.get("owner", "data-engineering"),
            "depends_on_past": False,
            "email_on_failure": False,
            "retries": 2,
            "retry_delay": timedelta(seconds=10),
        }

        dag = DAG(
            dag_id=dag_id,
            default_args=default_args,
            description=f"Auto-generated multi-tenant ETL pipeline for {client.get('display_name', client_id)}",
            schedule=schedule,
            start_date=datetime(2024, 1, 15),
            catchup=False,
            tags=["multi-tenant", "auto-generated", client_id],
        )

        with dag:
            def _extract_task(**kwargs):
                return extract(client)

            def _validate_task(**kwargs):
                ext_res = extract(client)
                return validate(client, ext_res.get("records", []))

            def _load_task(**kwargs):
                ext_res = extract(client)
                return load(client, ext_res.get("records", []))

            def _notify_task(**kwargs):
                load_res = load(client, extract(client).get("records", []))
                return notify(client, load_res)

            t_extract = PythonOperator(
                task_id=f"extract_{client_id}",
                python_callable=_extract_task,
            )

            t_validate = PythonOperator(
                task_id=f"validate_{client_id}",
                python_callable=_validate_task,
            )

            t_load = PythonOperator(
                task_id=f"load_{client_id}",
                python_callable=_load_task,
            )

            t_notify = PythonOperator(
                task_id=f"notify_{client_id}",
                python_callable=_notify_task,
            )

            t_extract >> t_validate >> t_load >> t_notify

        return dag

    def generate_all_dags(self) -> Dict[str, Any]:
        """Generates a mapping of dag_id -> DAG object."""
        configs = self.load_configs()
        dags = {}
        for cfg in configs:
            dag = self.create_dag(cfg)
            dags[dag.dag_id] = dag
        return dags
