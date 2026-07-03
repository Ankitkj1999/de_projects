"""
dags/dag_factory.py — Airflow Dynamic DAG Factory Entrypoint
============================================================
Real-World Problem:
    When an enterprise onboards a new client, manual DAG file creation leads to
    copy-paste bugs, inconsistent configurations, and repository clutter.

    This dynamic DAG file is parsed by the Airflow Scheduler. It reads `clients.yaml`,
    instantiates one DAG per client with custom SLAs, schedules, and thresholds,
    and dynamically registers each into the global scope: `globals()[dag_id] = dag`.
"""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "clients.yaml"

if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dag_factory import ClientDAGFactory

# Instantiate factory and generate DAGs into global namespace for Airflow scheduler
factory = ClientDAGFactory(CONFIG_PATH)
generated_dags = factory.generate_all_dags()

for dag_id, dag_obj in generated_dags.items():
    globals()[dag_id] = dag_obj
