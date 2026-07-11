"""
src/sla_callbacks.py — Airflow SLA Miss & Failure Callback Handlers
===================================================================
Real-World Problem:
    When critical business SLAs (e.g. 8 AM executive dashboard delivery) are at risk,
    silent delays are catastrophic.
    Airflow provides `sla_miss_callback` hooks to immediately notify on-call engineers
    and log breach incidents before downstream stakeholders notice the delay.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from rich.console import Console
from rich.panel import Panel

console = Console()
SLA_LOG_PATH = Path("data/processed/airflow/finance/sla_misses.json")


def on_sla_miss(dag: Any, task_list: List[Any], blocking_task_list: List[Any], slas: List[Any], blocking_tis: List[Any]) -> None:
    """
    Standard Airflow DAG-level SLA miss callback signature.
    """
    SLA_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    breach_event = {
        "event": "SLA_MISS",
        "dag_id": getattr(dag, "dag_id", "daily_finance_report"),
        "tasks_missed": [getattr(t, "task_id", str(t)) for t in task_list],
        "blocking_tasks": [getattr(t, "task_id", str(t)) for t in blocking_task_list],
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    events = []
    if SLA_LOG_PATH.exists():
        try:
            with open(SLA_LOG_PATH, "r", encoding="utf-8") as f:
                events = json.load(f)
        except Exception:
            events = []

    events.append(breach_event)
    with open(SLA_LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)

    console.print(
        Panel(
            f"[bold red]✖ SLA MISS DETECTED in DAG: {breach_event['dag_id']}[/bold red]\n"
            f"Tasks Exceeded SLA: {breach_event['tasks_missed']}\n"
            f"Blocking Tasks: {breach_event['blocking_tasks']}",
            title="[red]AIRFLOW SLA MONITOR[/red]",
            border_style="red",
        )
    )


def simulate_sla_miss(task_id: str, dag_id: str = "daily_finance_report", elapsed_secs: int = 720, sla_secs: int = 600) -> Dict[str, Any]:
    """
    Simulates an SLA breach incident for local testing and demo visualization.
    """
    SLA_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "event": "SLA_MISS",
        "dag_id": dag_id,
        "task_id": task_id,
        "elapsed_seconds": elapsed_secs,
        "sla_seconds": sla_secs,
        "breach_amount_seconds": elapsed_secs - sla_secs,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    events = []
    if SLA_LOG_PATH.exists():
        try:
            with open(SLA_LOG_PATH, "r", encoding="utf-8") as f:
                events = json.load(f)
        except Exception:
            events = []
    events.append(event)
    with open(SLA_LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(events, f, indent=2)

    return event
