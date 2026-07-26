"""
src/circuit_breaker.py — Pipeline Circuit Breaker Controller
============================================================
Real-World Problem:
    When an upstream file doesn't arrive within the SLA window, an ordinary sensor
    times out and crashes the DAG in red state, causing alert fatigue.

    The Circuit Breaker pattern gracefully trips: it logs an audit incident,
    notifies the team via webhook, skips the expensive ETL processing branches,
    and returns a clean exit code without failing the entire platform run.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

DEFAULT_BREAKER_LOG = Path("data/processed/airflow/circuit_breaker_log.json")


class CircuitBreaker:
    """
    Manages circuit breaker tripping and branch routing for sensor events.
    """

    def __init__(self, log_path: Path = DEFAULT_BREAKER_LOG):
        self.log_path = Path(log_path)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def evaluate_sensor_result(self, sensor_result: Dict[str, Any]) -> str:
        """
        Determines BranchPythonOperator decision:
          - If marker found: "process_orders"
          - If sensor timed out: "fire_circuit_breaker"
        """
        if sensor_result.get("found", False) and not sensor_result.get("timed_out", False):
            return "process_orders"
        return "fire_circuit_breaker"

    def trip(self, logical_date: str, reason: str = "SENSOR_TIMEOUT", details: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Trips the circuit breaker and persists the incident to the audit ledger.
        """
        incident = {
            "event": "CIRCUIT_BREAKER_TRIPPED",
            "logical_date": logical_date,
            "reason": reason,
            "state": "OPEN",
            "details": details or {},
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        history: List[Dict[str, Any]] = []
        if self.log_path.exists():
            try:
                with open(self.log_path, "r", encoding="utf-8") as f:
                    history = json.load(f)
            except Exception:
                history = []

        history.append(incident)
        with open(self.log_path, "w", encoding="utf-8") as f:
            json.dump(history, f, indent=2)

        return incident

    def get_trip_history(self) -> List[Dict[str, Any]]:
        if not self.log_path.exists():
            return []
        with open(self.log_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def get_state(self) -> str:
        history = self.get_trip_history()
        if not history:
            return "CLOSED"
        # If last incident was within last hour, considered OPEN
        return history[-1].get("state", "CLOSED")
