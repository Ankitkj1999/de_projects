"""
src/audit_logger.py — Historical Pipeline Load Audit Trail
==========================================================
Real-World Problem:
    Backfilling data across historical dates must be auditable: compliance teams
    and data engineers need to know who ran a backfill, when it occurred, how many
    records were loaded, and whether it was an original scheduled run or a backfill.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_AUDIT_PATH = str(_PROJECT_ROOT / "data" / "processed" / "airflow" / "audit" / "load_audit.json")


def log(
    logical_date: str,
    stats: Dict[str, Any],
    run_id: str = "manual",
    is_backfill: bool = False,
    audit_file_path: str = DEFAULT_AUDIT_PATH,
) -> Dict[str, Any]:
    """
    Appends an immutable audit event to the load audit ledger.
    """
    path = Path(audit_file_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    event = {
        "run_id": run_id,
        "logical_date": logical_date,
        "rows_loaded": stats.get("upserted", 0),
        "total_partition_rows": stats.get("partition_row_count", 0),
        "is_backfill": is_backfill,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "status": stats.get("status", "SUCCESS"),
    }

    history: List[Dict[str, Any]] = []
    if path.exists():
        try:
            with open(path, "r", encoding="utf-8") as f:
                history = json.load(f)
        except Exception:
            history = []

    history.append(event)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)

    return event


def get_audit_trail(audit_file_path: str = DEFAULT_AUDIT_PATH) -> List[Dict[str, Any]]:
    path = Path(audit_file_path)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)
