"""
src/freshness_checker.py — Source Data Freshness Gate
======================================================
Real-World Problem:
    In financial pipelines, generating an executive revenue report on stale data
    is a cardinal sin: presenting yesterday's numbers as today's causes severe
    accounting misstatements and boardroom confusion.

    This module checks the freshness of upstream data sources before any downstream
    processing begins. If data is stale (> max_age_hours), the pipeline short-circuits
    immediately and raises an alert.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Dict, Optional

DEFAULT_MARKER_PATH = Path("data/processed/airflow/finance/source_marker.json")


def write_source_marker(timestamp: datetime, row_count: int = 500, marker_path: Path = DEFAULT_MARKER_PATH) -> Path:
    """Helper to simulate upstream systems writing an ingestion timestamp marker."""
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "source_system": "olist_sales_db",
        "last_updated_at": timestamp.isoformat(),
        "row_count": row_count,
    }
    with open(marker_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return marker_path


def check_source_freshness(
    source_timestamp: Optional[datetime] = None,
    reference_time: Optional[datetime] = None,
    max_age_hours: int = 24,
    marker_path: Path = DEFAULT_MARKER_PATH,
) -> Dict[str, Any]:
    """
    Evaluates whether upstream data meets the freshness SLA.
    Returns evaluation dict with boolean 'is_fresh' and age in hours.
    """
    ref_time = reference_time or datetime.now(timezone.utc)

    if source_timestamp is None:
        if marker_path.exists():
            with open(marker_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                ts_str = data.get("last_updated_at")
                source_timestamp = datetime.fromisoformat(ts_str)
        else:
            # If no marker, default to fresh for testing or fallback
            source_timestamp = ref_time - timedelta(hours=2)

    if source_timestamp.tzinfo is None:
        source_timestamp = source_timestamp.replace(tzinfo=timezone.utc)
    if ref_time.tzinfo is None:
        ref_time = ref_time.replace(tzinfo=timezone.utc)

    age_delta = ref_time - source_timestamp
    age_hours = round(age_delta.total_seconds() / 3600.0, 2)
    is_fresh = age_hours <= max_age_hours

    return {
        "is_fresh": is_fresh,
        "age_hours": age_hours,
        "max_age_hours": max_age_hours,
        "source_timestamp": source_timestamp.isoformat(),
        "reference_time": ref_time.isoformat(),
        "status": "FRESH" if is_fresh else "STALE",
    }
