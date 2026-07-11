"""
src/report_generator.py — Financial Executive Report Publisher
===============================================================
Real-World Problem:
    Automating financial delivery requires persisting formal immutable daily report
    artifacts that internal audit and accounting teams can review.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict

DEFAULT_REPORTS_DIR = Path("data/processed/airflow/finance/reports")


def generate(
    aggregation: Dict[str, Any],
    logical_date: str = "2024-01-15",
    output_dir: Path | str = DEFAULT_REPORTS_DIR,
) -> Path:
    """
    Persists executive financial summary artifact as JSON.
    """
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_file = out_dir / f"revenue_report_{logical_date}.json"

    report_payload = {
        "report_title": "Daily Executive Revenue Summary",
        "reporting_date": logical_date,
        "generated_by": "airflow_sla_finance_pipeline",
        "financial_kpis": aggregation,
    }

    with open(report_file, "w", encoding="utf-8") as f:
        json.dump(report_payload, f, indent=2)

    return report_file
