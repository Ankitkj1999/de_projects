"""
src/order_processor.py — Validated Order Ingestion Processor
============================================================
Real-World Problem:
    Before ingesting orders, downstream jobs must verify that the CSV file has not
    been modified, truncated, or tampered with by verifying its SHA-256 checksum
    against the metadata in the `_SUCCESS` marker file.
"""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List


def compute_checksum(file_path: Path | str) -> str:
    sha = hashlib.sha256()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def process(logical_date: str, landing_dir: str = "data/processed/airflow/landing/") -> Dict[str, Any]:
    """
    Reads companion CSV, validates against the _SUCCESS marker checksum and metadata,
    and returns parsed records.
    """
    landing_path = Path(landing_dir)
    csv_file = landing_path / f"orders_{logical_date}.csv"
    marker_file = landing_path / f"_SUCCESS_{logical_date}"

    if not csv_file.exists():
        raise FileNotFoundError(f"Missing order file: {csv_file}")
    if not marker_file.exists():
        raise FileNotFoundError(f"Missing required marker file: {marker_file}")

    with open(marker_file, "r", encoding="utf-8") as f:
        marker_meta = json.load(f)

    # 1. Checksum verification
    actual_checksum = compute_checksum(csv_file)
    expected_checksum = marker_meta.get("checksum")
    checksum_verified = actual_checksum == expected_checksum

    # 2. Parse records
    records: List[Dict[str, Any]] = []
    with open(csv_file, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(dict(row))

    # 3. Row count reconciliation
    count_verified = len(records) == int(marker_meta.get("row_count", -1))
    file_verified = checksum_verified and count_verified

    return {
        "logical_date": logical_date,
        "row_count": len(records),
        "records": records,
        "checksum_verified": checksum_verified,
        "count_verified": count_verified,
        "file_verified": file_verified,
        "actual_checksum": actual_checksum,
        "expected_checksum": expected_checksum,
        "status": "VERIFIED" if file_verified else "CORRUPTED",
    }
