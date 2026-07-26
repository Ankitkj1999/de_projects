"""
src/file_producer.py — Upstream File Producer

Real-World Problem:
    In event-driven data pipelines, upstream teams write large CSV/Parquet files to
    shared storage (HDFS, S3, ADLS) and then atomically drop a "_SUCCESS" marker file
    once the write is fully complete. This two-phase write pattern is the backbone of
    Hadoop MapReduce, Apache Spark, AWS Glue, and Databricks pipelines.

    Without the marker, a downstream consumer might begin reading a file that is still
    being written — causing partial reads, corrupt DataFrames, or silent data loss.
    This module simulates the upstream producer side of that contract.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import random
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Olist-style order field definitions
# ---------------------------------------------------------------------------
ORDER_STATUSES = ["delivered", "shipped", "processing", "invoiced", "canceled"]
PAYMENT_TYPES  = ["credit_card", "boleto", "voucher", "debit_card"]
PRODUCT_CATS   = [
    "health_beauty", "bed_bath_table", "sports_leisure", "furniture_decor",
    "computers_accessories", "housewares", "watches_gifts", "telephony",
    "garden_tools", "toys",
]


def _make_order_row(rng: random.Random, logical_date: str, idx: int) -> dict:
    """Generate a single synthetic Olist-style order row."""
    order_id      = f"ORD-{logical_date.replace('-', '')}-{idx:05d}"
    customer_id   = f"CUST-{rng.randint(10000, 99999)}"
    status        = rng.choice(ORDER_STATUSES)
    purchase_ts   = f"{logical_date} {rng.randint(0,23):02d}:{rng.randint(0,59):02d}:{rng.randint(0,59):02d}"
    payment_type  = rng.choice(PAYMENT_TYPES)
    payment_value = round(rng.uniform(15.0, 850.0), 2)
    freight_value = round(rng.uniform(5.0, 60.0), 2)
    product_cat   = rng.choice(PRODUCT_CATS)
    review_score  = rng.choice([1, 2, 3, 4, 5, None])

    return {
        "order_id":           order_id,
        "customer_id":        customer_id,
        "order_status":       status,
        "order_purchase_timestamp": purchase_ts,
        "payment_type":       payment_type,
        "payment_value":      payment_value,
        "freight_value":      freight_value,
        "product_category":   product_cat,
        "review_score":       review_score if review_score is not None else "",
    }


def _compute_checksum(file_path: str) -> str:
    """Compute SHA-256 checksum of a file."""
    sha = hashlib.sha256()
    with open(file_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def produce(
    logical_date: str,
    output_dir: str = "data/processed/airflow/landing/",
    row_count: int = 300,
) -> dict:
    """
    Generate synthetic Olist-style orders CSV for the given logical date.

    Parameters
    ----------
    logical_date : str
        ISO date string (e.g. "2024-01-15") used as random seed for reproducibility.
    output_dir : str
        Target directory for the CSV file.
    row_count : int
        Number of order rows to generate (default 300).

    Returns
    -------
    dict
        {"file_path": str, "row_count": int, "produced_at": str, "checksum": str}
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    file_name = f"orders_{logical_date}.csv"
    file_path = str(Path(output_dir) / file_name)

    # Deterministic seed: same date always produces same data
    seed = int(logical_date.replace("-", ""))
    rng  = random.Random(seed)

    fieldnames = [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "payment_type",
        "payment_value", "freight_value", "product_category", "review_score",
    ]
    rows = [_make_order_row(rng, logical_date, i + 1) for i in range(row_count)]

    with open(file_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    checksum    = _compute_checksum(file_path)
    produced_at = datetime.utcnow().isoformat()

    return {
        "file_path":   file_path,
        "row_count":   row_count,
        "produced_at": produced_at,
        "checksum":    checksum,
    }


def write_marker(
    logical_date: str,
    metadata: dict | None = None,
    output_dir: str = "data/processed/airflow/landing/",
) -> str:
    """
    Write a JSON _SUCCESS marker file for the given logical date.

    The marker contains row_count, produced_at timestamp, and SHA-256 checksum of
    the companion CSV — enabling downstream consumers to verify data integrity before
    processing.

    Parameters
    ----------
    logical_date : str
        ISO date string matching the companion CSV file.
    metadata : dict | None
        Metadata from produce() output. If None, re-derives it from the existing CSV.
    output_dir : str
        Directory where both CSV and marker are stored.

    Returns
    -------
    str
        Absolute path of the written _SUCCESS marker file.
    """
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    marker_name = f"_SUCCESS_{logical_date}"
    marker_path = str(Path(output_dir) / marker_name)

    if metadata is None:
        # Re-derive from the existing CSV file
        csv_path = str(Path(output_dir) / f"orders_{logical_date}.csv")
        if not Path(csv_path).exists():
            raise FileNotFoundError(
                f"CSV not found at {csv_path}. Run produce() first."
            )
        with open(csv_path, encoding="utf-8") as fh:
            row_count = sum(1 for _ in fh) - 1  # subtract header
        checksum    = _compute_checksum(csv_path)
        produced_at = datetime.utcnow().isoformat()
        metadata    = {
            "row_count":   row_count,
            "produced_at": produced_at,
            "checksum":    checksum,
        }

    marker_content = {
        "logical_date": logical_date,
        "row_count":    metadata.get("row_count"),
        "produced_at":  metadata.get("produced_at"),
        "checksum":     metadata.get("checksum"),
        "marker_written_at": datetime.utcnow().isoformat(),
    }

    with open(marker_path, "w", encoding="utf-8") as fh:
        json.dump(marker_content, fh, indent=2)

    return marker_path
