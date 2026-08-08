"""
Unit Tests for Project 4: Delta Lake Quarantine Pipeline
"""

import pytest
import os
import sys
import shutil
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.quarantine_pipeline import DeltaQuarantinePipeline

@pytest.fixture
def temp_pipeline(tmp_path):
    silver_dir = tmp_path / "silver"
    quarantine_dir = tmp_path / "quarantine"
    return DeltaQuarantinePipeline(
        silver_path=str(silver_dir),
        quarantine_path=str(quarantine_dir),
        circuit_breaker_threshold=0.15,
    )

def test_row_splitting_and_enrichment(temp_pipeline):
    now = datetime.now()
    records = [
        # Clean row 1
        {"order_id": "ord_1", "order_status": "delivered", "order_purchase_timestamp": "2024-01-01 10:00:00", "order_delivered_customer_date": "2024-01-05 10:00:00"},
        # Clean row 2
        {"order_id": "ord_2", "order_status": "shipped", "order_purchase_timestamp": "2024-01-02 10:00:00", "order_delivered_customer_date": ""},
        # Corrupt 1: Missing PK
        {"order_id": "", "order_status": "delivered", "order_purchase_timestamp": "2024-01-01 10:00:00", "order_delivered_customer_date": "2024-01-05 10:00:00"},
        # Corrupt 2: Invalid status
        {"order_id": "ord_4", "order_status": "UNKNOWN_BOGUS_STATUS", "order_purchase_timestamp": "2024-01-01 10:00:00", "order_delivered_customer_date": "2024-01-05 10:00:00"},
        # Corrupt 3: Future date
        {"order_id": "ord_5", "order_status": "delivered", "order_purchase_timestamp": "2029-01-01 10:00:00", "order_delivered_customer_date": "2029-01-05 10:00:00"},
        # Corrupt 4: Delivered before purchase
        {"order_id": "ord_6", "order_status": "delivered", "order_purchase_timestamp": "2024-01-10 10:00:00", "order_delivered_customer_date": "2024-01-02 10:00:00"},
    ]
    df = pd.DataFrame(records)
    valid_df, invalid_df = temp_pipeline.evaluate_rows(df)

    assert len(valid_df) == 2
    assert len(invalid_df) == 4
    assert len(valid_df) + len(invalid_df) == len(df)

    # Verify audit columns in invalid_df
    assert "rejection_reason" in invalid_df.columns
    assert "quarantined_at" in invalid_df.columns
    assert "failed_expectations" in invalid_df.columns

def test_zero_data_loss_and_delta_write(temp_pipeline, tmp_path):
    bronze_path = "data/corrupt/quarantine_bronze_feed_5k.csv"
    res = temp_pipeline.process_bronze_batch(bronze_path, run_id="test_run_01")

    assert res["status"] == "COMMITTED"
    assert res["valid_rows"] + res["quarantined_rows"] == res["total_rows"]

    silver_tbl = temp_pipeline.get_silver_table()
    quarantine_tbl = temp_pipeline.get_quarantine_table()

    assert silver_tbl.to_pandas().shape[0] == res["valid_rows"]
    assert quarantine_tbl.to_pandas().shape[0] == res["quarantined_rows"]

def test_circuit_breaker_trips_on_excessive_corruption(temp_pipeline, tmp_path):
    # Threshold is 0.15 (15%), provide 50% corrupt batch
    records = []
    for i in range(100):
        status = "delivered" if i < 50 else "CORRUPT_STATUS"
        records.append({
            "order_id": f"ord_{i}",
            "order_status": status,
            "order_purchase_timestamp": "2024-01-01 10:00:00",
            "order_delivered_customer_date": "2024-01-05 10:00:00",
        })
    csv_file = tmp_path / "extreme_corrupt.csv"
    pd.DataFrame(records).to_csv(csv_file, index=False)

    res = temp_pipeline.process_bronze_batch(str(csv_file), run_id="cb_test")
    assert res["status"] == "CIRCUIT_BREAKER_TRIPPED"
    assert res["circuit_breaker_tripped"] is True

