"""
Unit and Integration Tests for Project 3: Volume & Freshness Anomaly Monitor
"""

import pytest
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.freshness_volume_gate import VolumeFreshnessGate
from monitors.baseline_tracker import HistoricalBaselineTracker

@pytest.fixture
def clean_orders():
    path = "data/raw/olist_orders_dataset.csv"
    df = pd.read_csv(path)
    # Set fresh timestamps
    now = datetime.now()
    df["order_purchase_timestamp"] = (now - timedelta(hours=2)).strftime("%Y-%m-%d %H:%M:%S")
    return df

@pytest.fixture
def gate(tmp_path):
    history_file = tmp_path / "test_history.json"
    tracker = HistoricalBaselineTracker(history_file=str(history_file))
    return VolumeFreshnessGate(tracker=tracker)

def test_healthy_batch_passes(gate, clean_orders):
    now = datetime.now()
    report = gate.evaluate_feed(clean_orders, reference_time=now)
    assert report["status"] == "HEALTHY"
    assert report["should_halt"] is False
    assert len(report["hard_failures"]) == 0
    assert len(report["warnings"]) == 0

def test_warning_level_on_minor_volume_dip(gate, clean_orders):
    now = datetime.now()
    baseline = gate.tracker.get_trailing_7day_baseline()
    # Sample to fall between warn_min and hard_min
    dip_count = int((baseline["hard_min"] + baseline["warn_min"]) / 2)
    sample_df = clean_orders.sample(n=dip_count, random_state=42).copy()

    report = gate.evaluate_feed(sample_df, reference_time=now)
    assert report["status"] == "WARNING"
    assert report["should_halt"] is False
    assert any(w["expectation"] == "expect_table_row_count_to_be_between" for w in report["warnings"])

def test_hard_fail_on_half_load_volume_collapse(gate, clean_orders):
    now = datetime.now()
    # 300 rows is far below 60% hard floor (~1500)
    collapsed_df = clean_orders.sample(n=300, random_state=42).copy()

    report = gate.evaluate_feed(collapsed_df, reference_time=now)
    assert report["status"] == "CRITICAL_FAILURE"
    assert report["should_halt"] is True
    assert any(f["expectation"] == "expect_table_row_count_to_be_between" for f in report["hard_failures"])

def test_hard_fail_on_stale_data(gate, clean_orders):
    now = datetime.now()
    stale_df = clean_orders.copy()
    # Timestamps from 5 days ago (120 hours stale)
    stale_df["order_purchase_timestamp"] = (now - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")

    report = gate.evaluate_feed(stale_df, reference_time=now, max_freshness_hours=48)
    assert report["status"] == "CRITICAL_FAILURE"
    assert any(f["expectation"] == "expect_column_max_to_be_between" for f in report["hard_failures"])

def test_mostly_tolerance_allows_small_nulls_but_catches_spikes(gate, clean_orders):
    now = datetime.now()
    # Case A: 0.5% nulls (12 rows out of 2500) -> Should pass mostly=0.98
    mild_nulls = clean_orders.copy()
    mild_nulls.loc[0:11, "customer_id"] = np.nan
    report_a = gate.evaluate_feed(mild_nulls, reference_time=now)
    assert not any(f["target"] == "customer_id" for f in report_a["hard_failures"])

    # Case B: 15% nulls (375 rows) -> Breaches mostly=0.98 -> Hard fail
    spike_nulls = clean_orders.copy()
    spike_nulls.loc[0:375, "customer_id"] = np.nan
    report_b = gate.evaluate_feed(spike_nulls, reference_time=now)
    assert report_b["status"] == "CRITICAL_FAILURE"
    assert any(f["target"] == "customer_id" for f in report_b["hard_failures"])

