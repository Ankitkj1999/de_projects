"""
Unit and Integration Tests for Project 1: Schema Drift Gate
"""

import pytest
import os
import sys
import shutil
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.schema_gate import SchemaValidationGate, EXPECTED_COLUMNS_ORDERED

@pytest.fixture
def gate(tmp_path):
    bronze_dir = tmp_path / "bronze"
    return SchemaValidationGate(bronze_landing_dir=str(bronze_dir))

@pytest.fixture
def clean_orders_path():
    path = "data/raw/olist_orders_dataset.csv"
    assert os.path.exists(path), "Run data/generate_olist_data.py first!"
    return path

def test_clean_feed_passes_gate(gate, clean_orders_path):
    is_valid, report = gate.validate_feed(clean_orders_path)
    assert is_valid is True
    assert len(report["failures"]) == 0
    assert len(report["missing_columns"]) == 0
    assert len(report["extra_columns"]) == 0

def test_renamed_column_fails_gate(gate, tmp_path, clean_orders_path):
    # Vendor renamed order_purchase_timestamp -> purchase_date
    df = pd.read_csv(clean_orders_path).head(50)
    df = df.rename(columns={"order_purchase_timestamp": "purchase_date"})
    corrupt_file = tmp_path / "orders_renamed.csv"
    df.to_csv(corrupt_file, index=False)

    is_valid, report = gate.validate_feed(str(corrupt_file))
    assert is_valid is False
    assert "order_purchase_timestamp" in report["missing_columns"]
    assert "purchase_date" in report["extra_columns"]
    assert any(f["expectation"] == "expect_table_columns_to_match_ordered_list" for f in report["failures"])

def test_illegal_status_fails_gate(gate, tmp_path, clean_orders_path):
    df = pd.read_csv(clean_orders_path).head(50)
    df.loc[0, "order_status"] = "INVALID_VENDOR_STATE"
    corrupt_file = tmp_path / "orders_bad_status.csv"
    df.to_csv(corrupt_file, index=False)

    is_valid, report = gate.validate_feed(str(corrupt_file))
    assert is_valid is False
    assert any(f["expectation"] == "expect_column_values_to_be_in_set" for f in report["failures"])

def test_missing_column_fails_gate(gate, tmp_path, clean_orders_path):
    df = pd.read_csv(clean_orders_path).head(50)
    df = df.drop(columns=["customer_id"])
    corrupt_file = tmp_path / "orders_missing_col.csv"
    df.to_csv(corrupt_file, index=False)

    is_valid, report = gate.validate_feed(str(corrupt_file))
    assert is_valid is False
    assert "customer_id" in report["missing_columns"]

def test_bronze_process_feed_flow(gate, clean_orders_path, tmp_path):
    # Test valid feed lands file in bronze
    success = gate.process_feed(clean_orders_path)
    assert success is True
    bronze_files = list(gate.bronze_landing_dir.glob("bronze_*.csv"))
    assert len(bronze_files) > 0

    # Test invalid feed does not land file in bronze
    df = pd.read_csv(clean_orders_path).head(10).rename(columns={"order_id": "id"})
    bad_file = tmp_path / "bad.csv"
    df.to_csv(bad_file, index=False)

    initial_bronze_count = len(list(gate.bronze_landing_dir.glob("bronze_*.csv")))
    success_bad = gate.process_feed(str(bad_file))
    assert success_bad is False
    assert len(list(gate.bronze_landing_dir.glob("bronze_*.csv"))) == initial_bronze_count
