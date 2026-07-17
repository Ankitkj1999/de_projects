"""
Unit Tests for Project 2: Business Rules Validator
Asserts that each specific Great Expectations check catches its intended corruption.
"""

import pytest
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from src.validator import BusinessRulesValidator

@pytest.fixture
def validator():
    return BusinessRulesValidator(catalog_products_path="data/raw/olist_products_dataset.csv")

@pytest.fixture
def clean_data():
    orders = pd.read_csv("data/raw/olist_orders_dataset.csv")
    items = pd.read_csv("data/raw/olist_order_items_dataset.csv")
    payments = pd.read_csv("data/raw/olist_order_payments_dataset.csv")
    return orders, items, payments

def test_clean_data_passes_all_rules(validator, clean_data):
    orders, items, payments = clean_data
    orders_pass, orders_fail = validator.validate_orders(orders)
    items_pass, items_fail = validator.validate_items(items)
    recon_pass, recon_fail = validator.validate_order_total_reconciliation(items, payments)

    assert orders_pass is True
    assert items_pass is True
    assert recon_pass is True

def test_catches_negative_price(validator, clean_data):
    orders, items, _ = clean_data
    items_corrupt = items.copy()
    items_corrupt.loc[5, "price"] = -59.90

    passed, failures = validator.validate_items(items_corrupt)
    assert passed is False
    assert any(f["target"] == "price" for f in failures)

def test_catches_future_dated_orders(validator, clean_data):
    orders, _, _ = clean_data
    orders_corrupt = orders.copy()
    orders_corrupt.loc[10, "order_purchase_timestamp"] = (datetime.now() + timedelta(days=200)).strftime("%Y-%m-%d %H:%M:%S")

    passed, failures = validator.validate_orders(orders_corrupt)
    assert passed is False
    assert any(f["expectation"] == "expect_column_values_to_be_between" and f["target"] == "order_purchase_timestamp" for f in failures)

def test_catches_temporal_causality_paradox(validator, clean_data):
    orders, _, _ = clean_data
    orders_corrupt = orders.copy()
    # Set delivered date before purchase date
    p_ts = datetime.strptime(orders_corrupt.loc[12, "order_purchase_timestamp"], "%Y-%m-%d %H:%M:%S")
    orders_corrupt.loc[12, "order_delivered_customer_date"] = (p_ts - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")

    passed, failures = validator.validate_orders(orders_corrupt)
    assert passed is False
    assert any("expect_column_pair_values" in f["expectation"] for f in failures)

def test_catches_phantom_sku_catalog_integrity(validator, clean_data):
    _, items, _ = clean_data
    items_corrupt = items.copy()
    items_corrupt.loc[3, "product_id"] = "non_existent_ghost_sku_12345"

    passed, failures = validator.validate_items(items_corrupt)
    assert passed is False
    assert any(f["expectation"] == "expect_column_values_to_be_in_set" and f["target"] == "product_id" for f in failures)

def test_catches_duplicate_order_id(validator, clean_data):
    orders, _, _ = clean_data
    dup = orders.iloc[0:1].copy()
    orders_corrupt = pd.concat([orders, dup], ignore_index=True)

    passed, failures = validator.validate_orders(orders_corrupt)
    assert passed is False
    assert any(f["expectation"] == "expect_column_values_to_be_unique" for f in failures)

def test_catches_order_total_mismatch(validator, clean_data):
    _, items, payments = clean_data
    payments_corrupt = payments.copy()
    # Corrupt first payment value
    payments_corrupt.loc[0, "payment_value"] = payments_corrupt.loc[0, "payment_value"] + 500.00

    recon_pass, recon_fail = validator.validate_order_total_reconciliation(items, payments_corrupt)
    assert recon_pass is False
    assert len(recon_fail) > 0
    assert recon_fail[0]["mismatched_orders_count"] > 0

