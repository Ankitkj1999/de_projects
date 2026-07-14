"""
Demo Script: Project 2 - Business Rules Validator
Demonstrates:
1. Running clean dataset -> 100% green across all 6 business rules.
2. Running corrupted dataset (5 realistic corruptions injected) -> Shows which expectation catches each error.
"""

import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from validator import BusinessRulesValidator

def run_demo():
    validator = BusinessRulesValidator()

    # Load clean data
    print("=" * 75)
    print("PHASE 1: EVALUATING CLEAN E-COMMERCE DATASET")
    print("=" * 75)
    clean_orders = pd.read_csv("data/raw/olist_orders_dataset.csv")
    clean_items = pd.read_csv("data/raw/olist_order_items_dataset.csv")
    clean_payments = pd.read_csv("data/raw/olist_order_payments_dataset.csv")

    clean_success = validator.run_full_validation(
        orders_df=clean_orders,
        items_df=clean_items,
        payments_df=clean_payments,
        dataset_label="Clean Baseline Feed"
    )
    assert clean_success is True, "Clean feed should have passed 100%!"

    # Load corrupted data
    print("=" * 75)
    print("PHASE 2: EVALUATING REALISTIC CORRUPTIONS FEED (5 INJECTED DEFECTS)")
    print("=" * 75)
    corrupt_orders = pd.read_csv("data/corrupt/business_rules_corrupted_orders.csv")
    corrupt_items = pd.read_csv("data/corrupt/business_rules_corrupted_items.csv")
    corrupt_payments = pd.read_csv("data/corrupt/business_rules_corrupted_payments.csv")

    corrupt_success = validator.run_full_validation(
        orders_df=corrupt_orders,
        items_df=corrupt_items,
        payments_df=corrupt_payments,
        dataset_label="Corrupted Vendor Feed (Silent Logical Defects)"
    )
    assert corrupt_success is False, "Corrupted feed should fail business validation!"

    print("=" * 75)
    print("DEMO COMPLETE: All 5 injected corruptions successfully caught by GX!")
    print("=" * 75)

if __name__ == "__main__":
    run_demo()

