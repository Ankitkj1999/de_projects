"""
Demo Script: Project 1 - Schema Drift Gate
Demonstrates how the validation gate protects Bronze ingestion:
1. Ingest clean feed -> Passes gate and lands in Bronze.
2. Ingest drifted feed (vendor renamed column) -> Pipeline stops with actionable report.
"""

import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))
from schema_gate import SchemaValidationGate

def run_demo():
    gate = SchemaValidationGate()

    clean_feed = "data/raw/olist_orders_dataset.csv"
    drifted_feed = "data/corrupt/orders_vendor_renamed_drift.csv"

    print("=" * 70)
    print("STEP 1: INGESTING CLEAN VENDOR FEED")
    print("=" * 70)
    clean_result = gate.process_feed(clean_feed)
    assert clean_result is True, "Clean feed should have passed gate!"

    print("\n" + "=" * 70)
    print("STEP 2: INGESTING CORRUPTED / DRIFTED VENDOR FEED")
    print("(Vendor unannounced rename: order_purchase_timestamp -> purchase_date)")
    print("=" * 70)
    drift_result = gate.process_feed(drifted_feed)
    assert drift_result is False, "Drifted feed should have been rejected by gate!"

    print("\n" + "=" * 70)
    print("DEMO SUMMARY: Gate successfully protected Bronze from schema drift!")
    print("=" * 70)

if __name__ == "__main__":
    run_demo()

