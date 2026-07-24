"""
Demo Script: Project 3 - Volume & Freshness Anomaly Gate
Demonstrates:
1. Scenario 1: Healthy Daily Run -> HEALTHY (Pass).
2. Scenario 2: Minor Seasonal Dip -> WARNING (Soft alert dispatched, pipeline continues).
3. Scenario 3: "Job was green but loaded half data" -> HARD FAIL (Pipeline halted).
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from freshness_volume_gate import VolumeFreshnessGate

def run_demo():
    gate = VolumeFreshnessGate()

    clean_orders = pd.read_csv("data/raw/olist_orders_dataset.csv")

    # Set timestamps relative to now for reproducible demo
    now = datetime.now()
    clean_orders["order_purchase_timestamp"] = (now - timedelta(hours=3)).strftime("%Y-%m-%d %H:%M:%S")

    print("=" * 75)
    print("SCENARIO 1: NORMAL DAILY BATCH INGESTION (HEALTHY)")
    print("=" * 75)
    report_1 = gate.evaluate_feed(clean_orders, reference_time=now)
    gate.render_report(report_1, feed_name="Daily Production Orders Ingest (2,500 rows)")
    assert report_1["status"] == "HEALTHY"
    assert report_1["should_halt"] is False

    print("=" * 75)
    print("SCENARIO 2: MINOR VOLUME FLUCTUATION (WARNING LEVEL)")
    print("Row count dips slightly below 1.8-sigma (2,100 rows), but within 3-sigma hard floor")
    print("=" * 75)
    minor_dip_df = clean_orders.sample(n=2100, random_state=42).copy()
    report_2 = gate.evaluate_feed(minor_dip_df, reference_time=now)
    gate.render_report(report_2, feed_name="Minor Holiday Dip Batch (2,100 rows)")
    assert report_2["status"] == "WARNING"
    assert report_2["should_halt"] is False, "Warning should NOT halt pipeline!"

    print("=" * 75)
    print("SCENARIO 3: 'THE JOB WAS GREEN BUT LOADED HALF THE DATA' (CRITICAL FAILURE)")
    print("Silent API pagination failure: 350 rows (86% drop), 14-day stale data, 18% nulls")
    print("=" * 75)
    corrupted_half_load = pd.read_csv("data/corrupt/volume_freshness_anomalous_feed.csv")
    report_3 = gate.evaluate_feed(corrupted_half_load, reference_time=now)
    gate.render_report(report_3, feed_name="Silent Partial Load Feed (350 rows)")
    assert report_3["status"] == "CRITICAL_FAILURE"
    assert report_3["should_halt"] is True, "Critical volume anomaly MUST halt pipeline!"

    print("=" * 75)
    print("DEMO COMPLETE: Multi-tier volume & freshness monitoring successfully demonstrated!")
    print("=" * 75)

if __name__ == "__main__":
    run_demo()

