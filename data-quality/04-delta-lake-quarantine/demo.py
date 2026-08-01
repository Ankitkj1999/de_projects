"""
Demo Script: Project 4 - Delta Lake Quarantine Pattern
Demonstrates:
1. Processing 5,000 Bronze records with ~3% defects:
   - Clean rows committed to Silver Delta table.
   - Corrupt rows isolated to Quarantine Delta table with reasons.
   - Zero row loss verification.
2. Circuit Breaker trigger on high defect rates.
"""

import sys
import shutil
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))
from quarantine_pipeline import DeltaQuarantinePipeline

def run_demo():
    # Setup fresh delta target dirs for clean demo run
    silver_dir = "data/processed/delta/silver_orders"
    quarantine_dir = "data/processed/delta/quarantine_orders"

    shutil.rmtree(silver_dir, ignore_errors=True)
    shutil.rmtree(quarantine_dir, ignore_errors=True)

    pipeline = DeltaQuarantinePipeline(
        silver_path=silver_dir,
        quarantine_path=quarantine_dir,
        circuit_breaker_threshold=0.10,  # 10%
    )

    bronze_feed = "data/corrupt/quarantine_bronze_feed_5k.csv"

    print("=" * 75)
    print("STEP 1: RUNNING QUARANTINE PIPELINE ON 5,000 BRONZE ORDERS (~3% DEFECTS)")
    print("=" * 75)
    result = pipeline.process_bronze_batch(bronze_feed, run_id="run_prod_batch_001")

    assert result["status"] == "COMMITTED"
    assert result["circuit_breaker_tripped"] is False
    assert result["total_rows"] == 5000
    assert result["valid_rows"] + result["quarantined_rows"] == 5000
    assert result["quarantined_rows"] > 0

    print("=" * 75)
    print("STEP 2: INSPECTING DELTA LAKE METADATA & COMMIT LOGS")
    print("=" * 75)
    silver_table = pipeline.get_silver_table()
    quarantine_table = pipeline.get_quarantine_table()

    print(f"Silver Delta Table Version:     {silver_table.version()}")
    print(f"Silver Delta Files Count:       {len(silver_table.file_uris())}")
    print(f"Quarantine Delta Table Version: {quarantine_table.version()}")
    print(f"Quarantine Delta Files Count:   {len(quarantine_table.file_uris())}")

    print("\n" + "=" * 75)
    print("STEP 3: TESTING CIRCUIT BREAKER WITH HIGH CORRUPTION BATCH (40% DEFECTS)")
    print("=" * 75)
    # Create high corruption batch
    df_high_corrupt = pd.read_csv(bronze_feed).head(500).copy()
    df_high_corrupt.loc[0:200, "order_status"] = "CATASTROPHIC_CORRUPT_STATE"
    high_corrupt_file = "data/corrupt/high_corrupt_test_feed.csv"
    df_high_corrupt.to_csv(high_corrupt_file, index=False)

    cb_result = pipeline.process_bronze_batch(high_corrupt_file, run_id="run_emergency_test")
    assert cb_result["status"] == "CIRCUIT_BREAKER_TRIPPED"
    assert cb_result["circuit_breaker_tripped"] is True

    print("=" * 75)
    print("DEMO COMPLETE: Delta Lake Quarantine Pattern fully verified!")
    print("=" * 75)

if __name__ == "__main__":
    run_demo()
