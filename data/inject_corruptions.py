"""
Olist Dataset Corruption Engine
Generates specifically targeted data corruption scenarios for the 5 Data Quality projects.
"""

import os
import random
import uuid
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_DIR = os.path.join(BASE_DIR, "raw")
CORRUPT_DIR = os.path.join(BASE_DIR, "corrupt")

os.makedirs(CORRUPT_DIR, exist_ok=True)


def create_corruptions():
    print("Generating targeted corruptions for data quality scenarios...")
    
    # Load clean base
    df_orders = pd.read_csv(os.path.join(RAW_DIR, "olist_orders_dataset.csv"))
    df_items = pd.read_csv(os.path.join(RAW_DIR, "olist_order_items_dataset.csv"))
    df_payments = pd.read_csv(os.path.join(RAW_DIR, "olist_order_payments_dataset.csv"))
    df_products = pd.read_csv(os.path.join(RAW_DIR, "olist_products_dataset.csv"))

    # -------------------------------------------------------------
    # Project 1: Schema Drift
    # "The vendor renamed a column and nobody noticed"
    # -------------------------------------------------------------
    drift_orders = df_orders.copy()
    # Scenario 1A: Renamed columns
    drift_orders = drift_orders.rename(columns={
        "order_purchase_timestamp": "purchase_date",
        "order_status": "status"
    })
    # Add an unexpected column
    drift_orders["vendor_internal_code"] = "VENDOR-API-V2"
    drift_orders.to_csv(os.path.join(CORRUPT_DIR, "orders_vendor_renamed_drift.csv"), index=False)

    # Scenario 1B: Wrong categorical values and shuffled columns
    drift_categorical = df_orders.copy()
    drift_categorical.loc[0:20, "order_status"] = "PENDING_VENDOR_REVIEW"  # Illegal status
    drift_categorical.loc[21:40, "order_status"] = "REFUND_REQUESTED"     # Illegal status
    # Shuffle columns
    cols = list(drift_categorical.columns)
    random.shuffle(cols)
    drift_categorical = drift_categorical[cols]
    drift_categorical.to_csv(os.path.join(CORRUPT_DIR, "orders_schema_invalid_status_shuffled.csv"), index=False)

    # -------------------------------------------------------------
    # Project 2: Business Rules Violations
    # "The dashboard is wrong but nothing failed"
    # -------------------------------------------------------------
    biz_orders = df_orders.copy()
    biz_items = df_items.copy()
    biz_payments = df_payments.copy()

    # Corruption 1: Negative price & negative freight on items
    # Corrupt row 10 and 11
    biz_items.loc[10, "price"] = -149.99
    biz_items.loc[11, "freight_value"] = -25.00

    # Corruption 2: Future-dated orders
    future_date = (datetime.now() + timedelta(days=120)).strftime("%Y-%m-%d %H:%M:%S")
    biz_orders.loc[15, "order_purchase_timestamp"] = future_date
    biz_orders.loc[16, "order_purchase_timestamp"] = (datetime.now() + timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")

    # Corruption 3: Delivered date BEFORE purchase date (Temporal paradox)
    # Delivered customer date is 10 days before purchase timestamp
    purch_ts = datetime.strptime(biz_orders.loc[25, "order_purchase_timestamp"], "%Y-%m-%d %H:%M:%S")
    biz_orders.loc[25, "order_delivered_customer_date"] = (purch_ts - timedelta(days=5)).strftime("%Y-%m-%d %H:%M:%S")
    biz_orders.loc[26, "order_delivered_customer_date"] = (purch_ts - timedelta(days=12)).strftime("%Y-%m-%d %H:%M:%S")

    # Corruption 4: Phantom SKUs (product_id does not exist in catalog)
    biz_items.loc[30, "product_id"] = "phantom_sku_0000000000000000000"
    biz_items.loc[31, "product_id"] = "deleted_product_999999999999999"

    # Corruption 5: Duplicate order IDs in orders table
    dup_row = biz_orders.iloc[40:41].copy()
    biz_orders = pd.concat([biz_orders, dup_row, dup_row], ignore_index=True)

    # Corruption 6: Order total mismatch (Payment value does not equal line item sum)
    # Tamper with payment value of order at index 50
    mismatch_order_id = biz_orders.loc[50, "order_id"]
    pay_idx = biz_payments[biz_payments["order_id"] == mismatch_order_id].index
    if len(pay_idx) > 0:
        biz_payments.loc[pay_idx[0], "payment_value"] = 9999.99

    biz_orders.to_csv(os.path.join(CORRUPT_DIR, "business_rules_corrupted_orders.csv"), index=False)
    biz_items.to_csv(os.path.join(CORRUPT_DIR, "business_rules_corrupted_items.csv"), index=False)
    biz_payments.to_csv(os.path.join(CORRUPT_DIR, "business_rules_corrupted_payments.csv"), index=False)

    # -------------------------------------------------------------
    # Project 3: Volume & Freshness Anomaly
    # "The job was green but loaded half the data"
    # -------------------------------------------------------------
    # Ingest feed that dropped 65% of rows, stale timestamps, and null spike
    half_feed = df_orders.sample(n=350, random_state=101).copy()
    # Stale timestamps: shift max timestamp back by 14 days
    stale_date = (datetime.now() - timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
    half_feed["order_purchase_timestamp"] = stale_date
    # High null rate: set 18% of customer_id to null
    null_indices = half_feed.sample(frac=0.18, random_state=42).index
    half_feed.loc[null_indices, "customer_id"] = np.nan
    half_feed.to_csv(os.path.join(CORRUPT_DIR, "volume_freshness_anomalous_feed.csv"), index=False)

    # -------------------------------------------------------------
    # Project 4: Quarantine Pattern
    # "One bad row shouldn’t block 10M good ones"
    # -------------------------------------------------------------
    # Generate 5,000 Bronze records with ~3% corrupt records mixed in
    bronze_records = pd.concat([df_orders, df_orders], ignore_index=True).iloc[:5000].copy()
    # Reset order_id to unique for base
    bronze_records["order_id"] = [uuid.uuid4().hex[:32] for _ in range(len(bronze_records))]
    
    # Inject ~150 corrupt rows with distinct identifiable defects
    corrupt_indices = random.sample(range(len(bronze_records)), 150)
    for idx in corrupt_indices[:30]:
        bronze_records.loc[idx, "order_id"] = np.nan  # Null primary key
    for idx in corrupt_indices[30:70]:
        bronze_records.loc[idx, "order_status"] = "UNKNOWN_VENDOR_STATE"  # Invalid status enum
    for idx in corrupt_indices[70:110]:
        # Future timestamp
        bronze_records.loc[idx, "order_purchase_timestamp"] = (datetime.now() + timedelta(days=90)).strftime("%Y-%m-%d %H:%M:%S")
    for idx in corrupt_indices[110:150]:
        # Delivered before purchase
        p_date = datetime.strptime(bronze_records.loc[idx, "order_purchase_timestamp"], "%Y-%m-%d %H:%M:%S")
        bronze_records.loc[idx, "order_delivered_customer_date"] = (p_date - timedelta(days=10)).strftime("%Y-%m-%d %H:%M:%S")

    bronze_records.to_csv(os.path.join(CORRUPT_DIR, "quarantine_bronze_feed_5k.csv"), index=False)

    print("Targeted corruption datasets created successfully:")
    print(" - Project 1: orders_vendor_renamed_drift.csv")
    print(" - Project 1: orders_schema_invalid_status_shuffled.csv")
    print(" - Project 2: business_rules_corrupted_orders.csv & items.csv")
    print(" - Project 3: volume_freshness_anomalous_feed.csv")
    print(" - Project 4: quarantine_bronze_feed_5k.csv")


if __name__ == "__main__":
    create_corruptions()

