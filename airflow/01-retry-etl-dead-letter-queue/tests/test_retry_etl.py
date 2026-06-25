"""
tests/test_retry_etl.py — Unit Tests for Retry ETL & Dead Letter Queue
"""

import sys
import tempfile
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from api_extractor import VendorAPIExtractor, extract_vendor_orders
from dead_letter_queue import DeadLetterQueue
from staging_loader import StagingLoader
from notifier import build_slack_payload, send_summary


def test_extractor_returns_valid_records():
    """Verify standard extraction returns 3 pages with valid structure."""
    extractor = VendorAPIExtractor()
    result = extractor.extract_vendor_orders(batch_date="2024-01-15", simulate_failure=False)

    assert "records" in result
    assert result["page_count"] == 3
    assert result["total_records"] > 0
    assert len(result["records"]) == result["total_records"]

    first = result["records"][0]
    for key in ["order_id", "customer_id", "order_status", "price", "order_purchase_timestamp"]:
        assert key in first


def test_extractor_raises_on_simulated_failure():
    """Verify simulate_failure triggers the API timeout exception."""
    extractor = VendorAPIExtractor()
    with pytest.raises(Exception) as exc_info:
        extractor.extract_vendor_orders(batch_date="2024-01-15", simulate_failure=True)
    assert "timed out" in str(exc_info.value).lower()


def test_dlq_push_and_retrieve():
    """Verify dead letter queue stores corrupted records and preserves metadata."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        dlq = DeadLetterQueue(db_path=tmp.name)

        corrupt_batch = [
            ({"order_id": None, "price": 100.0}, "NULL_PRIMARY_KEY"),
            ({"order_id": "ord_bad_price", "price": -50.0}, "NON_POSITIVE_PRICE"),
        ]

        inserted = dlq.push(corrupt_batch, dag_run_id="test_run_001")
        assert inserted == 2

        stats = dlq.get_stats()
        assert stats["total_quarantined"] == 2
        assert stats["failure_reasons"].get("NULL_PRIMARY_KEY") == 1
        assert stats["failure_reasons"].get("NON_POSITIVE_PRICE") == 1

        df = dlq.get_all()
        assert len(df) == 2
        assert "ord_bad_price" in df["order_id"].values


def test_staging_loader_upsert_idempotency():
    """Verify loading the same records twice does not duplicate rows."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        loader = StagingLoader(db_path=tmp.name)

        records = [
            {
                "order_id": "ord_1001",
                "customer_id": "cust_01",
                "order_status": "delivered",
                "price": 129.50,
                "order_purchase_timestamp": "2024-01-15 10:00:00",
            },
            {
                "order_id": "ord_1002",
                "customer_id": "cust_02",
                "order_status": "shipped",
                "price": 45.00,
                "order_purchase_timestamp": "2024-01-15 11:30:00",
            },
        ]

        res1 = loader.load(records, dag_run_id="run_1")
        assert res1["loaded"] == 2
        assert loader.count() == 2

        # Re-run same batch (simulate retry or backfill)
        res2 = loader.load(records, dag_run_id="run_1_retry")
        assert res2["loaded"] == 2
        assert loader.count() == 2  # Row count MUST NOT double


def test_notifier_builds_correct_payload():
    """Verify Slack Block Kit payload structure matches expected schema."""
    stats = {
        "dag_run_id": "prod_run_20240115",
        "batch_date": "2024-01-15",
        "total_extracted": 150,
        "valid_loaded": 143,
        "quarantined_count": 7,
        "dlq_reasons": {"NON_POSITIVE_PRICE": 5, "NULL_PRIMARY_KEY": 2},
        "retry_attempts": 1,
        "status": "SUCCESS",
    }

    payload = build_slack_payload(stats)
    assert payload["channel"] == "#de-alerts"
    assert "attachments" in payload
    assert len(payload["attachments"]) == 1

    blocks = payload["attachments"][0]["blocks"]
    header = blocks[0]["text"]["text"]
    assert "prod_run_20240115" in header


def test_end_to_end_validation_and_dlq_split():
    """Verify end-to-end split logic between valid staging and DLQ."""
    with tempfile.NamedTemporaryFile(suffix="_staging.db") as s_tmp, tempfile.NamedTemporaryFile(suffix="_dlq.db") as d_tmp:
        staging = StagingLoader(db_path=s_tmp.name)
        dlq = DeadLetterQueue(db_path=d_tmp.name)

        raw_batch = [
            {"order_id": "ord_ok_1", "customer_id": "c1", "order_status": "delivered", "price": 50.0, "order_purchase_timestamp": "2024-01-15 10:00:00"},
            {"order_id": "", "customer_id": "c2", "order_status": "delivered", "price": 75.0, "order_purchase_timestamp": "2024-01-15 10:05:00"},
            {"order_id": "ord_ok_2", "customer_id": "c3", "order_status": "delivered", "price": -10.0, "order_purchase_timestamp": "2024-01-15 10:10:00"},
        ]

        valid = []
        quarantine = []
        for r in raw_batch:
            if not r.get("order_id"):
                quarantine.append((r, "NULL_PRIMARY_KEY"))
            elif float(r.get("price", 0)) <= 0:
                quarantine.append((r, "NON_POSITIVE_PRICE"))
            else:
                valid.append(r)

        staging.load(valid, dag_run_id="test_run")
        dlq.push(quarantine, dag_run_id="test_run")

        assert staging.count() == 1
        assert dlq.get_stats()["total_quarantined"] == 2
