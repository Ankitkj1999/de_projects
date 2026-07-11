"""
tests/test_sla_pipeline.py — Unit Tests for SLA-Monitored Finance Pipeline
"""

import json
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Purge any cached colliding module names from sibling test suites
for mod in ["notifier", "extractor", "aggregator"]:
    sys.modules.pop(mod, None)

sys.path.insert(0, str(PROJECT_ROOT / "src"))

from freshness_checker import check_source_freshness, write_source_marker
from sales_extractor import extract
from currency_converter import convert
from aggregator import aggregate
from report_generator import generate
from sla_callbacks import simulate_sla_miss


def test_fresh_source_passes_check():
    """Verify source updated 2 hours ago passes 24h freshness SLA."""
    now = datetime.now(timezone.utc)
    fresh_time = now - timedelta(hours=2)

    res = check_source_freshness(source_timestamp=fresh_time, reference_time=now, max_age_hours=24)
    assert res["is_fresh"] is True
    assert res["status"] == "FRESH"
    assert res["age_hours"] == 2.0


def test_stale_source_fails_check():
    """Verify source updated 26 hours ago breaches 24h SLA and triggers ShortCircuit."""
    now = datetime.now(timezone.utc)
    stale_time = now - timedelta(hours=26)

    res = check_source_freshness(source_timestamp=stale_time, reference_time=now, max_age_hours=24)
    assert res["is_fresh"] is False
    assert res["status"] == "STALE"
    assert res["age_hours"] == 26.0


def test_extractor_returns_500_records():
    """Verify sales extractor outputs exactly 500 deterministic transactional records."""
    res = extract("2024-01-15")
    assert res["row_count"] == 500
    assert len(res["records"]) == 500

    sample = res["records"][0]
    for key in ["order_id", "product_id", "quantity", "unit_price_brl", "total_price_brl", "sales_channel"]:
        assert key in sample


def test_currency_conversion_correct():
    """Verify FX converter multiplies BRL totals by exchange rate into USD."""
    raw = [
        {"order_id": "ord_1", "total_price_brl": 100.0, "quantity": 1},
        {"order_id": "ord_2", "total_price_brl": 250.0, "quantity": 2},
    ]
    converted = convert(raw, fx_rate=0.20)
    assert len(converted) == 2
    assert converted[0]["total_price_usd"] == 20.0  # 100 * 0.20
    assert converted[1]["total_price_usd"] == 50.0  # 250 * 0.20
    assert converted[0]["to_currency"] == "USD"


def test_aggregator_computes_correct_totals():
    """Verify aggregator calculates total revenue, channels, and AOV correctly."""
    records = [
        {"total_price_brl": 100.0, "total_price_usd": 20.0, "quantity": 1, "sales_channel": "direct_web", "product_id": "p1", "product_name": "Chair"},
        {"total_price_brl": 200.0, "total_price_usd": 40.0, "quantity": 2, "sales_channel": "marketplace", "product_id": "p2", "product_name": "Desk"},
    ]
    summary = aggregate(records)
    assert summary["total_transactions"] == 2
    assert summary["total_revenue_brl"] == 300.0
    assert summary["total_revenue_usd"] == 60.0
    assert summary["average_order_value_usd"] == 30.0
    assert summary["revenue_by_channel"]["direct_web"] == 20.0
    assert summary["revenue_by_channel"]["marketplace"] == 40.0


def test_report_generator_creates_file():
    """Verify report publisher persists valid JSON artifact to disk."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        dummy_agg = {"total_revenue_usd": 50000.0, "total_transactions": 500}
        report_file = generate(dummy_agg, logical_date="2024-01-15", output_dir=tmp_dir)

        assert report_file.exists()
        with open(report_file, "r", encoding="utf-8") as f:
            data = json.load(f)
        assert data["reporting_date"] == "2024-01-15"
        assert data["financial_kpis"]["total_revenue_usd"] == 50000.0


def test_sla_miss_simulation_records_event():
    """Verify SLA breach recorder captures breached task and duration."""
    event = simulate_sla_miss(task_id="aggregate_revenue", elapsed_secs=720, sla_secs=600)
    assert event["event"] == "SLA_MISS"
    assert event["task_id"] == "aggregate_revenue"
    assert event["breach_amount_seconds"] == 120
