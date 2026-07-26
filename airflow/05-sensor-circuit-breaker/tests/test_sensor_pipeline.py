"""
tests/test_sensor_pipeline.py — Unit Tests for Sensor & Circuit Breaker Pipeline
"""

import sys
import tempfile
import json
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Purge any cached colliding module names from sibling test suites
for mod in ["warehouse_loader", "sensor", "circuit_breaker", "order_processor", "file_producer"]:
    sys.modules.pop(mod, None)

sys.path.insert(0, str(PROJECT_ROOT / "src"))

from file_producer import produce, write_marker
from sensor import SuccessMarkerSensor
from circuit_breaker import CircuitBreaker
from order_processor import process, compute_checksum
from warehouse_loader import load


def test_producer_creates_csv_and_marker():
    """Verify producer creates both data file and companion _SUCCESS marker."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        res = produce("2024-01-15", output_dir=tmp_dir, row_count=50)
        marker_path = write_marker("2024-01-15", metadata=res, output_dir=tmp_dir)

        assert Path(res["file_path"]).exists()
        assert Path(marker_path).exists()
        assert res["row_count"] == 50


def test_sensor_finds_existing_marker():
    """Verify sensor immediately locates existing marker file."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        res = produce("2024-01-15", output_dir=tmp_dir, row_count=20)
        write_marker("2024-01-15", metadata=res, output_dir=tmp_dir)

        sensor = SuccessMarkerSensor(landing_dir=tmp_dir)
        poll_res = sensor.poll("2024-01-15", timeout_seconds=5)

        assert poll_res["found"] is True
        assert poll_res["timed_out"] is False


def test_sensor_times_out_on_missing_marker():
    """Verify sensor flags timeout when marker file does not appear within budget."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        sensor = SuccessMarkerSensor(landing_dir=tmp_dir)
        poll_res = sensor.poll("2024-01-15", timeout_seconds=1, poke_interval=1)

        assert poll_res["found"] is False
        assert poll_res["timed_out"] is True


def test_circuit_breaker_evaluates_success():
    """Verify breaker routes to process_orders when sensor succeeds."""
    cb = CircuitBreaker()
    branch = cb.evaluate_sensor_result({"found": True, "timed_out": False})
    assert branch == "process_orders"


def test_circuit_breaker_evaluates_timeout():
    """Verify breaker routes to fire_circuit_breaker branch upon sensor timeout."""
    cb = CircuitBreaker()
    branch = cb.evaluate_sensor_result({"found": False, "timed_out": True})
    assert branch == "fire_circuit_breaker"


def test_circuit_breaker_trip_logs_event():
    """Verify circuit breaker persists audit log entry when tripped."""
    with tempfile.NamedTemporaryFile(suffix=".json") as tmp_json:
        cb = CircuitBreaker(log_path=Path(tmp_json.name))
        incident = cb.trip("2024-01-15", reason="SENSOR_TIMEOUT")

        assert incident["event"] == "CIRCUIT_BREAKER_TRIPPED"
        assert incident["state"] == "OPEN"

        history = cb.get_trip_history()
        assert len(history) == 1
        assert history[0]["logical_date"] == "2024-01-15"


def test_order_processor_validates_checksum():
    """Verify processor validates companion CSV against marker checksum."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        res = produce("2024-01-15", output_dir=tmp_dir, row_count=30)
        write_marker("2024-01-15", metadata=res, output_dir=tmp_dir)

        processed = process("2024-01-15", landing_dir=tmp_dir)
        assert processed["file_verified"] is True
        assert processed["checksum_verified"] is True
        assert processed["row_count"] == 30

        # Now simulate tampering with the CSV file
        csv_file = Path(tmp_dir) / "orders_2024-01-15.csv"
        with open(csv_file, "a", encoding="utf-8") as f:
            f.write("TAMPERED_ROW,x,x,x,x,0,0,x,\n")

        tampered = process("2024-01-15", landing_dir=tmp_dir)
        assert tampered["checksum_verified"] is False
        assert tampered["file_verified"] is False
        assert tampered["status"] == "CORRUPTED"


def test_warehouse_loader_commits_records():
    """Verify sensor pipeline orders load cleanly into warehouse SQLite."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp_db:
        records = [
            {"order_id": "ord_1", "customer_id": "c1", "order_status": "delivered", "payment_value": 99.0},
            {"order_id": "ord_2", "customer_id": "c2", "order_status": "shipped", "payment_value": 150.0},
        ]
        res = load("2024-01-15", records, db_path=Path(tmp_db.name))
        assert res["rows_loaded"] == 2
        assert res["partition_total"] == 2
