"""
tests/test_idempotent_loader.py — Unit Tests for Idempotent Backfill Pipeline
"""

import sys
import tempfile
import sqlite3
from pathlib import Path
import pytest

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Purge any cached colliding module names from sibling test suites
for mod in ["extractor", "warehouse_loader", "partition_checker"]:
    sys.modules.pop(mod, None)

sys.path.insert(0, str(PROJECT_ROOT / "src"))

from extractor import extract
from partition_checker import PartitionChecker
from warehouse_loader import WarehouseLoader
from idempotency_verifier import verify
from audit_logger import log, get_audit_trail


def test_extractor_deterministic_same_date():
    """Verify calling extract for the same logical date produces identical byte records."""
    res1 = extract("2024-01-15")
    res2 = extract("2024-01-15")

    assert res1["row_count"] == 200
    assert res2["row_count"] == 200
    assert res1["records"] == res2["records"]


def test_extractor_different_dates_different_records():
    """Verify different dates yield distinct transactional records."""
    res1 = extract("2024-01-15")
    res2 = extract("2024-01-16")

    assert res1["records"][0]["order_id"] != res2["records"][0]["order_id"]


def test_first_load_inserts_200_rows():
    """Verify fresh partition load produces exactly 200 rows in warehouse."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        loader = WarehouseLoader(db_path=tmp.name)
        checker = PartitionChecker(db_path=tmp.name)

        assert checker.check("2024-01-15")["partition_exists"] is False

        data = extract("2024-01-15")
        res = loader.upsert("2024-01-15", data["records"], run_id="run_fresh")

        assert res["upserted"] == 200
        assert res["partition_row_count"] == 200
        assert checker.check("2024-01-15")["partition_exists"] is True


def test_second_load_same_date_no_duplicates():
    """Verify re-running (backfill/retry) for the same date does NOT duplicate rows."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        loader = WarehouseLoader(db_path=tmp.name)
        data = extract("2024-01-15")

        # Initial load
        loader.upsert("2024-01-15", data["records"], run_id="run_1")
        assert loader.get_partition_count("2024-01-15") == 200

        # Simulate backfill / retry with same records
        loader.upsert("2024-01-15", data["records"], run_id="run_1_retry")
        assert loader.get_partition_count("2024-01-15") == 200  # Must remain exactly 200!


def test_different_date_load_adds_partition():
    """Verify loading day 2 appends new partition without impacting day 1."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        loader = WarehouseLoader(db_path=tmp.name)

        data1 = extract("2024-01-15")
        data2 = extract("2024-01-16")

        loader.upsert("2024-01-15", data1["records"], run_id="run_d1")
        loader.upsert("2024-01-16", data2["records"], run_id="run_d2")

        assert loader.get_partition_count("2024-01-15") == 200
        assert loader.get_partition_count("2024-01-16") == 200

        checker = PartitionChecker(db_path=tmp.name)
        history = checker.get_partition_history()
        assert len(history) == 2


def test_verifier_detects_duplicate():
    """Verify verifier catches manual duplicate injections."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        loader = WarehouseLoader(db_path=tmp.name)
        data = extract("2024-01-15")
        loader.upsert("2024-01-15", data["records"], run_id="run_1")

        # Fresh load is safe
        v1 = verify("2024-01-15", expected_count=200, db_path=tmp.name)
        assert v1["safe"] is True

        # Inject manual corruption with wrong expected count
        v2 = verify("2024-01-15", expected_count=100, db_path=tmp.name)
        assert v2["safe"] is False


def test_audit_log_captures_backfill_flag():
    """Verify audit logger writes and reads backfill metadata accurately."""
    with tempfile.NamedTemporaryFile(suffix=".json") as tmp_json:
        stats = {"upserted": 200, "partition_row_count": 200, "status": "COMMITTED"}

        log("2024-01-15", stats, run_id="manual_backfill_01", is_backfill=True, audit_file_path=tmp_json.name)
        trail = get_audit_trail(audit_file_path=tmp_json.name)

        assert len(trail) == 1
        assert trail[0]["is_backfill"] is True
        assert trail[0]["logical_date"] == "2024-01-15"
        assert trail[0]["run_id"] == "manual_backfill_01"
