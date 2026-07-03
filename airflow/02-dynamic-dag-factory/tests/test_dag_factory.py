"""
tests/test_dag_factory.py — Unit Tests for Dynamic Multi-Tenant DAG Factory
"""

import sys
import tempfile
from pathlib import Path
import pytest
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = PROJECT_ROOT / "config" / "clients.yaml"

# Purge any cached colliding module names from sibling test suites
for mod in ["notifier", "validator", "loader", "dag_factory"]:
    sys.modules.pop(mod, None)

sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dag_factory import ClientDAGFactory
from client_extractor import extract
from validator import validate
from loader import load, query_client_table


def test_config_loads_all_4_clients():
    """Verify that clients.yaml parses correctly and loads all 4 tenants."""
    factory = ClientDAGFactory(CONFIG_PATH)
    configs = factory.load_configs()
    assert len(configs) == 4
    ids = [c["client_id"] for c in configs]
    assert "acme_retail" in ids
    assert "globex_ecom" in ids
    assert "initech_shop" in ids
    assert "umbrella_mart" in ids


def test_factory_creates_dag_with_correct_id():
    """Verify DAG factory assigns standardized etl_{client_id} naming convention."""
    factory = ClientDAGFactory(CONFIG_PATH)
    dags = factory.generate_all_dags()
    assert len(dags) == 4
    assert "etl_acme_retail" in dags
    assert "etl_globex_ecom" in dags
    assert "etl_initech_shop" in dags
    assert "etl_umbrella_mart" in dags


def test_extractor_generates_correct_row_counts():
    """Verify each client receives tailored reproducible order volume."""
    factory = ClientDAGFactory(CONFIG_PATH)
    configs = {c["client_id"]: c for c in factory.load_configs()}

    acme_res = extract(configs["acme_retail"])
    assert acme_res["record_count"] == 150
    assert len(acme_res["records"]) == 150

    globex_res = extract(configs["globex_ecom"])
    assert globex_res["record_count"] == 600

    initech_res = extract(configs["initech_shop"])
    assert initech_res["record_count"] == 80


def test_validator_fails_below_threshold():
    """Verify validator catches batches below contractual minimum row thresholds."""
    client_cfg = {
        "client_id": "test_client",
        "min_row_threshold": 100,
        "max_price_ceiling": 500.0,
    }

    # Under-threshold batch (2 records vs min 100)
    small_batch = [
        {"order_id": "ord_1", "price": 50.0},
        {"order_id": "ord_2", "price": 40.0},
    ]

    res = validate(client_cfg, small_batch)
    assert res["passed"] is False
    assert res["status"] == "FAILED"
    assert any(v["rule"] == "MIN_ROW_THRESHOLD_BREACH" for v in res["violations"])


def test_validator_passes_healthy_batch():
    """Verify healthy batch passes validation with zero violations."""
    client_cfg = {
        "client_id": "test_client",
        "min_row_threshold": 2,
        "max_price_ceiling": 500.0,
    }

    healthy_batch = [
        {"order_id": "ord_101", "price": 50.0},
        {"order_id": "ord_102", "price": 99.0},
        {"order_id": "ord_103", "price": 149.0},
    ]

    res = validate(client_cfg, healthy_batch)
    assert res["passed"] is True
    assert res["critical_count"] == 0


def test_loader_upsert_is_idempotent():
    """Verify loading identical batch twice maintains identical table row count."""
    with tempfile.NamedTemporaryFile(suffix=".db") as tmp:
        db_path = Path(tmp.name)
        client_cfg = {
            "client_id": "acme_retail",
            "target_table": "test_acme_orders",
        }

        records = [
            {"order_id": "ord_01", "client_id": "acme", "customer_id": "c1", "order_status": "delivered", "price": 50.0, "order_date": "2024-01-15"},
            {"order_id": "ord_02", "client_id": "acme", "customer_id": "c2", "order_status": "shipped", "price": 80.0, "order_date": "2024-01-15"},
        ]

        res1 = load(client_cfg, records, db_path=db_path)
        assert res1["total_in_table"] == 2

        # Re-run same batch (simulate backfill / retry)
        res2 = load(client_cfg, records, db_path=db_path)
        assert res2["total_in_table"] == 2  # Must not become 4!


def test_onboarding_new_client_via_yaml_only():
    """Verify adding a 5th tenant to YAML automatically generates a 5th DAG without code changes."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as tmp_yaml:
        yaml_content = """
clients:
  - client_id: "client_alpha"
    display_name: "Alpha"
    schedule: "@daily"
    source_path: "data/alpha.csv"
    target_table: "orders_alpha"
  - client_id: "client_beta"
    display_name: "Beta"
    schedule: "@hourly"
    source_path: "data/beta.csv"
    target_table: "orders_beta"
"""
        tmp_yaml.write(yaml_content)
        tmp_yaml.flush()

        factory = ClientDAGFactory(tmp_yaml.name)
        dags = factory.generate_all_dags()

        assert len(dags) == 2
        assert "etl_client_alpha" in dags
        assert "etl_client_beta" in dags
