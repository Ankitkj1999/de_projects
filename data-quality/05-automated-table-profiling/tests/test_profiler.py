"""
Unit and Integration Tests for Project 5: Profiler, Data Docs & Notifier
"""

import pytest
import os
import sys
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from profiler.auto_profiler import AutomatedTableProfiler
from profiler.suite_pruner import ExpectationSuitePruner
from src.data_docs_publisher import DataDocsPublisher
from src.notifier import QualityAlertNotifier

import great_expectations as gx

@pytest.fixture
def sample_df():
    return pd.DataFrame({
        "order_id": ["o1", "o2", "o3", "o4"],
        "status": ["delivered", "shipped", "delivered", "canceled"],
        "amount": [10.5, 25.0, 99.9, 14.2],
        "notes": ["ok", "pending", None, "done"],
    })

def test_profiler_discovers_schema_and_pk(sample_df):
    profiler = AutomatedTableProfiler()
    res = profiler.profile_dataframe(sample_df, "test_table")

    assert res["table_name"] == "test_table"
    assert res["row_count"] == 4
    assert res["columns"]["order_id"]["is_candidate_pk"] is True
    assert any(e["type"] == "expect_column_values_to_be_unique" for e in res["raw_expectations"])
    assert any(e["type"] == "expect_column_values_to_be_in_set" for e in res["raw_expectations"])

def test_pruner_filters_brittle_expectations(sample_df):
    profiler = AutomatedTableProfiler()
    res = profiler.profile_dataframe(sample_df, "test_table")

    pruner = ExpectationSuitePruner()
    suite, _ = pruner.prune_and_compile(res, min_confidence=0.85)

    # Brittle numeric range (confidence 0.70) should be pruned
    assert not any(e.expectation_type == "expect_column_values_to_be_between" for e in suite.expectations)
    # Primary key uniqueness (confidence 0.99) should be preserved
    assert any(e.expectation_type == "expect_column_values_to_be_unique" for e in suite.expectations)

def test_data_docs_publisher(tmp_path, sample_df):
    profiler = AutomatedTableProfiler()
    res = profiler.profile_dataframe(sample_df, "test_orders")
    pruner = ExpectationSuitePruner()
    suite, context = pruner.prune_and_compile(res)

    ds = context.data_sources.add_pandas("d")
    asset = ds.add_dataframe_asset("a")
    b = asset.add_batch_definition_whole_dataframe("b")
    val = gx.ValidationDefinition(data=b, suite=suite, name="v")
    context.validation_definitions.add(val)
    result = val.run(batch_parameters={"dataframe": sample_df})

    publisher = DataDocsPublisher(docs_dir=str(tmp_path / "docs"))
    doc_path = publisher.publish_report("test_orders", suite.name, "run_1", result)

    assert doc_path.exists()
    assert (tmp_path / "docs" / "index.html").exists()

def test_notifier_payload_generation():
    notifier = QualityAlertNotifier()
    slack_payload = notifier.send_alert(
        table_name="orders",
        run_id="run_101",
        total_checks=10,
        failed_checks=2,
        failures=[{"expectation": "expect_column_values_to_be_unique", "column": "order_id"}],
        docs_url="https://docs.mycompany.internal/orders.html",
        channel="slack",
        dry_run=True,
    )
    assert slack_payload["status"] == "DRY_RUN_EMITTED"
    assert "blocks" in slack_payload["payload"]

    cliq_payload = notifier.send_alert(
        table_name="orders",
        run_id="run_101",
        total_checks=10,
        failed_checks=2,
        failures=[{"expectation": "expect_column_values_to_be_unique", "column": "order_id"}],
        docs_url="https://docs.mycompany.internal/orders.html",
        channel="cliq",
        dry_run=True,
    )
    assert cliq_payload["status"] == "DRY_RUN_EMITTED"
    assert "card" in cliq_payload["payload"]
