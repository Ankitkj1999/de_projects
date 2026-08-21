"""
Demo Script: Project 5 - Cold Start Table Profiler, Data Docs & Slack/Cliq Alerts
Demonstrates:
1. Profiling an inherited legacy table with 0 existing tests.
2. Pruning brittle candidate expectations into a resilient production suite.
3. Executing validation.
4. Publishing standalone HTML Data Docs.
5. Emitting Slack and Zoho Cliq webhook alerts.
"""

import sys
import os
import uuid
from pathlib import Path
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

sys.path.insert(0, str(Path(__file__).resolve().parent))
from profiler.auto_profiler import AutomatedTableProfiler
from profiler.suite_pruner import ExpectationSuitePruner
from src.data_docs_publisher import DataDocsPublisher
from src.notifier import QualityAlertNotifier

import great_expectations as gx

console = Console()

def run_demo():
    console.print(Panel("[bold cyan]PROJECT 5: COLD START PROFILING, DATA DOCS & ALERTING[/bold cyan]", expand=False))

    orders_path = "data/raw/olist_orders_dataset.csv"
    df_orders = pd.read_csv(orders_path)

    # 1. Profile Table
    print("\n" + "=" * 75)
    print("STEP 1: PROFILING UNTESTED LEGACY TABLE (olist_orders_dataset)")
    print("=" * 75)
    profiler = AutomatedTableProfiler()
    profile = profiler.profile_dataframe(df_orders, table_name="olist_orders")

    print(f"Scanned Table: {profile['table_name']} ({profile['row_count']:,} rows)")
    print(f"Generated {len(profile['raw_expectations'])} candidate expectations across {len(profile['columns'])} columns.")

    # 2. Prune Suite
    print("\n" + "=" * 75)
    print("STEP 2: PRUNING BRITTLE RULES & COMPILING PRODUCTION SUITE")
    print("=" * 75)
    pruner = ExpectationSuitePruner()
    curated_suite, context = pruner.prune_and_compile(profile, min_confidence=0.85)

    print(f"Compiled Curated Suite: '{curated_suite.name}' with {len(curated_suite.expectations)} resilient expectations.")

    # 3. Validate Batch
    print("\n" + "=" * 75)
    print("STEP 3: RUNNING VALIDATION DEFINITION WITH GREAT EXPECTATIONS")
    print("=" * 75)
    ds = context.data_sources.add_pandas("profiler_ds")
    asset = ds.add_dataframe_asset("profiler_asset")
    bdef = asset.add_batch_definition_whole_dataframe("profiler_batch_def")

    val_def = gx.ValidationDefinition(data=bdef, suite=curated_suite, name="curated_orders_val")
    context.validation_definitions.add(val_def)

    val_result = val_def.run(batch_parameters={"dataframe": df_orders})
    print(f"Validation Status: {'SUCCESS' if val_result.success else 'FAILED'}")

    # 4. Generate & Publish Data Docs
    print("\n" + "=" * 75)
    print("STEP 4: PUBLISHING STANDALONE HTML DATA DOCS (S3 / GITHUB PAGES READY)")
    print("=" * 75)
    publisher = DataDocsPublisher()
    run_id = f"run_{uuid.uuid4().hex[:8]}"
    docs_path = publisher.publish_report(
        table_name="olist_orders",
        suite_name=curated_suite.name,
        run_id=run_id,
        validation_result=val_result
    )
    print(f"✔ Data Docs site published to: {docs_path}")
    print(f"✔ Latest index page available at: {docs_path.parent / 'index.html'}")

    # 5. Send Multi-Channel Notifications (Slack & Cliq)
    print("\n" + "=" * 75)
    print("STEP 5: DISPATCHING SLACK & ZOHO CLIQ ALERT WEBHOOKS")
    print("=" * 75)
    notifier = QualityAlertNotifier()
    failures = [
        {"expectation": r.expectation_config.type, "column": r.expectation_config.kwargs.get("column", "table")}
        for r in val_result.results if not r.success
    ]
    docs_url = f"file://{docs_path.resolve()}"

    slack_alert = notifier.send_alert(
        table_name="olist_orders",
        run_id=run_id,
        total_checks=len(val_result.results),
        failed_checks=len(failures),
        failures=failures,
        docs_url=docs_url,
        channel="slack",
        dry_run=True,
    )
    print(f"Dispatched Slack Notification: [Status: {slack_alert['status']}]")

    cliq_alert = notifier.send_alert(
        table_name="olist_orders",
        run_id=run_id,
        total_checks=len(val_result.results),
        failed_checks=len(failures),
        failures=failures,
        docs_url=docs_url,
        channel="cliq",
        dry_run=True,
    )
    print(f"Dispatched Zoho Cliq Notification: [Status: {cliq_alert['status']}]")

    print("\n" + "=" * 75)
    print("DEMO COMPLETE: Cold-start profiling, Data Docs & alerting demonstrated!")
    print("=" * 75)

if __name__ == "__main__":
    run_demo()
