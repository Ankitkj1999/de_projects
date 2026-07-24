"""
Project 3: Volume & Freshness Anomaly Gate
Implements dynamic statistical volume checks, freshness SLAs, null tolerances (mostly=),
and multi-tier severity handling (HARD FAIL vs WARNING).
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, Tuple, List, Optional
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

import great_expectations as gx

# Local monitor
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from monitors.baseline_tracker import HistoricalBaselineTracker

console = Console()


class VolumeFreshnessGate:
    """
    Evaluates dynamic statistical volume anomalies, freshness SLAs, and tolerance-based null rates.
    """

    def __init__(self, tracker: Optional[HistoricalBaselineTracker] = None):
        self.tracker = tracker or HistoricalBaselineTracker()

    def evaluate_feed(
        self,
        df: pd.DataFrame,
        reference_time: Optional[datetime] = None,
        max_freshness_hours: int = 48,
    ) -> Dict[str, Any]:
        """
        Evaluates incoming batch against dynamic baselines and SLA tolerances.
        Categorizes violations into CRITICAL (Hard Fail) and WARNING (Soft Alert).
        """
        ref_time = reference_time or datetime.now()
        baseline = self.tracker.get_trailing_7day_baseline()
        row_count = len(df)

        df_eval = df.copy()
        df_eval["order_purchase_timestamp"] = pd.to_datetime(df_eval["order_purchase_timestamp"], errors="coerce")

        context = gx.get_context(mode="ephemeral")
        ds = context.data_sources.add_pandas("feed_ds")
        asset = ds.add_dataframe_asset("feed_asset")
        batch_def = asset.add_batch_definition_whole_dataframe("feed_batch_def")

        # -------------------------------------------------------------
        # 1. HARD FAIL CHECKS (Must halt if breached)
        # -------------------------------------------------------------
        hard_suite = gx.ExpectationSuite(name="hard_fail_suite")
        
        # Hard Rule 1: Row count within 3-sigma / 60% of trailing baseline
        hard_suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(
                min_value=baseline["hard_min"],
                max_value=baseline["hard_max"],
            )
        )

        # Hard Rule 2: Primary Key 100% non-null
        hard_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="order_id",
                mostly=1.0,
            )
        )

        # Hard Rule 3: Customer ID null rate tolerance (mostly=0.98 allows max 2% nulls)
        hard_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToNotBeNull(
                column="customer_id",
                mostly=0.98,
            )
        )

        # Hard Rule 4: Hard Freshness Ceiling (data must not be older than max_freshness_hours)
        hard_earliest = ref_time - timedelta(hours=max_freshness_hours)
        hard_suite.add_expectation(
            gx.expectations.ExpectColumnMaxToBeBetween(
                column="order_purchase_timestamp",
                min_value=hard_earliest,
                max_value=ref_time + timedelta(hours=1),
            )
        )

        # -------------------------------------------------------------
        # 2. WARNING CHECKS (Soft Alerts: log notification, run continues)
        # -------------------------------------------------------------
        warn_suite = gx.ExpectationSuite(name="warning_suite")

        # Warn Rule 1: Row count within tighter 1.8-sigma bound
        warn_suite.add_expectation(
            gx.expectations.ExpectTableRowCountToBeBetween(
                min_value=baseline["warn_min"],
                max_value=baseline["warn_max"],
            )
        )

        # Warn Rule 2: Optimal Freshness SLA (within 24 hours)
        warn_earliest = ref_time - timedelta(hours=24)
        warn_suite.add_expectation(
            gx.expectations.ExpectColumnMaxToBeBetween(
                column="order_purchase_timestamp",
                min_value=warn_earliest,
                max_value=ref_time + timedelta(hours=1),
            )
        )

        # Warn Rule 3: Optional field null rate tolerance (order_approved_at mostly=0.95)
        if "order_approved_at" in df_eval.columns:
            warn_suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(
                    column="order_approved_at",
                    mostly=0.95,
                )
            )

        context.suites.add(hard_suite)
        context.suites.add(warn_suite)

        hard_val = gx.ValidationDefinition(data=batch_def, suite=hard_suite, name="hard_val")
        warn_val = gx.ValidationDefinition(data=batch_def, suite=warn_suite, name="warn_val")
        context.validation_definitions.add(hard_val)
        context.validation_definitions.add(warn_val)

        hard_results = hard_val.run(batch_parameters={"dataframe": df_eval})
        warn_results = warn_val.run(batch_parameters={"dataframe": df_eval})

        hard_failures = []
        for r in hard_results.results:
            if not r.success:
                hard_failures.append({
                    "expectation": r.expectation_config.type,
                    "target": r.expectation_config.kwargs.get("column", "table-level"),
                    "details": r.expectation_config.kwargs,
                    "observed": r.result,
                })

        warn_failures = []
        for r in warn_results.results:
            if not r.success:
                warn_failures.append({
                    "expectation": r.expectation_config.type,
                    "target": r.expectation_config.kwargs.get("column", "table-level"),
                    "details": r.expectation_config.kwargs,
                    "observed": r.result,
                })

        # Determine Overall Status
        if len(hard_failures) > 0:
            status = "CRITICAL_FAILURE"
            should_halt = True
        elif len(warn_failures) > 0:
            status = "WARNING"
            should_halt = False
        else:
            status = "HEALTHY"
            should_halt = False

        max_ts = df_eval["order_purchase_timestamp"].max()

        report = {
            "status": status,
            "should_halt": should_halt,
            "row_count": row_count,
            "max_timestamp": str(max_ts),
            "baseline": baseline,
            "hard_failures": hard_failures,
            "warnings": warn_failures,
        }

        return report

    def render_report(self, report: Dict[str, Any], feed_name: str = "Feed Ingestion"):
        """Displays rich terminal diagnostic summary."""
        status = report["status"]
        if status == "HEALTHY":
            color = "green"
            badge = "✔ HEALTHY (ALL CHECKS PASSED)"
        elif status == "WARNING":
            color = "yellow"
            badge = "⚠ WARNING (SOFT ANOMALY DETECTED - PIPELINE CONTINUES)"
        else:
            color = "red"
            badge = "✖ CRITICAL FAILURE (HARD GATE BREACH - PIPELINE HALTED)"

        console.print(Panel(f"[bold {color}]{badge}[/bold {color}]\nTarget Feed: {feed_name}", expand=False))

        table = Table(title=f"Anomaly Inspection: {feed_name}", show_lines=True)
        table.add_column("Metric / Dimension", style="cyan")
        table.add_column("Observed Value", style="magenta")
        table.add_column("Baseline / SLA Bound", style="blue")
        table.add_column("Severity & Outcome", justify="center")

        # Row count
        b = report["baseline"]
        rc = report["row_count"]
        rc_severity = "[green]NORMAL[/green]"
        if rc < b["hard_min"] or rc > b["hard_max"]:
            rc_severity = "[red]CRITICAL[/red]"
        elif rc < b["warn_min"] or rc > b["warn_max"]:
            rc_severity = "[yellow]WARNING[/yellow]"

        table.add_row(
            "Batch Row Count",
            f"{rc:,} rows",
            f"Mean: {b['mean']:,} (Warn: [{b['warn_min']}, {b['warn_max']}] | Hard: [{b['hard_min']}, {b['hard_max']}])",
            rc_severity
        )

        table.add_row(
            "Max Freshness Timestamp",
            report["max_timestamp"],
            "SLA: Within past 24h (Warn) / 48h (Hard)",
            "[green]FRESH[/green]" if not any(f["expectation"] == "expect_column_max_to_be_between" for f in report["hard_failures"]) else "[red]STALE[/red]"
        )

        table.add_row(
            "Null Rate Tolerances",
            f"Hard Failures: {len(report['hard_failures'])}, Warnings: {len(report['warnings'])}",
            "mostly=0.98 (Customer ID), mostly=0.95 (Approval Date)",
            "[green]ACCEPTABLE[/green]" if len(report["hard_failures"]) == 0 and len(report["warnings"]) == 0 else rc_severity
        )

        console.print(table)
        console.print(f"[bold]Action Taken:[/bold] {'[red]PIPELINE ABORTED (Exit 1)[/red]' if report['should_halt'] else '[green]PIPELINE CONTINUED (Exit 0)[/green]'}\n")

