"""
Project 1: Schema Drift Validation Gate
Guards the Bronze landing layer against upstream contract breaking changes:
- Column set and strict column ordering
- Non-null constraints on primary and partition keys
- Enumerated allowed categorical values
- Data types & format invariants
"""

import os
import sys
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Tuple, List, Optional
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

import great_expectations as gx

console = Console()

# Canonical Schema Definition for Olist Orders Feed
EXPECTED_COLUMNS_ORDERED = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

ALLOWED_STATUSES = [
    "delivered",
    "shipped",
    "invoiced",
    "processing",
    "approved",
    "canceled",
    "unavailable",
]


class SchemaValidationGate:
    """
    Validation Gate that evaluates incoming feeds using Great Expectations
    before allowing data to land in the Bronze layer.
    """

    def __init__(self, bronze_landing_dir: str = "data/processed/bronze/orders"):
        self.bronze_landing_dir = Path(bronze_landing_dir)
        self.bronze_landing_dir.mkdir(parents=True, exist_ok=True)

    def build_suite(self, suite_name: str = "bronze_orders_schema_suite"):
        suite = gx.ExpectationSuite(name=suite_name)

        # 1. Exact Column Set and Order Invariant
        suite.add_expectation(
            gx.expectations.ExpectTableColumnsToMatchOrderedList(
                column_list=EXPECTED_COLUMNS_ORDERED
            )
        )

        # 2. Required Non-Null Columns
        for col in ["order_id", "customer_id", "order_status", "order_purchase_timestamp"]:
            suite.add_expectation(
                gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
            )

        # 3. Categorical Domain Check
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeInSet(
                column="order_status",
                value_set=ALLOWED_STATUSES,
            )
        )

        # 4. Timestamp format pattern check (YYYY-MM-DD HH:MM:SS)
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToMatchRegex(
                column="order_purchase_timestamp",
                regex=r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$",
            )
        )

        return suite

    def validate_feed(
        self,
        file_path: str,
        enforce_order: bool = True,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Validates an incoming CSV file against the canonical schema.
        Returns: (success: bool, detailed_report: dict)
        """
        input_path = Path(file_path)
        if not input_path.exists():
            raise FileNotFoundError(f"Input feed file not found: {file_path}")

        df = pd.read_csv(input_path)
        actual_columns = list(df.columns)

        # Quick structural pre-checks to provide pinpoint diagnostics
        missing_cols = [c for c in EXPECTED_COLUMNS_ORDERED if c not in actual_columns]
        extra_cols = [c for c in actual_columns if c not in EXPECTED_COLUMNS_ORDERED]

        # Connect DataFrame into Great Expectations Ephemeral Data Source
        context = gx.get_context(mode="ephemeral")
        data_source = context.data_sources.add_pandas("incoming_feed_source")
        data_asset = data_source.add_dataframe_asset("orders_batch_asset")
        batch_def = data_asset.add_batch_definition_whole_dataframe("batch_def")

        suite = self.build_suite()
        context.suites.add(suite)

        validation_definition = gx.ValidationDefinition(
            data=batch_def,
            suite=suite,
            name="schema_gate_validation",
        )
        context.validation_definitions.add(validation_definition)

        batch_params = {"dataframe": df}
        validation_result = validation_definition.run(batch_parameters=batch_params)

        # Extract failed expectations
        failures = []
        for result in validation_result.results:
            if not result.success:
                exp_type = result.expectation_config.type
                kwargs = result.expectation_config.kwargs
                res_details = result.result
                failures.append({
                    "expectation": exp_type,
                    "details": kwargs,
                    "observed": res_details,
                })

        is_valid = validation_result.success

        report = {
            "timestamp": datetime.now().isoformat(),
            "input_file": str(input_path),
            "rows_evaluated": len(df),
            "is_valid": is_valid,
            "actual_columns": actual_columns,
            "expected_columns": EXPECTED_COLUMNS_ORDERED,
            "missing_columns": missing_cols,
            "extra_columns": extra_cols,
            "failures": failures,
        }

        return is_valid, report

    def process_feed(self, file_path: str) -> bool:
        """
        Executes validation gate. If valid, ingests to Bronze.
        If invalid, blocks pipeline with structured alert and exits with failure.
        """
        input_path = Path(file_path)
        console.print(Panel(f"[bold cyan]SCHEMA VALIDATION GATE[/bold cyan]\nInspecting: {input_path.name}", expand=False))

        is_valid, report = self.validate_feed(file_path)

        if is_valid:
            target_path = self.bronze_landing_dir / f"bronze_{input_path.name}"
            shutil.copy(input_path, target_path)
            console.print(f"[bold green]✔ GATE PASSED:[/bold green] Feed adheres 100% to schema contract.")
            console.print(f" -> Ingested {report['rows_evaluated']} clean rows to Bronze: [yellow]{target_path}[/yellow]\n")
            return True
        else:
            console.print(f"[bold red]✖ GATE BREACH DETECTED:[/bold red] Upstream feed violated schema contract!\nPipeline halted immediately to prevent Bronze corruption.\n")

            # Diagnostic Table
            table = Table(title="Schema Drift Analysis", show_lines=True)
            table.add_column("Category", style="cyan", no_wrap=True)
            table.add_column("Details", style="magenta")

            if report["missing_columns"]:
                table.add_row("Missing Required Columns", f"[red]{report['missing_columns']}[/red] (Likely renamed upstream!)")
            if report["extra_columns"]:
                table.add_row("Unexpected Extra Columns", f"[yellow]{report['extra_columns']}[/yellow] (Unannounced vendor payload)")
            
            for f in report["failures"]:
                exp_name = f["expectation"]
                col = f["details"].get("column", "table-level")
                table.add_row(f"Failed Check: {exp_name}", f"Target: [bold]{col}[/bold]\nObserved: {f['observed']}")

            console.print(table)

            # Dump Incident Report
            report_file = self.bronze_landing_dir.parent / f"incident_drift_{input_path.stem}.json"
            with open(report_file, "w") as rf:
                json.dump(report, rf, indent=2, default=str)
            console.print(f"\n[red]Incident report saved to: {report_file}[/red]\n")
            return False


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python schema_gate.py <path_to_csv>")
        sys.exit(1)

    gate = SchemaValidationGate()
    success = gate.process_feed(sys.argv[1])
    sys.exit(0 if success else 1)
