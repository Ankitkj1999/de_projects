"""
Project 2: Business Rules Validator Engine
Validates complex semantic logic and cross-table financial reconciliations.
"""

import sys
from pathlib import Path
from typing import Dict, Any, Tuple, List
import pandas as pd
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

import great_expectations as gx

# Local suite builder
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from suites.ecommerce_rules import build_orders_business_suite, build_order_items_business_suite

console = Console()


class BusinessRulesValidator:
    """
    Evaluates business domain rules and financial invariants across tables.
    """

    def __init__(self, catalog_products_path: str = "data/raw/olist_products_dataset.csv"):
        self.catalog_df = pd.read_csv(catalog_products_path)
        self.catalog_skus = set(self.catalog_df["product_id"].unique())

    def validate_orders(self, orders_df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validates orders table against semantic rules (uniqueness, causality, dates)."""
        orders_eval = orders_df.copy()
        orders_eval["order_purchase_timestamp"] = pd.to_datetime(orders_eval["order_purchase_timestamp"], errors="coerce")
        orders_eval["order_delivered_customer_date"] = pd.to_datetime(orders_eval["order_delivered_customer_date"], errors="coerce")

        context = gx.get_context(mode="ephemeral")
        ds = context.data_sources.add_pandas("orders_ds")
        asset = ds.add_dataframe_asset("orders_asset")
        batch_def = asset.add_batch_definition_whole_dataframe("orders_batch_def")

        suite = build_orders_business_suite()
        context.suites.add(suite)

        val_def = gx.ValidationDefinition(data=batch_def, suite=suite, name="orders_val")
        context.validation_definitions.add(val_def)

        results = val_def.run(batch_parameters={"dataframe": orders_eval})

        failures = []
        for r in results.results:
            if not r.success:
                failures.append({
                    "expectation": r.expectation_config.type,
                    "target": r.expectation_config.kwargs.get("column") or f"{r.expectation_config.kwargs.get('column_A')} vs {r.expectation_config.kwargs.get('column_B')}",
                    "details": r.expectation_config.kwargs,
                    "result": r.result,
                })
        return results.success, failures

    def validate_items(self, items_df: pd.DataFrame) -> Tuple[bool, List[Dict[str, Any]]]:
        """Validates items table (prices > 0, freight >= 0, SKU in catalog)."""
        context = gx.get_context(mode="ephemeral")
        ds = context.data_sources.add_pandas("items_ds")
        asset = ds.add_dataframe_asset("items_asset")
        batch_def = asset.add_batch_definition_whole_dataframe("items_batch_def")

        suite = build_order_items_business_suite(catalog_skus=self.catalog_skus)
        context.suites.add(suite)

        val_def = gx.ValidationDefinition(data=batch_def, suite=suite, name="items_val")
        context.validation_definitions.add(val_def)

        results = val_def.run(batch_parameters={"dataframe": items_df})

        failures = []
        for r in results.results:
            if not r.success:
                failures.append({
                    "expectation": r.expectation_config.type,
                    "target": r.expectation_config.kwargs.get("column", "table"),
                    "details": r.expectation_config.kwargs,
                    "result": r.result,
                })
        return results.success, failures

    def validate_order_total_reconciliation(
        self,
        items_df: pd.DataFrame,
        payments_df: pd.DataFrame,
        tolerance_cents: float = 0.05
    ) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Business Rule: Order Total Integrity
        sum(item price + item freight) per order MUST match total payment value.
        """
        # Aggregate calculated order total from line items
        items_agg = items_df.groupby("order_id").apply(
            lambda g: round((g["price"] + g["freight_value"]).sum(), 2)
        ).reset_index(name="calculated_items_total")

        # Aggregate reported payment total
        payments_agg = payments_df.groupby("order_id")["payment_value"].sum().round(2).reset_index(name="reported_payment_total")

        reconciled = pd.merge(items_agg, payments_agg, on="order_id", how="inner")
        reconciled["difference"] = (reconciled["calculated_items_total"] - reconciled["reported_payment_total"]).abs().round(2)

        mismatched = reconciled[reconciled["difference"] > tolerance_cents]

        # Evaluate using GX on reconciled table
        context = gx.get_context(mode="ephemeral")
        ds = context.data_sources.add_pandas("reconciliation_ds")
        asset = ds.add_dataframe_asset("reconciliation_asset")
        batch_def = asset.add_batch_definition_whole_dataframe("recon_batch_def")

        suite = gx.ExpectationSuite(name="reconciliation_suite")
        # Difference must be <= tolerance
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column="difference",
                min_value=0.0,
                max_value=tolerance_cents,
            )
        )
        context.suites.add(suite)

        val_def = gx.ValidationDefinition(data=batch_def, suite=suite, name="recon_val")
        context.validation_definitions.add(val_def)

        results = val_def.run(batch_parameters={"dataframe": reconciled})

        failures = []
        if not results.success or len(mismatched) > 0:
            failures.append({
                "expectation": "expect_order_total_to_equal_line_items_sum",
                "target": "difference (calculated_items_total vs reported_payment_total)",
                "mismatched_orders_count": len(mismatched),
                "sample_mismatches": mismatched.head(5).to_dict(orient="records"),
            })

        return (len(mismatched) == 0), failures

    def run_full_validation(
        self,
        orders_df: pd.DataFrame,
        items_df: pd.DataFrame,
        payments_df: pd.DataFrame,
        dataset_label: str = "Batch Evaluation",
    ) -> bool:
        """Runs all business rules and prints executive summary report."""
        console.print(Panel(f"[bold cyan]BUSINESS RULES VALIDATION ENGINE[/bold cyan]\nEvaluating: {dataset_label}", expand=False))

        orders_pass, orders_fail = self.validate_orders(orders_df)
        items_pass, items_fail = self.validate_items(items_df)
        recon_pass, recon_fail = self.validate_order_total_reconciliation(items_df, payments_df)

        all_passed = orders_pass and items_pass and recon_pass

        table = Table(title=f"Business Rules Execution Results ({dataset_label})", show_lines=True)
        table.add_column("Rule / Expectation", style="cyan")
        table.add_column("Domain Target", style="magenta")
        table.add_column("Status", justify="center")
        table.add_column("Diagnostic Details")

        # Summarize Orders rules
        table.add_row(
            "Unique Order IDs\n[dim]expect_column_values_to_be_unique[/dim]",
            "orders.order_id",
            "[green]PASS[/green]" if not any(f["expectation"] == "expect_column_values_to_be_unique" for f in orders_fail) else "[red]FAIL[/red]",
            "No duplicate order IDs found" if not any(f["expectation"] == "expect_column_values_to_be_unique" for f in orders_fail) else "[red]Duplicate order IDs present![/red]"
        )
        table.add_row(
            "No Future Dates\n[dim]expect_column_values_to_be_between[/dim]",
            "orders.order_purchase_timestamp",
            "[green]PASS[/green]" if not any(f["expectation"] == "expect_column_values_to_be_between" for f in orders_fail) else "[red]FAIL[/red]",
            "All orders are past or present" if not any(f["expectation"] == "expect_column_values_to_be_between" for f in orders_fail) else "[red]Future-dated orders detected![/red]"
        )
        table.add_row(
            "Temporal Causality\n[dim]expect_column_pair_values_a_to_be_greater_than_b[/dim]",
            "delivery_date >= purchase_date",
            "[green]PASS[/green]" if not any("expect_column_pair_values" in f["expectation"] for f in orders_fail) else "[red]FAIL[/red]",
            "Causality valid" if not any("expect_column_pair_values" in f["expectation"] for f in orders_fail) else "[red]Delivered date before order purchase date![/red]"
        )

        # Summarize Items rules
        table.add_row(
            "Non-Negative Prices\n[dim]expect_column_values_to_be_between[/dim]",
            "order_items.price",
            "[green]PASS[/green]" if not any(f["target"] == "price" for f in items_fail) else "[red]FAIL[/red]",
            "All unit prices > 0" if not any(f["target"] == "price" for f in items_fail) else "[red]Negative or zero price detected![/red]"
        )
        table.add_row(
            "Catalog SKU Exists (FK)\n[dim]expect_column_values_to_be_in_set[/dim]",
            "order_items.product_id",
            "[green]PASS[/green]" if not any(f["target"] == "product_id" for f in items_fail) else "[red]FAIL[/red]",
            "All items exist in product catalog" if not any(f["target"] == "product_id" for f in items_fail) else "[red]Phantom SKUs found (referential integrity broken)![/red]"
        )

        # Summarize Reconciliation
        table.add_row(
            "Order Total = Sum of Items\n[dim]expect_order_total_to_equal_line_items_sum[/dim]",
            "payment_value == sum(price + freight)",
            "[green]PASS[/green]" if recon_pass else "[red]FAIL[/red]",
            "Order totals match itemized sums" if recon_pass else f"[red]{recon_fail[0]['mismatched_orders_count']} orders have financial discrepancy![/red]"
        )

        console.print(table)
        if all_passed:
            console.print("[bold green]✔ ALL 6 BUSINESS INVARIANTS SATISFIED.[/bold green]\n")
        else:
            console.print("[bold red]✖ BUSINESS RULE VIOLATIONS DETECTED: Dashboard corruption prevented.[/bold red]\n")

        return all_passed
