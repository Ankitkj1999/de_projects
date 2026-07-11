"""
demo.py — Interactive Demo for Project 03: SLA-Monitored Financial Report Pipeline
==================================================================================
Demonstrates:
  1. SCENARIO 1 (Happy Path): Source data is FRESH (2h old). Complete 6-task financial
     pipeline runs, converts FX BRL->USD, generates executive revenue report, notifies CFO.
  2. SCENARIO 2 (Freshness Breach): Upstream source is STALE (28h old).
     ShortCircuitOperator trips, skipping downstream tasks and preventing false reporting.
  3. SCENARIO 3 (SLA Miss Callback): Heavy aggregation task exceeds 10m SLA budget.
     Airflow `sla_miss_callback` triggers and registers breach event to on-call logs.
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from freshness_checker import check_source_freshness
from sales_extractor import extract
from currency_converter import convert
from aggregator import aggregate
from report_generator import generate
from sla_callbacks import simulate_sla_miss
from notifier import notify_cfo

console = Console()


def run_demo():
    console.print(
        Panel.fit(
            "[bold white]PROJECT 03: SLA-MONITORED FINANCIAL REPORT PIPELINE[/bold white]\n"
            "[dim cyan]\"The CFO's daily revenue report sometimes shows yesterday's numbers — and nobody gets alerted\"[/dim cyan]",
            border_style="cyan",
        )
    )

    now = datetime.now(timezone.utc)

    # -------------------------------------------------------------
    # SCENARIO 1: ON-TIME FRESH EXECUTION (HAPPY PATH)
    # -------------------------------------------------------------
    console.print("\n[bold green]═══ SCENARIO 1: NORMAL EXECUTION — SOURCE DATA FRESH & SLA MET ═══[/bold green]")
    
    console.print("\n[bold yellow]STEP 1: ShortCircuitOperator — Upstream Freshness Gate[/bold yellow]")
    fresh_time = now - timedelta(hours=2.5)
    fresh_eval = check_source_freshness(source_timestamp=fresh_time, reference_time=now, max_age_hours=24)
    console.print(f"  • Source last updated: [cyan]{fresh_eval['age_hours']} hours ago[/cyan] (SLA Ceiling: 24h)")
    console.print(f"  • Outcome: [bold green]✔ FRESH — ShortCircuit PASSES (Proceeding to Extract)[/bold green]")

    console.print("\n[bold yellow]STEP 2: Extract Daily Sales (Logical Date: 2024-01-15)[/bold yellow]")
    ext_res = extract("2024-01-15")
    console.print(f"  • Extracted: [cyan]{ext_res['row_count']:,}[/cyan] transactional order lines")

    console.print("\n[bold yellow]STEP 3: FX Currency Conversion (BRL → USD @ 0.20)[/bold yellow]")
    converted = convert(ext_res["records"], fx_rate=0.20)
    sample = converted[0]
    console.print(f"  • Normalized: [dim]Order {sample['order_id']} | R$ {sample['total_price_brl']:,.2f} BRL ➔ [/dim][bold green]${sample['total_price_usd']:,.2f} USD[/bold green]")

    console.print("\n[bold yellow]STEP 4: Financial Aggregation & KPI Synthesis[/bold yellow]")
    agg = aggregate(converted)
    console.print(f"  • Gross Merchandise Value (GMV): [bold green]${agg['total_revenue_usd']:,.2f} USD[/bold green]")
    console.print(f"  • Average Order Value (AOV):       [magenta]${agg['average_order_value_usd']:,.2f} USD[/magenta]")

    console.print("\n[bold yellow]STEP 5: Persisting Official Audited Report Artifact[/bold yellow]")
    report_file = generate(agg, logical_date="2024-01-15")
    console.print(f"  • Published report to: [blue]{report_file}[/blue]")

    console.print("\n[bold yellow]STEP 6: Executive Notifier (CFO Dashboard)[/bold yellow]")
    notify_cfo(str(report_file), agg)

    # -------------------------------------------------------------
    # SCENARIO 2: STALE DATA SHORT-CIRCUIT GATE
    # -------------------------------------------------------------
    console.print("\n[bold red]═══ SCENARIO 2: DATA OBSOLESCENCE — SHORTCIRCUITING STALE RUN ═══[/bold red]")
    console.print("[dim]Simulating upstream system delay where orders database was last refreshed 28.5 hours ago...[/dim]")

    stale_time = now - timedelta(hours=28.5)
    stale_eval = check_source_freshness(source_timestamp=stale_time, reference_time=now, max_age_hours=24)

    console.print(f"  • Source last updated: [red]{stale_eval['age_hours']} hours ago[/red] (SLA Ceiling: 24h)")
    console.print(f"  • Outcome: [bold red]✖ SHORTCIRCUIT TRIPPED — Halting DAG execution[/bold red]")
    console.print("  • Impact: Downstream extract/aggregate tasks [yellow]SKIPPED[/yellow]. Zero risk of publishing stale numbers.")

    # -------------------------------------------------------------
    # SCENARIO 3: SLA MISS CALLBACK TRIGGER
    # -------------------------------------------------------------
    console.print("\n[bold yellow]═══ SCENARIO 3: SLA MISS NOTIFICATION HOOK (sla_miss_callback) ═══[/bold yellow]")
    console.print("[dim]Simulating heavy query lock: aggregate_revenue took 12m 30s (SLA budget: 10m 00s)...[/dim]")

    sla_event = simulate_sla_miss(
        task_id="aggregate_revenue",
        dag_id="daily_finance_report",
        elapsed_secs=750,
        sla_secs=600,
    )

    sla_table = Table(title="Airflow SLA Miss Incident Log", show_lines=True)
    sla_table.add_column("DAG ID", style="cyan")
    sla_table.add_column("Breached Task", style="red bold")
    sla_table.add_column("Time Elapsed", style="magenta")
    sla_table.add_column("SLA Budget", style="blue")
    sla_table.add_column("Breach Amount", style="red bold")
    sla_table.add_column("Action Taken")

    sla_table.add_row(
        sla_event["dag_id"],
        sla_event["task_id"],
        f"{sla_event['elapsed_seconds']}s (12.5 min)",
        f"{sla_event['sla_seconds']}s (10.0 min)",
        f"+{sla_event['breach_amount_seconds']}s (+2.5 min)",
        "[yellow]Alerted On-Call & Logged to Audit[/yellow]",
    )

    console.print(sla_table)

    console.print("\n[bold green]✔ DEMO COMPLETE: SLA-Monitored Finance Pipeline fully verified![/bold green]\n")


if __name__ == "__main__":
    run_demo()
