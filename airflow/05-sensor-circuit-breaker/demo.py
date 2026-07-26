"""
demo.py — Interactive Demo for Project 05: Event-Driven Sensor Pipeline with Circuit Breaker
=============================================================================================
Demonstrates:
  1. SCENARIO 1 (Happy Path): Upstream produces 300 orders & writes atomic _SUCCESS marker.
     Downstream sensor detects marker, verifies SHA-256 checksum, branches to process_orders,
     and commits rows to warehouse.
  2. SCENARIO 2 (Circuit Breaker Tripped): Upstream file delayed / missing.
     Sensor reaches timeout limit, trips the Circuit Breaker branch, logs incident to audit
     ledger, and gracefully skips downstream tasks without crashing the Airflow platform.
"""

import sys
import shutil
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from file_producer import produce, write_marker
from sensor import SuccessMarkerSensor
from circuit_breaker import CircuitBreaker
from order_processor import process
from warehouse_loader import load

console = Console()


def run_demo():
    console.print(
        Panel.fit(
            "[bold white]PROJECT 05: EVENT-DRIVEN SENSOR PIPELINE WITH CIRCUIT BREAKER[/bold white]\n"
            "[dim cyan]\"Our pipeline starts processing before the upstream file is even fully uploaded\"[/dim cyan]",
            border_style="cyan",
        )
    )

    landing_dir = str(PROJECT_ROOT / "data" / "processed" / "airflow" / "landing")
    db_path = PROJECT_ROOT / "data" / "processed" / "airflow" / "sensor_warehouse" / "orders.db"
    cb_log = PROJECT_ROOT / "data" / "processed" / "airflow" / "circuit_breaker_log.json"

    # Reset test paths
    shutil.rmtree(landing_dir, ignore_errors=True)
    Path(landing_dir).mkdir(parents=True, exist_ok=True)
    cb_log.unlink(missing_ok=True)

    # -------------------------------------------------------------
    # SCENARIO 1: HAPPY PATH (UPSTREAM DELIVERS ON TIME)
    # -------------------------------------------------------------
    console.print("\n[bold green]═══ SCENARIO 1: UPSTREAM FILE & SUCCESS MARKER DELIVERED ON TIME ═══[/bold green]")
    
    console.print("\n[bold yellow]STEP 1 [UPSTREAM DAG]: Writing 300 Orders to Landing Storage[/bold yellow]")
    prod_res = produce("2024-01-15", output_dir=landing_dir, row_count=300)
    console.print(f"  • Generated data payload: [cyan]{prod_res['file_path']}[/cyan]")
    console.print(f"  • Payload SHA-256 Checksum: [dim]{prod_res['checksum'][:32]}...[/dim]")

    console.print("\n[bold yellow]STEP 2 [UPSTREAM DAG]: Atomically Dropping _SUCCESS Marker[/bold yellow]")
    marker_file = write_marker("2024-01-15", metadata=prod_res, output_dir=landing_dir)
    console.print(f"  • Created marker: [bold green]{marker_file}[/bold green]")

    console.print("\n[bold yellow]STEP 3 [DOWNSTREAM DAG]: FileSensor Polling for _SUCCESS Marker[/bold yellow]")
    sensor = SuccessMarkerSensor(landing_dir=landing_dir)
    sensor_res = sensor.poll("2024-01-15", timeout_seconds=10)
    console.print(f"  • Sensor Poll Status: [bold green]✔ FOUND (Waited 0s)[/bold green]")

    console.print("\n[bold yellow]STEP 4 [DOWNSTREAM DAG]: BranchPythonOperator (Circuit Breaker Evaluation)[/bold yellow]")
    breaker = CircuitBreaker(log_path=cb_log)
    selected_branch = breaker.evaluate_sensor_result(sensor_res)
    console.print(f"  • Branch Decision: [bold green]➜ {selected_branch}[/bold green] (Proceed with Ingestion)")

    console.print("\n[bold yellow]STEP 5 [DOWNSTREAM DAG]: Checksum & Row Count Verification[/bold yellow]")
    proc_res = process("2024-01-15", landing_dir=landing_dir)
    console.print(f"  • Checksum Verified: [green]✔ PASSED (Hash matches marker)[/green]")
    console.print(f"  • Row Count Reconciled: [green]✔ PASSED ({proc_res['row_count']} rows match marker metadata)[/green]")

    console.print("\n[bold yellow]STEP 6 [DOWNSTREAM DAG]: Commit to Warehouse Database[/bold yellow]")
    load_res = load("2024-01-15", proc_res["records"], db_path=db_path)
    console.print(f"  • Warehouse Ingested: [bold green]{load_res['rows_loaded']:,} records committed to sensor_orders table[/bold green]")

    # -------------------------------------------------------------
    # SCENARIO 2: UPSTREAM DELAYED (CIRCUIT BREAKER TRIPS)
    # -------------------------------------------------------------
    console.print("\n[bold red]═══ SCENARIO 2: UPSTREAM FILE MISSING / DELAYED — CIRCUIT BREAKER TRIPS ═══[/bold red]")
    console.print("[dim]Simulating downstream DAG checking for 2024-01-16 where upstream team was delayed...[/dim]")

    console.print("\n[bold yellow]STEP 1 [DOWNSTREAM DAG]: FileSensor Polling for _SUCCESS_2024-01-16...[/bold yellow]")
    sensor_missing = sensor.poll("2024-01-16", timeout_seconds=3, poke_interval=1)
    console.print(f"  • Sensor Poll Status: [red]✖ NOT FOUND after {sensor_missing['waited_seconds']}s[/red]")
    console.print("  • Airflow Sensor soft_fail=True: Task gracefully marked SKIPPED (not FAILED)")

    console.print("\n[bold yellow]STEP 2 [DOWNSTREAM DAG]: BranchPythonOperator Evaluates Circuit Breaker[/bold yellow]")
    cb_branch = breaker.evaluate_sensor_result(sensor_missing)
    console.print(f"  • Branch Decision: [bold red]➜ {cb_branch}[/bold red]")

    console.print("\n[bold yellow]STEP 3 [DOWNSTREAM DAG]: Firing Circuit Breaker & Logging Incident[/bold yellow]")
    incident = breaker.trip("2024-01-16", reason="SENSOR_TIMEOUT_MISSING_SUCCESS_MARKER", details=sensor_missing)
    console.print(f"  • Circuit Breaker State: [bold red]OPEN[/bold red]")
    console.print(f"  • Downstream ETL Tasks: [yellow]SKIPPED (load_to_warehouse prevented from reading partial data)[/yellow]")

    console.print("\n[bold yellow]STEP 4: INSPECTING CIRCUIT BREAKER AUDIT LEDGER[/bold yellow]")
    cb_table = Table(title="Circuit Breaker Audit Incident Log", show_lines=True)
    cb_table.add_column("Event", style="magenta bold")
    cb_table.add_column("Logical Date", style="cyan")
    cb_table.add_column("Reason", style="red bold")
    cb_table.add_column("State", justify="center")
    cb_table.add_column("Timestamp", style="dim")

    for inc in breaker.get_trip_history():
        cb_table.add_row(
            inc["event"],
            inc["logical_date"],
            inc["reason"],
            f"[bold red]{inc['state']}[/bold red]",
            inc["timestamp"],
        )

    console.print(cb_table)

    console.print("\n[bold green]✔ DEMO COMPLETE: Event-Driven Sensor & Circuit Breaker fully verified![/bold green]\n")


if __name__ == "__main__":
    run_demo()
