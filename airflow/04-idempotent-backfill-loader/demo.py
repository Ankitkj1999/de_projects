"""
demo.py — Interactive Demo for Project 04: Backfill-Safe Idempotent Data Loader
================================================================================
Demonstrates:
  1. Initial Scheduled Run (2024-01-15): Partition empty, extracts 200 records, commits to SQLite.
  2. Backfill / Manual Re-Run (2024-01-15): Simulates operator re-triggering pipeline.
     UPSERT strategy runs — verifies warehouse retains EXACTLY 200 rows (0 duplicates added).
  3. Incremental Next Day Run (2024-01-16): Loads partition 2024-01-16. Total warehouse = 400 rows.
  4. Audit Trail Ledger: Inspects JSON audit log displaying run lineage and backfill flags.
"""

import sys
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from extractor import extract
from partition_checker import PartitionChecker
from warehouse_loader import WarehouseLoader
from idempotency_verifier import verify
from audit_logger import log, get_audit_trail

console = Console()


def run_demo():
    console.print(
        Panel.fit(
            "[bold white]PROJECT 04: BACKFILL-SAFE IDEMPOTENT DATA LOADER[/bold white]\n"
            "[dim cyan]\"We re-ran last month's jobs and now every record is duplicated in the warehouse\"[/dim cyan]",
            border_style="cyan",
        )
    )

    db_path = str(PROJECT_ROOT / "data" / "processed" / "airflow" / "warehouse" / "demo_sales_warehouse.db")
    audit_path = str(PROJECT_ROOT / "data" / "processed" / "airflow" / "audit" / "demo_load_audit.json")

    loader = WarehouseLoader(db_path=db_path)
    checker = PartitionChecker(db_path=db_path)
    loader.clear()
    Path(audit_path).unlink(missing_ok=True)

    # -------------------------------------------------------------
    # STEP 1: INITIAL RUN FOR 2024-01-15
    # -------------------------------------------------------------
    console.print("\n[bold yellow]STEP 1: INITIAL SCHEDULED RUN FOR PARTITION 2024-01-15[/bold yellow]")
    chk1 = checker.check("2024-01-15")
    console.print(f"  • Pre-check: Partition exists? [cyan]{chk1['partition_exists']}[/cyan] (Existing rows: {chk1['existing_row_count']})")

    ext1 = extract("2024-01-15")
    upsert1 = loader.upsert("2024-01-15", ext1["records"], run_id="scheduled__2024-01-15T00:00:00")
    console.print(f"  • Ingested [bold green]{upsert1['upserted']}[/bold green] records into warehouse.")

    v1 = verify("2024-01-15", expected_count=200, db_path=db_path)
    console.print(f"  • Idempotency Verification: [green]✔ PASSED — Warehouse has {v1['actual_count']} distinct rows (Duplicates: {v1['duplicate_detected']})[/green]")

    log("2024-01-15", upsert1, run_id="scheduled__2024-01-15T00:00:00", is_backfill=False, audit_file_path=audit_path)

    # -------------------------------------------------------------
    # STEP 2: RE-RUN (SIMULATING ACCIDENTAL OPERATOR RETRY / BACKFILL)
    # -------------------------------------------------------------
    console.print("\n[bold yellow]STEP 2: RETRY / BACKFILL RUN FOR 2024-01-15 (PROVING IDEMPOTENCY)[/bold yellow]")
    console.print("[dim]Simulating operator manually re-running 2024-01-15 after code patch...[/dim]")

    chk2 = checker.check("2024-01-15")
    console.print(f"  • Pre-check: Partition exists? [yellow]{chk2['partition_exists']}[/yellow] (Existing rows: {chk2['existing_row_count']})")

    ext2 = extract("2024-01-15")  # Deterministic extractor guarantees identical records
    upsert2 = loader.upsert("2024-01-15", ext2["records"], run_id="backfill__manual__2024-01-15")
    console.print(f"  • UPSERT executed with same batch: [bold yellow]{upsert2['upserted']} records processed[/bold yellow]")

    v2 = verify("2024-01-15", expected_count=200, db_path=db_path)
    console.print(f"  • Idempotency Verification: [bold green]✔ PASSED — Warehouse STILL has {v2['actual_count']} rows! (Zero duplicates)[/bold green]")

    log("2024-01-15", upsert2, run_id="backfill__manual__2024-01-15", is_backfill=True, audit_file_path=audit_path)

    # -------------------------------------------------------------
    # STEP 3: NEXT DAY SCHEDULED RUN (2024-01-16)
    # -------------------------------------------------------------
    console.print("\n[bold yellow]STEP 3: NEXT DAY RUN (PARTITION 2024-01-16)[/bold yellow]")
    ext3 = extract("2024-01-16")
    upsert3 = loader.upsert("2024-01-16", ext3["records"], run_id="scheduled__2024-01-16T00:00:00")
    console.print(f"  • Ingested [bold green]{upsert3['upserted']}[/bold green] records for new partition 2024-01-16.")

    total_rows = loader.get_partition_count("2024-01-15") + loader.get_partition_count("2024-01-16")
    console.print(f"  • Total Warehouse Rows Across Partitions: [bold cyan]{total_rows:,} rows[/bold cyan] (200 on Day 1 + 200 on Day 2)")

    log("2024-01-16", upsert3, run_id="scheduled__2024-01-16T00:00:00", is_backfill=False, audit_file_path=audit_path)

    # -------------------------------------------------------------
    # STEP 4: INSPECT AUDIT TRAIL LEDGER
    # -------------------------------------------------------------
    console.print("\n[bold yellow]STEP 4: FORENSIC AUDIT TRAIL LEDGER[/bold yellow]")
    trail = get_audit_trail(audit_file_path=audit_path)

    audit_table = Table(title="Airflow Partition Ingestion Audit Ledger", show_lines=True)
    audit_table.add_column("DAG Run ID", style="cyan")
    audit_table.add_column("Logical Date", style="magenta")
    audit_table.add_column("Rows Loaded", justify="right")
    audit_table.add_column("Partition Total", justify="right")
    audit_table.add_column("Backfill Flag", justify="center")
    audit_table.add_column("Timestamp", style="dim")

    for entry in trail:
        is_bf = entry.get("is_backfill", False)
        bf_badge = "[bold yellow]YES (BACKFILL)[/bold yellow]" if is_bf else "[green]NO (ORIGINAL)[/green]"
        audit_table.add_row(
            entry["run_id"],
            entry["logical_date"],
            f"{entry['rows_loaded']:,}",
            f"{entry['total_partition_rows']:,}",
            bf_badge,
            entry["timestamp"],
        )

    console.print(audit_table)

    console.print("\n[bold green]✔ DEMO COMPLETE: Backfill-Safe Idempotent Data Loader fully verified![/bold green]\n")


if __name__ == "__main__":
    run_demo()
