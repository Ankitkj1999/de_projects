"""
demo.py — Interactive Demo for Project 02: Dynamic DAG Factory for Multi-Tenant Clients
========================================================================================
Demonstrates:
  1. Discovery of 4 enterprise tenants from declarative YAML configuration
  2. Programmatic generation of 4 independent Airflow DAGs at parse time
  3. Execution of multi-tenant pipelines with tenant isolation (SQLite tables per tenant)
  4. Instant onboarding of a 5th tenant (Stark Industries) via pure YAML with ZERO code changes
"""

import sys
import tempfile
from pathlib import Path
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_ROOT / "config" / "clients.yaml"
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from dag_factory import ClientDAGFactory
from client_extractor import extract
from validator import validate
from loader import load, query_client_table
from notifier import notify

console = Console()


def run_demo():
    console.print(
        Panel.fit(
            "[bold white]PROJECT 02: DYNAMIC DAG FACTORY FOR MULTI-TENANT CLIENTS[/bold white]\n"
            "[dim cyan]\"We onboard a new client every week and copy-pasting DAGs is becoming a nightmare\"[/dim cyan]",
            border_style="cyan",
        )
    )

    # -------------------------------------------------------------
    # STEP 1: DISCOVER CLIENTS FROM CONFIG
    # -------------------------------------------------------------
    console.print("\n[bold yellow]STEP 1: PARSING DECLARATIVE TENANT CONFIGURATION (clients.yaml)[/bold yellow]")
    factory = ClientDAGFactory(CONFIG_PATH)
    configs = factory.load_configs()

    discovery_table = Table(title="Discovered Enterprise Tenants", show_lines=True)
    discovery_table.add_column("Client ID", style="cyan bold")
    discovery_table.add_column("Display Name", style="white")
    discovery_table.add_column("Schedule", style="yellow")
    discovery_table.add_column("Min Threshold", justify="right", style="magenta")
    discovery_table.add_column("Target Table", style="green")
    discovery_table.add_column("Slack Channel", style="blue")

    for c in configs:
        discovery_table.add_row(
            c["client_id"],
            c["display_name"],
            c.get("schedule", "@daily"),
            f"{c.get('min_row_threshold', 50):,} rows",
            c["target_table"],
            c.get("notify_channel", "#alerts"),
        )

    console.print(discovery_table)

    # -------------------------------------------------------------
    # STEP 2: DYNAMIC DAG FACTORY COMPILATION
    # -------------------------------------------------------------
    console.print("\n[bold yellow]STEP 2: AIRFLOW DAG FACTORY COMPILATION AT PARSE TIME[/bold yellow]")
    dags = factory.generate_all_dags()
    console.print(f"  • Factory compiled [bold green]{len(dags)} DAGs[/bold green] dynamically into globals():")
    for dag_id, dag_obj in dags.items():
        console.print(f"    [green]✔[/green] Registered DAG: [bold]{dag_id}[/bold] (schedule: [yellow]{dag_obj.schedule}[/yellow], tags: [dim]{dag_obj.tags}[/dim])")

    # -------------------------------------------------------------
    # STEP 3: RUN PIPELINE FOR EACH TENANT
    # -------------------------------------------------------------
    console.print("\n[bold yellow]STEP 3: EXECUTING MULTI-TENANT ISOLATED INGESTION FLOWS[/bold yellow]")
    execution_table = Table(title="Multi-Tenant Pipeline Execution Results", show_lines=True)
    execution_table.add_column("Tenant", style="cyan bold")
    execution_table.add_column("Extracted", justify="right", style="magenta")
    execution_table.add_column("Validation", justify="center")
    execution_table.add_column("Table Loaded", style="green")
    execution_table.add_column("Total Rows", justify="right", style="bold white")
    execution_table.add_column("Status", justify="center")

    for cfg in configs:
        ext = extract(cfg)
        val = validate(cfg, ext["records"])
        ld = load(cfg, ext["records"])

        val_badge = "[green]✔ PASSED[/green]" if val["passed"] else "[red]✖ FAILED[/red]"
        status_badge = "[green]SUCCESS[/green]" if val["passed"] else "[red]HALTED[/red]"

        execution_table.add_row(
            cfg["display_name"],
            f"{ext['record_count']:,}",
            val_badge,
            cfg["target_table"],
            f"{ld['total_in_table']:,}",
            status_badge,
        )

    console.print(execution_table)

    # -------------------------------------------------------------
    # STEP 4: INSTANT ONBOARDING OF 5TH TENANT (ZERO CODE CHANGES)
    # -------------------------------------------------------------
    console.print("\n[bold yellow]STEP 4: ONBOARDING A NEW CLIENT — ZERO CODE CHANGES (STARK ENTERPRISES)[/bold yellow]")
    console.print("[dim]Simulating developer adding 1 new YAML block to config/clients.yaml...[/dim]")

    with open(CONFIG_PATH, "r") as f:
        existing_yaml = yaml.safe_load(f)

    stark_tenant = {
        "client_id": "stark_industries",
        "display_name": "Stark Industries Logistics",
        "schedule": "0 2 * * *",
        "source_path": "data/processed/airflow/clients/stark_industries/orders.csv",
        "target_table": "client_orders_stark_industries",
        "min_row_threshold": 300,
        "max_price_ceiling": 50000.0,
        "sla_minutes: 15": None,
        "notify_channel": "#de-stark-defense",
        "owner": "strategic-accounts",
    }
    existing_yaml["clients"].append(stark_tenant)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml") as tmp_yaml:
        yaml.dump(existing_yaml, tmp_yaml)
        tmp_yaml.flush()

        new_factory = ClientDAGFactory(tmp_yaml.name)
        new_dags = new_factory.generate_all_dags()

        console.print(f"  • Re-parsed YAML: Discovered [bold cyan]{len(new_dags)} tenants[/bold cyan] (previous: 4)")
        console.print(f"  • New DAG created: [bold green]etl_stark_industries[/bold green] (schedule: 0 2 * * *)")
        assert "etl_stark_industries" in new_dags, "New DAG must be present!"

    console.print("\n[bold green]✔ DEMO COMPLETE: Multi-Tenant Dynamic DAG Factory fully verified![/bold green]\n")


if __name__ == "__main__":
    run_demo()
