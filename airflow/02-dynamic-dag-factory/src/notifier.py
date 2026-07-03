"""
src/notifier.py — Tenant Pipeline Notifier
==========================================
Real-World Problem:
    In a multi-tenant environment, tenant SLA notifications must be routed
    to tenant-specific channels (e.g. #de-acme-alerts, #de-globex-alerts).
"""

from __future__ import annotations

from typing import Any, Dict
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def notify(client_config: Dict[str, Any], stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Renders rich console alert and returns mock webhook payload.
    """
    client_id = client_config["client_id"]
    display_name = client_config.get("display_name", client_id)
    channel = client_config.get("notify_channel", "#general-alerts")
    status = stats.get("status", "SUCCESS")

    color = "green" if status == "SUCCESS" else "red"

    payload = {
        "channel": channel,
        "tenant": client_id,
        "title": f"ETL Run for {display_name}",
        "status": status,
        "rows_loaded": stats.get("loaded_batch", 0),
        "total_in_table": stats.get("total_in_table", 0),
    }

    table = Table(show_header=True, header_style="bold magenta")
    table.add_column("Tenant Property", style="bold")
    table.add_column("Value")

    table.add_row("Client ID", client_id)
    table.add_row("Display Name", display_name)
    table.add_row("Slack Channel", channel)
    table.add_row("Rows Ingested", f"{stats.get('loaded_batch', 0):,}")
    table.add_row("Target Table", client_config.get("target_table", "N/A"))
    table.add_row("Pipeline Outcome", f"[{color}]{status}[/{color}]")

    console.print(
        Panel(
            table,
            title=f"[{color}]✔ TENANT ALERT DISPATCHED: {display_name}[/{color}]",
            subtitle=f"[dim]Routed to {channel}[/dim]",
            border_style=color,
        )
    )

    return payload
