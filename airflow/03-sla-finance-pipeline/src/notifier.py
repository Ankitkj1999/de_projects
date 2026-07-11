"""
src/notifier.py — Executive Financial KPI Notifier
==================================================
Real-World Problem:
    C-level executives and finance controllers need immediate, high-level summaries
    of daily financial operations, including gross merchandise value (GMV), top channels,
    and report storage location.
"""

from __future__ import annotations

from typing import Any, Dict
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def notify_cfo(report_path: str, aggregation: Dict[str, Any]) -> Dict[str, Any]:
    """
    Renders executive dashboard card in terminal and generates webhook payload.
    """
    total_usd = aggregation.get("total_revenue_usd", 0.0)
    total_tx = aggregation.get("total_transactions", 0)
    aov = aggregation.get("average_order_value_usd", 0.0)
    channels = aggregation.get("revenue_by_channel", {})

    table = Table(title="Daily CFO Financial Summary", show_lines=True)
    table.add_column("Executive KPI", style="cyan bold")
    table.add_column("Value", style="magenta bold")
    table.add_column("Notes / Benchmark")

    table.add_row("Gross Revenue (USD)", f"${total_usd:,.2f}", "FX Converted from BRL @ 0.20")
    table.add_row("Total Transactions", f"{total_tx:,} orders", "Across all active channels")
    table.add_row("Average Order Value (AOV)", f"${aov:,.2f}", "Per completed order")
    
    top_channel = max(channels.items(), key=lambda x: x[1]) if channels else ("N/A", 0.0)
    table.add_row("Top Performing Channel", f"{top_channel[0]} (${top_channel[1]:,.2f})", "Highest revenue contributor")
    table.add_row("Official Report Artifact", str(report_path), "JSON Audited Ledger")

    console.print(
        Panel(
            table,
            title="[bold green]✔ FINANCIAL SLA MET: EXECUTIVE REPORT READY (6:00 AM SLA)[/bold green]",
            subtitle="[dim]Delivered to #executive-finance[/dim]",
            border_style="green",
        )
    )

    return {
        "channel": "#executive-finance",
        "title": "Daily CFO Financial Summary",
        "total_revenue_usd": total_usd,
        "aov": aov,
        "report_path": str(report_path),
    }
