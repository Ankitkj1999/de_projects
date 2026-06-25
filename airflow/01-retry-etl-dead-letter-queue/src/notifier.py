"""
src/notifier.py — Rich Slack / Teams Notification Formatter
============================================================
Real-World Problem:
    Pipelines often fail silently or alert engineers with noisy, unhelpful stack traces.
    In enterprise data platforms, engineers need structured notifications detailing:
      - Total records fetched vs valid vs quarantined
      - Specific failure breakdown (e.g. 5 negative prices, 2 null IDs)
      - Destination of quarantined records for operational debugging
"""

from __future__ import annotations

import json
from typing import Any, Dict
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

console = Console()


def build_slack_payload(stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Constructs a production-ready Slack webhook payload with Block Kit structure.
    """
    status = stats.get("status", "SUCCESS")
    color = "#36a64f" if status == "SUCCESS" else "#ff0000" if status == "FAILED" else "#ffaa00"
    
    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": f"Pipeline Run: {stats.get('dag_run_id', 'manual_run')}",
            },
        },
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Status:* {status}"},
                {"type": "mrkdwn", "text": f"*Batch Date:* {stats.get('batch_date', 'N/A')}"},
                {"type": "mrkdwn", "text": f"*Extracted:* {stats.get('total_extracted', 0):,} records"},
                {"type": "mrkdwn", "text": f"*Staged (Valid):* {stats.get('valid_loaded', 0):,} records"},
                {"type": "mrkdwn", "text": f"*Quarantined (DLQ):* {stats.get('quarantined_count', 0):,} records"},
                {"type": "mrkdwn", "text": f"*Retry Attempts:* {stats.get('retry_attempts', 0)}"},
            ],
        },
    ]

    dlq_reasons = stats.get("dlq_reasons", {})
    if dlq_reasons:
        reason_lines = "\n".join(f"• `{k}`: {v}" for k, v in dlq_reasons.items())
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Quarantine Breakdown:*\n{reason_lines}",
            },
        })

    return {
        "channel": "#de-alerts",
        "username": "Airflow Sentinel",
        "attachments": [{"color": color, "blocks": blocks}],
    }


def send_summary(stats: Dict[str, Any]) -> Dict[str, Any]:
    """
    Simulates sending the Slack webhook and renders a rich visual console card.
    """
    payload = build_slack_payload(stats)

    status = stats.get("status", "SUCCESS")
    badge_color = "green" if status == "SUCCESS" else "yellow" if stats.get("quarantined_count", 0) > 0 else "red"
    
    table = Table(show_header=True, header_style="bold cyan")
    table.add_column("Pipeline Metric", style="bold")
    table.add_column("Value", style="magenta")
    table.add_column("Destination / Impact")

    table.add_row("Extracted Records", f"{stats.get('total_extracted', 0):,}", "Vendor API Pages 1-3")
    table.add_row("Valid Records Staged", f"[green]{stats.get('valid_loaded', 0):,}[/green]", "staging_vendor_orders (SQLite)")
    table.add_row("Quarantined to DLQ", f"[yellow]{stats.get('quarantined_count', 0):,}[/yellow]", "vendor_orders_dlq (SQLite)")
    table.add_row("Retry Count", str(stats.get("retry_attempts", 0)), "Exponential Backoff (5s delay)")

    console.print(
        Panel(
            table,
            title=f"[{badge_color}]✔ AIRFLOW DAG NOTIFICATION: {stats.get('dag_run_id', 'run')}[/{badge_color}]",
            subtitle="[dim]Slack Webhook Payload Generated (Mocked)[/dim]",
            border_style=badge_color,
        )
    )

    return payload
