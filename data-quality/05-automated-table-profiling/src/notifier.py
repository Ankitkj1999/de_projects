"""
Project 5: Multi-Channel Alert Notifier (Slack & Zoho Cliq)
Dispatches structured failure alerts with execution summaries and direct links to Data Docs.
"""

import json
from typing import Dict, Any, List, Optional
import requests


class QualityAlertNotifier:
    """
    Formats and dispatches Data Quality failure alerts to Slack or Zoho Cliq.
    """

    def __init__(
        self,
        slack_webhook_url: Optional[str] = None,
        cliq_webhook_url: Optional[str] = None,
    ):
        self.slack_webhook_url = slack_webhook_url
        self.cliq_webhook_url = cliq_webhook_url

    def build_slack_payload(
        self,
        table_name: str,
        run_id: str,
        total_checks: int,
        failed_checks: int,
        failures: List[Dict[str, Any]],
        docs_url: str,
    ) -> Dict[str, Any]:
        """Builds rich Slack Block Kit payload."""
        status_emoji = "🚨" if failed_checks > 0 else "✅"
        status_text = "FAILED" if failed_checks > 0 else "PASSED"

        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"{status_emoji} Data Quality Alert: {table_name} ({status_text})",
                }
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Table:* `{table_name}`"},
                    {"type": "mrkdwn", "text": f"*Run ID:* `{run_id}`"},
                    {"type": "mrkdwn", "text": f"*Total Checks:* {total_checks}"},
                    {"type": "mrkdwn", "text": f"*Failed Checks:* `{failed_checks}`"},
                ]
            }
        ]

        if failures:
            failure_lines = []
            for f in failures[:5]:
                col = f.get("column", "table")
                rule = f.get("expectation", "Rule")
                failure_lines.append(f"• *{col}*: `{rule}`")
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Sample Failed Expectations:*\n" + "\n".join(failure_lines)
                }
            })

        blocks.append({
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Data Docs Report"},
                    "url": docs_url,
                    "style": "danger" if failed_checks > 0 else "primary",
                }
            ]
        })

        return {"blocks": blocks}

    def build_cliq_payload(
        self,
        table_name: str,
        run_id: str,
        total_checks: int,
        failed_checks: int,
        failures: List[Dict[str, Any]],
        docs_url: str,
    ) -> Dict[str, Any]:
        """Builds Zoho Cliq bot card payload."""
        return {
            "text": f"Data Quality Check for {table_name}: {'FAILED' if failed_checks > 0 else 'PASSED'}",
            "card": {
                "title": f"Data Quality Report: {table_name}",
                "theme": "prompt" if failed_checks > 0 else "modern-inline",
            },
            "slides": [
                {
                    "type": "label",
                    "title": "Validation Summary",
                    "data": [
                        {"Total Checks": str(total_checks)},
                        {"Failed Checks": str(failed_checks)},
                        {"Report URL": docs_url},
                    ]
                }
            ]
        }

    def send_alert(
        self,
        table_name: str,
        run_id: str,
        total_checks: int,
        failed_checks: int,
        failures: List[Dict[str, Any]],
        docs_url: str,
        channel: str = "slack",
        dry_run: bool = True,
    ) -> Dict[str, Any]:
        """
        Sends alert or returns generated payload in dry_run mode.
        """
        if channel == "slack":
            payload = self.build_slack_payload(table_name, run_id, total_checks, failed_checks, failures, docs_url)
            target_url = self.slack_webhook_url
        else:
            payload = self.build_cliq_payload(table_name, run_id, total_checks, failed_checks, failures, docs_url)
            target_url = self.cliq_webhook_url

        if dry_run or not target_url:
            return {
                "status": "DRY_RUN_EMITTED",
                "channel": channel,
                "payload": payload,
            }

        response = requests.post(target_url, json=payload, timeout=5)
        return {
            "status": "SENT" if response.status_code == 200 else "ERROR",
            "status_code": response.status_code,
            "channel": channel,
            "payload": payload,
        }

