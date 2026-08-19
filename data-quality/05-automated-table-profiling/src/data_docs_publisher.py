"""
Project 5: Data Docs Publisher
Builds clean, self-contained Data Docs HTML reports ready for GitHub Pages or AWS S3 hosting.
"""

import os
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd
from jinja2 import Template

import great_expectations as gx

HTML_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Great Expectations Data Docs - {{ table_name }}</title>
    <style>
        :root {
            --bg-primary: #0f172a;
            --bg-card: #1e293b;
            --border: #334155;
            --text-primary: #f8fafc;
            --text-secondary: #94a3b8;
            --success: #10b981;
            --failure: #ef4444;
            --accent: #38bdf8;
        }
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: var(--bg-primary);
            color: var(--text-primary);
            margin: 0;
            padding: 2rem;
            line-height: 1.5;
        }
        .container {
            max-width: 1200px;
            margin: 0 auto;
        }
        header {
            border-bottom: 1px solid var(--border);
            padding-bottom: 1.5rem;
            margin-bottom: 2rem;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        h1 { margin: 0; font-size: 1.75rem; color: var(--text-primary); }
        .meta-pill {
            background: var(--bg-card);
            padding: 0.35rem 0.75rem;
            border-radius: 9999px;
            font-size: 0.875rem;
            border: 1px solid var(--border);
            color: var(--accent);
        }
        .summary-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 1rem;
            margin-bottom: 2rem;
        }
        .card {
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 8px;
            padding: 1.25rem;
        }
        .card-label { font-size: 0.8rem; color: var(--text-secondary); text-transform: uppercase; letter-spacing: 0.05em; }
        .card-value { font-size: 1.75rem; font-weight: 700; margin-top: 0.25rem; }
        .badge-success { color: var(--success); }
        .badge-failure { color: var(--failure); }
        table {
            width: 100%;
            border-collapse: collapse;
            background: var(--bg-card);
            border-radius: 8px;
            overflow: hidden;
            border: 1px solid var(--border);
        }
        th, td {
            padding: 0.85rem 1rem;
            text-align: left;
            border-bottom: 1px solid var(--border);
        }
        th {
            background: #1e293b;
            color: var(--text-secondary);
            font-weight: 600;
            font-size: 0.85rem;
            text-transform: uppercase;
        }
        tr:last-child td { border-bottom: none; }
        .tag-pass {
            background: rgba(16, 185, 129, 0.15);
            color: var(--success);
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-weight: 600;
            font-size: 0.8rem;
        }
        .tag-fail {
            background: rgba(239, 68, 68, 0.15);
            color: var(--failure);
            padding: 0.2rem 0.5rem;
            border-radius: 4px;
            font-weight: 600;
            font-size: 0.8rem;
        }
        code {
            font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
            background: rgba(0, 0, 0, 0.3);
            padding: 0.15rem 0.35rem;
            border-radius: 4px;
            font-size: 0.85em;
            color: #e2e8f0;
        }
        footer {
            margin-top: 3rem;
            text-align: center;
            color: var(--text-secondary);
            font-size: 0.85rem;
            border-top: 1px solid var(--border);
            padding-top: 1rem;
        }
    </style>
</head>
<body>
    <div class="container">
        <header>
            <div>
                <h1>Data Quality Report: {{ table_name }}</h1>
                <div style="color: var(--text-secondary); margin-top: 0.35rem;">Suite: <code>{{ suite_name }}</code></div>
            </div>
            <div class="meta-pill">Run: {{ run_id }} • {{ evaluated_at }}</div>
        </header>

        <div class="summary-grid">
            <div class="card">
                <div class="card-label">Overall Status</div>
                <div class="card-value {% if success %}badge-success{% else %}badge-failure{% endif %}">
                    {% if success %}PASSED{% else %}FAILED{% endif %}
                </div>
            </div>
            <div class="card">
                <div class="card-label">Total Expectations</div>
                <div class="card-value">{{ total_expectations }}</div>
            </div>
            <div class="card">
                <div class="card-label">Passed Checks</div>
                <div class="card-value badge-success">{{ passed_expectations }}</div>
            </div>
            <div class="card">
                <div class="card-label">Failed Checks</div>
                <div class="card-value {% if failed_expectations > 0 %}badge-failure{% endif %}">{{ failed_expectations }}</div>
            </div>
            <div class="card">
                <div class="card-label">Success Rate</div>
                <div class="card-value">{{ success_rate }}%</div>
            </div>
        </div>

        <h2 style="font-size: 1.25rem; margin-bottom: 1rem;">Expectation Validation Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Status</th>
                    <th>Expectation Name</th>
                    <th>Target Column</th>
                    <th>Rule Parameters</th>
                    <th>Observed Value</th>
                </tr>
            </thead>
            <tbody>
                {% for exp in expectations %}
                <tr>
                    <td>
                        <span class="{% if exp.success %}tag-pass{% else %}tag-fail{% endif %}">
                            {% if exp.success %}PASS{% else %}FAIL{% endif %}
                        </span>
                    </td>
                    <td><code>{{ exp.name }}</code></td>
                    <td><strong>{{ exp.column }}</strong></td>
                    <td style="color: var(--text-secondary); font-size: 0.9rem;">{{ exp.params }}</td>
                    <td>{{ exp.observed }}</td>
                </tr>
                {% endfor %}
            </tbody>
        </table>

        <footer>
            Automated Data Quality Documentation • Generated by Great Expectations & Antigravity
        </footer>
    </div>
</body>
</html>
"""


class DataDocsPublisher:
    """
    Renders standalone HTML Data Docs from GX validation results
    ready for deployment to S3 buckets, GitHub Pages, or internal dashboards.
    """

    def __init__(self, docs_dir: str = "data-quality/05-automated-table-profiling/docs/site"):
        self.docs_dir = Path(docs_dir)
        self.docs_dir.mkdir(parents=True, exist_ok=True)

    def publish_report(
        self,
        table_name: str,
        suite_name: str,
        run_id: str,
        validation_result: Any,
    ) -> Path:
        """
        Renders HTML report and returns the absolute file path.
        """
        total = len(validation_result.results)
        passed = sum(1 for r in validation_result.results if r.success)
        failed = total - passed
        success_rate = round((passed / total * 100), 1) if total > 0 else 100.0

        expectations = []
        for r in validation_result.results:
            kwargs = r.expectation_config.kwargs.copy()
            col = kwargs.pop("column", "table-level")
            obs = r.result.get("observed_value") or r.result.get("unexpected_count") or ""
            expectations.append({
                "success": r.success,
                "name": r.expectation_config.type,
                "column": col,
                "params": str(kwargs),
                "observed": str(obs) if obs != "" else ("✔ Clean" if r.success else "✖ Violation"),
            })

        template = Template(HTML_TEMPLATE)
        html_content = template.render(
            table_name=table_name,
            suite_name=suite_name,
            run_id=run_id,
            evaluated_at=datetime.now().strftime("%Y-%m-%d %H:%M:%S UTC"),
            success=validation_result.success,
            total_expectations=total,
            passed_expectations=passed,
            failed_expectations=failed,
            success_rate=success_rate,
            expectations=expectations,
        )

        output_file = self.docs_dir / f"{table_name}_data_docs.html"
        with open(output_file, "w") as f:
            f.write(html_content)

        # Also write index.html as latest entry
        index_file = self.docs_dir / "index.html"
        with open(index_file, "w") as f:
            f.write(html_content)

        return output_file

