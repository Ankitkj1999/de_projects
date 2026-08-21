# Project 5: "I inherited 200 tables with zero tests" (Cold Start & Visibility)

## The Real-World Problem
Joining a new engineering team or inheriting a mature enterprise warehouse (Snowflake, BigQuery, Databricks, Postgres) often presents a cold-start dilemma:
- Hundreds of tables power critical production pipelines, but have zero automated tests or documentation.
- Manually authoring thousands of expectations line-by-line is virtually impossible.
- Naive auto-profilers overfit and generate brittle rules (e.g. asserting exact row counts or pinning min/max to test values), resulting in alert fatigue and muted Slack channels.

---

## The Cold-Start Architecture
This project implements a pragmatic 4-step framework to transition an untested catalog into a monitored, self-documenting data mesh:

```
        Untested Warehouse Tables
                   │
                   ▼
     ┌───────────────────────────┐
     │  Automated Table Profiler │  • Inferred types & nullability
     │  Statistical Scanner      │  • Primary key candidates
     └─────────────┬─────────────┘  • Low-cardinality enum sets
                   │
                   ▼
     ┌───────────────────────────┐
     │  Intelligent Suite Pruner │  • Discards overfitting min/max
     │  Signal-to-Noise Filter   │  • Applies resilient mostly= thresholds
     └─────────────┬─────────────┘  • Curates production-grade GX Suite
                   │
         ┌─────────┴─────────┐
         ▼                   ▼
┌──────────────────┐ ┌──────────────────┐
│ Static Data Docs │ │ Webhook Alerts   │
│ HTML Site        │ │ Slack & Cliq     │
│ S3 / GH Pages    │ │ Actionable Cards │
└──────────────────┘ └──────────────────┘
```

---

## 1. Automated Profiling & Pruning Strategy
- **Primary Key Candidate Detection**: Identifies 100% unique, non-null ID columns and binds `ExpectColumnValuesToBeUnique` and `ExpectColumnValuesToNotBeNull`.
- **Low-Cardinality Enum Whitelisting**: Detects categorical columns ($\le 12$ unique values) and compiles `ExpectColumnValuesToBeInSet`.
- **Brittle Rule Pruning**: Discards narrow numeric min/max bounds and exact row-count matches that would break on subsequent transaction batches.

---

## 2. Multi-Channel Alerting (Slack & Zoho Cliq)
Formats rich webhook payloads featuring:
- High-visibility status badge (`PASSED` vs `FAILED`).
- Target table name, Run ID, and check tallies.
- Bulleted failure breakdown with target columns and violating metrics.
- One-click interactive link directing on-call engineers to the generated Data Docs page.

---

## 3. GitHub Actions CI/CD Integration
Includes a preconfigured GitHub Actions workflow:
[.github/workflows/data_quality_ci.yml](file:///home/ankit/Documents/Code/Projects/de_projects/.github/workflows/data_quality_ci.yml)
Runs validation suites on Pull Requests, generates PR comments, and gates merges before bad code hits production.

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python data-quality/05-automated-table-profiling/demo.py
```

### 2. View the Generated Data Docs
Open the generated HTML report in any web browser:
```
data-quality/05-automated-table-profiling/docs/site/index.html
```

### 3. Run the Test Suite
```bash
.venv/bin/pytest data-quality/05-automated-table-profiling/tests/ -v
```

