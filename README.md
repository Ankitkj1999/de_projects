# Production Data Engineering Portfolio: Data Quality & Apache Airflow

[![Data Engineering CI](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-blue.svg)](.github/workflows/data_quality_ci.yml)
[![Apache Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE.svg)](https://airflow.apache.org)
[![Great Expectations](https://img.shields.io/badge/Great%20Expectations-1.24.1-orange.svg)](https://greatexpectations.io)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00A4E4.svg)](https://delta.io)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB.svg)](https://python.org)
[![Tests Passing](https://img.shields.io/badge/Tests-59%20Passing-brightgreen.svg)](#test-suite-results)

An end-to-end, production-grade Data Engineering portfolio featuring **10 enterprise problem-solving implementations**:
- **5 Data Quality Engineering projects** built on Great Expectations & Delta Lake
- **5 Apache Airflow Problem-Solving projects** tackling real-world orchestration, multi-tenancy, idempotency, and SLA failures

Built on top of a realistic **Brazilian E-Commerce (Olist)** dataset with reproducible scenarios.

---

## Part 1: The 5 Apache Airflow Problem-Solving Projects

```
                     AIRFLOW ENTERPRISE ORCHESTRATION
                                    │
  ┌─────────────────────────────────┼─────────────────────────────────┐
  │                                 │                                 │
  ▼                                 ▼                                 ▼
┌─────────────────────────┐ ┌─────────────────────────┐ ┌─────────────────────────┐
│ Project 1: Retry-Aware  │ │ Project 2: Dynamic DAG   │ │ Project 3: SLA-Monitored│
│ ETL + Dead Letter Queue │ │ Factory (Multi-Tenant)   │ │ Financial Revenue Report│
│ Exponential Backoff API │ │ Declarative YAML Config  │ │ Freshness ShortCircuit  │
│ Staging vs DLQ Split    │ │ Auto-compiled globals()  │ │ Task SLA Miss Callbacks │
└────────────┬────────────┘ └───────────┬──────────────┘ └────────────┬────────────┘
             │                          │                             │
             └──────────────────┬───────┴─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Project 4: Backfill-Safe Idempotent Partition Loader (UPSERT & {{ ds }} Macros) │
│ Re-running historical dates never duplicates rows • Automated Lineage Ledger    │
└───────────────────────────────────────┬─────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│ Project 5: Event-Driven Sensor Pipeline with Circuit Breaker Pattern            │
│ Atomic _SUCCESS Marker • SHA-256 Checksum Validation • Graceful Soft-Fail Skips │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### Airflow Projects Matrix

| # | Project Name & Scenario | Problem Solved | Key Airflow & Engineering Patterns | Stack |
|---|-------------------------|----------------|------------------------------------|-------|
| **1** | [**Retry-Aware ETL + Dead Letter Queue**](file:///home/ankit/Documents/Code/Projects/de_projects/airflow/01-retry-etl-dead-letter-queue) <br> *"The API timed out at 3 AM and nobody knew until the morning dashboard was blank"* | Upstream vendor APIs fail intermittently. Flaky network crashes monolithic DAGs, while corrupt rows poison analytics tables. | `retries=3`, `retry_exponential_backoff=True`<br>Stream splitting: Staging vs DLQ<br>SQLite DLQ store with root causes<br>Slack Block Kit Channel Alerting | Python 3.12, SQLite, Airflow DAG, Rich |
| **2** | [**Dynamic DAG Factory for Multi-Tenant Clients**](file:///home/ankit/Documents/Code/Projects/de_projects/airflow/02-dynamic-dag-factory) <br> *"We onboard a new client every week and copy-pasting DAGs is becoming a nightmare"* | Onboarding 40 SaaS customers with duplicate DAGs causes configuration drift, template errors, and maintenance paralysis. | Declarative `clients.yaml` contract<br>Parse-time `globals()[dag_id]` compilation<br>Isolated tenant SQLite tables<br>Per-tenant SLAs and volume gates | PyYAML, Python Dynamic DAGs, SQLite |
| **3** | [**SLA-Monitored Financial Report Pipeline**](file:///home/ankit/Documents/Code/Projects/de_projects/airflow/03-sla-finance-pipeline) <br> *"The CFO's daily revenue report sometimes shows yesterday's numbers — and nobody gets alerted"* | Stale upstream data silently generates erroneous financial figures; heavy aggregation queries hang without notifying on-call. | `ShortCircuitOperator` freshness gate<br>`DAG(sla_miss_callback=on_sla_miss)`<br>Task-level SLAs & `execution_timeout`<br>Audited JSON executive ledger | Airflow Operators, ShortCircuit, FX Normalization |
| **4** | [**Backfill-Safe Idempotent Data Loader**](file:///home/ankit/Documents/Code/Projects/de_projects/airflow/04-idempotent-backfill-loader) <br> *"We re-ran last month's jobs and now every record is duplicated in the warehouse"* | Backfilling a month of history or manually re-running DAG runs with plain `INSERT` doubles row counts across warehouse tables. | Strict `{{ ds }}` logical date partitioning<br>Deterministic seeded extractors<br>SQLite UPSERT (`INSERT OR REPLACE`)<br>Automated idempotency assertions | SQLite, Airflow Date Macros, Lineage Ledger |
| **5** | [**Event-Driven Sensor + Circuit Breaker**](file:///home/ankit/Documents/Code/Projects/de_projects/airflow/05-sensor-circuit-breaker) <br> *"Our pipeline starts processing before the upstream file is even fully uploaded"* | Fixed cron schedules begin processing while upstream files are still streaming bytes. Delayed files cause infinite sensor hangs. | Atomic `_SUCCESS_{ds}` marker pattern<br>SHA-256 cryptographic checksum matching<br>`BranchPythonOperator` Circuit Breaker<br>`soft_fail=True` slot conservation | Python FileSensor, SHA-256, Branching |

---

## Part 2: The 5 Data Quality Projects Overview

| # | Project Name & Scenario | Problem Solved | Key Expectations & Patterns | Architecture & Stack |
|---|-------------------------|----------------|-----------------------------|----------------------|
| **1** | [**Schema Drift Validation Gate**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/01-schema-drift-gate) <br> *"The vendor renamed a column and nobody noticed"* | Vendor feeds unannounced rename (`order_purchase_timestamp` &rarr; `purchase_date`), swapped columns, or illegal status codes. | `ExpectTableColumnsToMatchOrderedList`<br>`ExpectColumnValuesToBeInSet`<br>`ExpectColumnValuesToNotBeNull`<br>`ExpectColumnValuesToMatchRegex` | Python CLI, Pre-Bronze Gate, Incident JSON report |
| **2** | [**Business Rules Validator**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/02-business-rules-validator) <br> *"The dashboard is wrong but nothing failed"* | Syntactic checks pass, but revenue reports show negative prices, future dates, broken foreign keys, or orders delivered before purchase. | `ExpectColumnValuesToBeBetween`<br>`ExpectColumnValuesToBeUnique`<br>`ExpectColumnValuesToBeInSet`<br>`ExpectColumnPairValuesAToBeGreaterThanB`<br>Cart-to-Payment Reconciliation | Multi-table relational validator, Diagnostic failure matrix |
| **3** | [**Volume & Freshness Monitor**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/03-volume-freshness-anomaly) <br> *"The job was green but loaded half the data"* | Silent pagination failure dropped 86% of records; partition bug loaded stale data from 2 weeks ago; null rate spiked to 18%. | `ExpectTableRowCountToBeBetween`<br>`ExpectColumnMaxToBeBetween`<br>`ExpectColumnValuesToNotBeNull(mostly=0.98)` | Rolling 7-day baseline tracker, Multi-tier severity (Hard Fail vs Warning) |
| **4** | [**Delta Lake Quarantine Pattern**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/04-delta-lake-quarantine) <br> *"One bad row shouldn't block 10M good ones"* | Failing 10M rows for 50 bad records halts business SLAs; silently dropping rows causes unrecoverable data loss. | Vectorized row-level GX rules<br>Silver Delta table commit<br>Quarantine Delta table with failure reasons<br>Automated Circuit Breaker | Native Delta Lake (`deltalake`), PySpark Glue / Databricks script, Zero data loss |
| **5** | [**Cold-Start Profiling & Docs**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/05-automated-table-profiling) <br> *"I inherited 200 tables with zero tests"* | Joining an enterprise data platform with hundreds of unmonitored tables and no tests. Manual rule authoring is impossible. | Automated Statistical Profiler<br>Intelligent Signal-to-Noise Pruner<br>Static HTML Data Docs Generator<br>Slack & Zoho Cliq Alert Webhooks | Ephemeral Data Context, Jinja2 Data Docs, Webhook Notifier, GitHub Actions CI |

---

## Quickstart: Running Demos & Test Suites

### 1. Run All 5 Airflow Demos
```bash
./run_all_airflow_demos.sh
```

### 2. Run All 5 Data Quality Demos
```bash
./run_all_demos.sh
```

### 3. Run Individual Airflow Project Demos
```bash
# Project 1: Retry-Aware ETL + Dead Letter Queue
.venv/bin/python airflow/01-retry-etl-dead-letter-queue/demo.py

# Project 2: Dynamic DAG Factory
.venv/bin/python airflow/02-dynamic-dag-factory/demo.py

# Project 3: SLA-Monitored Finance Pipeline
.venv/bin/python airflow/03-sla-finance-pipeline/demo.py

# Project 4: Backfill-Safe Idempotent Loader
.venv/bin/python airflow/04-idempotent-backfill-loader/demo.py

# Project 5: Event-Driven Sensor & Circuit Breaker
.venv/bin/python airflow/05-sensor-circuit-breaker/demo.py
```

### 4. Run the Full Pytest Suite (59 Test Cases Passing)
```bash
.venv/bin/pytest -v
```

---

## Test Suite Results

```
============================== 59 passed in 7.68s ==============================
✔ 35/35 Passing in Airflow Problem-Solving Suite
✔ 24/24 Passing in Great Expectations Data Quality Suite
```

---

## Production Deployment & Stack Compatibility
- **Lakehouse Engines**: Native Delta Lake tables, Apache Spark (PySpark), Databricks Runtime, AWS Glue, Amazon EMR.
- **Data Warehouses**: Snowflake, BigQuery, PostgreSQL, DuckDB, SQLite.
- **Orchestration**: Apache Airflow 2.x, Dagster, Prefect, GitHub Actions CI/CD.
- **Observability**: Rich Terminal Dashboards, Slack Block Kit Cards, Zoho Cliq Webhooks, HTML Data Docs.
