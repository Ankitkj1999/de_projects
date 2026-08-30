# Production Data Quality Engineering with Great Expectations

[![Data Quality CI](https://img.shields.io/badge/CI%2FCD-GitHub%20Actions-blue.svg)](.github/workflows/data_quality_ci.yml)
[![Great Expectations](https://img.shields.io/badge/Great%20Expectations-1.24.1-orange.svg)](https://greatexpectations.io)
[![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake-00A4E4.svg)](https://delta.io)
[![Python 3.12](https://img.shields.io/badge/Python-3.12-3776AB.svg)](https://python.org)
[![Tests Passing](https://img.shields.io/badge/Tests-24%20Passing-brightgreen.svg)](#test-suite-results)

An end-to-end, production-grade Data Quality engineering portfolio solving the 5 most critical data degradation failure modes encountered in enterprise data platforms (Databricks, AWS Glue, Snowflake, BigQuery, dbt).

Built on top of a realistic **Brazilian E-Commerce (Olist)** dataset with reproducible, injected real-world corruptions.

---

## The 5 Data Quality Projects Overview

```
                                  INCOMING DATA FEEDS
                                           │
  ┌────────────────────────────────────────┼────────────────────────────────────────┐
  │                                        │                                        │
  ▼                                        ▼                                        ▼
┌─────────────────────────┐  ┌─────────────────────────┐  ┌─────────────────────────┐
│ Project 1: Schema Drift │  │ Project 2: Biz Rules    │  │ Project 3: Volume &     │
│ Pre-Bronze Ingestion    │  │ Semantic Integrity      │  │ Freshness Anomalies     │
│ Gate (Columns, Order,   │  │ FK Catalog Checks,      │  │ Rolling 7-Day Baseline, │
│ Enums, Types)           │  │ Totals, No Future Dates │  │ Hard Fail vs Warning    │
└────────────┬────────────┘  └────────────┬────────────┘  └────────────┬────────────┘
             │                            │                            │
             └─────────────────────┬──────┴────────────────────────────┘
                                   │
                                   ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│ Project 4: High-Volume Bronze → Silver Delta Lake Quarantine Pattern              │
│ Splits valid vs invalid rows • Writes Rejects to Delta with Reason • Circuit Breaker│
└──────────────────────────────────┬────────────────────────────────────────────────┘
                                   │
                                   ▼
┌───────────────────────────────────────────────────────────────────────────────────┐
│ Project 5: Cold-Start Profiling, Curated Suites, Data Docs & Alerting Webhooks    │
│ Scans 200 Untested Tables • Prunes Brittle Rules • Publishes Docs • Slack/Cliq CI │
└───────────────────────────────────────────────────────────────────────────────────┘
```

---

## Projects Detailed Matrix

| # | Project Name & Scenario | Problem Solved | Key Expectations & Patterns | Architecture & Stack |
|---|-------------------------|----------------|-----------------------------|----------------------|
| **1** | [**Schema Drift Validation Gate**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/01-schema-drift-gate) <br> *"The vendor renamed a column and nobody noticed"* | Vendor feeds unannounced rename (`order_purchase_timestamp` &rarr; `purchase_date`), swapped columns, or illegal status codes. | `ExpectTableColumnsToMatchOrderedList`<br>`ExpectColumnValuesToBeInSet`<br>`ExpectColumnValuesToNotBeNull`<br>`ExpectColumnValuesToMatchRegex` | Python CLI, Pre-Bronze Gate, Incident JSON report |
| **2** | [**Business Rules Validator**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/02-business-rules-validator) <br> *"The dashboard is wrong but nothing failed"* | Syntactic checks pass, but revenue reports show negative prices, future dates, broken foreign keys, or orders delivered before purchase. | `ExpectColumnValuesToBeBetween`<br>`ExpectColumnValuesToBeUnique`<br>`ExpectColumnValuesToBeInSet`<br>`ExpectColumnPairValuesAToBeGreaterThanB`<br>Cart-to-Payment Reconciliation | Multi-table relational validator, Diagnostic failure matrix |
| **3** | [**Volume & Freshness Monitor**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/03-volume-freshness-anomaly) <br> *"The job was green but loaded half the data"* | Silent pagination failure dropped 86% of records; partition bug loaded stale data from 2 weeks ago; null rate spiked to 18%. | `ExpectTableRowCountToBeBetween`<br>`ExpectColumnMaxToBeBetween`<br>`ExpectColumnValuesToNotBeNull(mostly=0.98)` | Rolling 7-day baseline tracker, Multi-tier severity (Hard Fail vs Warning) |
| **4** | [**Delta Lake Quarantine Pattern**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/04-delta-lake-quarantine) <br> *"One bad row shouldn't block 10M good ones"* | Failing 10M rows for 50 bad records halts business SLAs; silently dropping rows causes unrecoverable data loss. | Vectorized row-level GX rules<br>Silver Delta table commit<br>Quarantine Delta table with failure reasons<br>Automated Circuit Breaker | Native Delta Lake (`deltalake`), PySpark Glue / Databricks script, Zero data loss |
| **5** | [**Cold-Start Profiling & Docs**](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/05-automated-table-profiling) <br> *"I inherited 200 tables with zero tests"* | Joining an enterprise data platform with hundreds of unmonitored tables and no tests. Manual rule authoring is impossible. | Automated Statistical Profiler<br>Intelligent Signal-to-Noise Pruner<br>Static HTML Data Docs Generator<br>Slack & Zoho Cliq Alert Webhooks | Ephemeral Data Context, Jinja2 Data Docs, Webhook Notifier, GitHub Actions CI |

---

## Directory Structure

```
de_projects/
├── .github/
│   └── workflows/
│       └── data_quality_ci.yml         # GitHub Actions CI check on PRs
├── data/
│   ├── generate_olist_data.py          # Synthetic high-fidelity Olist generator
│   ├── inject_corruptions.py           # Injects 5 realistic failure scenarios
│   ├── raw/                            # Clean baseline tables (orders, items, payments, etc.)
│   ├── corrupt/                        # Corrupted feeds for testing
│   └── processed/
│       ├── bronze/                     # Gate-approved raw landing files
│       └── delta/                      # Silver and Quarantine Delta Lake tables
├── data-quality/
│   ├── 01-schema-drift-gate/           # Project 1: Schema Drift Gate
│   │   ├── src/schema_gate.py
│   │   ├── tests/test_schema_drift.py
│   │   ├── demo.py
│   │   └── README.md
│   ├── 02-business-rules-validator/    # Project 2: Business Semantic Rules
│   │   ├── suites/ecommerce_rules.py
│   │   ├── src/validator.py
│   │   ├── tests/test_business_rules.py
│   │   ├── demo.py
│   │   └── README.md
│   ├── 03-volume-freshness-anomaly/    # Project 3: Volume & Freshness Monitor
│   │   ├── monitors/baseline_tracker.py
│   │   ├── src/freshness_volume_gate.py
│   │   ├── tests/test_volume_freshness.py
│   │   ├── demo.py
│   │   └── README.md
│   ├── 04-delta-lake-quarantine/       # Project 4: Delta Lake Quarantine Pattern
│   │   ├── src/quarantine_pipeline.py
│   │   ├── notebooks/pyspark_quarantine_pipeline.py
│   │   ├── tests/test_quarantine.py
│   │   ├── demo.py
│   │   └── README.md
│   └── 05-automated-table-profiling/   # Project 5: Cold-Start Profiling & Alerts
│       ├── profiler/auto_profiler.py
│       ├── profiler/suite_pruner.py
│       ├── src/data_docs_publisher.py
│       ├── src/notifier.py
│       ├── tests/test_profiler.py
│       ├── demo.py
│       └── README.md
├── requirements.txt                    # Modern Python package dependencies
├── run_all_demos.sh                    # Orchestrates all 5 live interactive demos
└── README.md                           # Master portfolio documentation
```

---

## Quickstart: Running Demos & Tests

### 1. Run All 5 Interactive Demos in Sequence
```bash
./run_all_demos.sh
```

### 2. Run Individual Project Demos
```bash
# Project 1: Schema Drift
.venv/bin/python data-quality/01-schema-drift-gate/demo.py

# Project 2: Business Rules
.venv/bin/python data-quality/02-business-rules-validator/demo.py

# Project 3: Volume & Freshness Anomaly
.venv/bin/python data-quality/03-volume-freshness-anomaly/demo.py

# Project 4: Delta Lake Quarantine Engine
.venv/bin/python data-quality/04-delta-lake-quarantine/demo.py

# Project 5: Cold-Start Profiler & Data Docs
.venv/bin/python data-quality/05-automated-table-profiling/demo.py
```

### 3. Run the Comprehensive Pytest Suite (24 Test Cases)
```bash
.venv/bin/pytest data-quality/ -v
```

---

## Test Suite Results

```
data-quality/01-schema-drift-gate/tests/test_schema_drift.py::test_clean_feed_passes_gate PASSED
data-quality/01-schema-drift-gate/tests/test_schema_drift.py::test_renamed_column_fails_gate PASSED
data-quality/01-schema-drift-gate/tests/test_schema_drift.py::test_illegal_status_fails_gate PASSED
data-quality/01-schema-drift-gate/tests/test_schema_drift.py::test_missing_column_fails_gate PASSED
data-quality/01-schema-drift-gate/tests/test_schema_drift.py::test_bronze_process_feed_flow PASSED

data-quality/02-business-rules-validator/tests/test_business_rules.py::test_clean_data_passes_all_rules PASSED
data-quality/02-business-rules-validator/tests/test_business_rules.py::test_catches_negative_price PASSED
data-quality/02-business-rules-validator/tests/test_business_rules.py::test_catches_future_dated_orders PASSED
data-quality/02-business-rules-validator/tests/test_business_rules.py::test_catches_temporal_causality_paradox PASSED
data-quality/02-business-rules-validator/tests/test_business_rules.py::test_catches_phantom_sku_catalog_integrity PASSED
data-quality/02-business-rules-validator/tests/test_business_rules.py::test_catches_duplicate_order_id PASSED
data-quality/02-business-rules-validator/tests/test_business_rules.py::test_catches_order_total_mismatch PASSED

data-quality/03-volume-freshness-anomaly/tests/test_volume_freshness.py::test_healthy_batch_passes PASSED
data-quality/03-volume-freshness-anomaly/tests/test_volume_freshness.py::test_warning_level_on_minor_volume_dip PASSED
data-quality/03-volume-freshness-anomaly/tests/test_volume_freshness.py::test_hard_fail_on_half_load_volume_collapse PASSED
data-quality/03-volume-freshness-anomaly/tests/test_volume_freshness.py::test_hard_fail_on_stale_data PASSED
data-quality/03-volume-freshness-anomaly/tests/test_volume_freshness.py::test_mostly_tolerance_allows_small_nulls_but_catches_spikes PASSED

data-quality/04-delta-lake-quarantine/tests/test_quarantine.py::test_row_splitting_and_enrichment PASSED
data-quality/04-delta-lake-quarantine/tests/test_quarantine.py::test_zero_data_loss_and_delta_write PASSED
data-quality/04-delta-lake-quarantine/tests/test_quarantine.py::test_circuit_breaker_trips_on_excessive_corruption PASSED

data-quality/05-automated-table-profiling/tests/test_profiler.py::test_profiler_discovers_schema_and_pk PASSED
data-quality/05-automated-table-profiling/tests/test_profiler.py::test_pruner_filters_brittle_expectations PASSED
data-quality/05-automated-table-profiling/tests/test_profiler.py::test_data_docs_publisher PASSED
data-quality/05-automated-table-profiling/tests/test_profiler.py::test_notifier_payload_generation PASSED

============================== 24 passed in 6.01s ==============================
```

---

## Production Deployment & Stack Compatibility
- **Lakehouse Engines**: Native Delta Lake tables, Apache Spark (PySpark), Databricks Runtime, AWS Glue, Amazon EMR.
- **Data Warehouses**: Snowflake, BigQuery, PostgreSQL, DuckDB.
- **Orchestration**: Apache Airflow, Dagster, Prefect, GitHub Actions.
- **Observability**: Static HTML Data Docs, Slack Webhooks, Zoho Cliq Bot Cards.

