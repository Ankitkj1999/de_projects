# Project 3: "The job was green but loaded half the data" (Volume & Freshness Anomaly Monitor)

## The Real-World Problem
Upstream ETL jobs, Airflow DAGs, and ingestion workers routinely finish with exit code `0` (Success) despite catastrophic silent data loss:
- An API pagination loop silently breaks after page 1, loading 350 rows instead of 2,500 rows.
- A partition timestamp filter `WHERE date = CURRENT_DATE()` misses data due to timezones, loading stale data from 2 weeks ago.
- An upstream vendor bug leaves 18% of mandatory fields null.

Without proactive volume, freshness, and null-rate monitoring, downstream tables and dashboards operate on truncated, stale datasets for weeks before stakeholders notice.

---

## Statistical Baselines & Multi-Tier Severity Architecture

```
           Incoming Ingestion Batch
                      │
                      ▼
         ┌─────────────────────────┐
         │ Historical Baseline     │
         │ Trailing 7-Day Stats    │
         │ Mean: 2,500 | Std: ~95  │
         └────────────┬────────────┘
                      │
        ┌─────────────┴─────────────┐
        ▼                           ▼
┌─────────────────────────┐ ┌─────────────────────────┐
│     CRITICAL CHECKS     │ │     WARNING CHECKS      │
│ (Hard Fail - Halt Run)  │ │ (Soft Alert - Continue) │
├─────────────────────────┤ ├─────────────────────────┤
│ • Row Count < 3-sigma   │ │ • Row Count < 1.8-sigma │
│ • Staleness > 48 Hours  │ │ • Staleness > 24 Hours  │
│ • PK Nulls > 0.0%       │ │ • Optional Nulls > 5.0% │
│ • Required Nulls > 2.0% │ │                         │
└───────────┬─────────────┘ └───────────┬─────────────┘
            │                           │
         [Breach]                    [Breach]
            ▼                           ▼
┌─────────────────────────┐ ┌─────────────────────────┐
│    Pipeline Aborted     │ │    Warning Dispatched   │
│  PagerDuty / Slack P1   │ │     Slack Channel P3    │
│  Prevent Downstream ETL │ │    Pipeline Proceeds    │
└─────────────────────────┘ └─────────────────────────┘
```

---

## Tolerance Modeling with `mostly=`
In production, data is rarely 100% clean. Using naive `ExpectColumnValuesToNotBeNull()` causes alert fatigue and stops pipelines for 1 accidental missing row.

Great Expectations provides `mostly=`:
- **`mostly=0.98` on `customer_id`**: Allows up to 2% expected vendor dropouts without failing the job. If upstream dropouts spike to 18%, it triggers an immediate hard failure.
- **`mostly=0.95` on `order_approved_at`**: Tolerates unapproved or pending orders gracefully while monitoring the drift trend.

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python data-quality/03-volume-freshness-anomaly/demo.py
```

### 2. Run the Test Suite
```bash
.venv/bin/pytest data-quality/03-volume-freshness-anomaly/tests/ -v
```

