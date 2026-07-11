# Project 3: "The CFO's daily revenue report sometimes shows yesterday's numbers — and nobody gets alerted" (SLA-Monitored Financial Report Pipeline)

## The Real-World Problem
In financial data engineering, publishing an executive revenue report on stale numbers is unacceptable:
- If an upstream database backup or CDC replication pipeline stalls overnight, the 6:00 AM finance reporting DAG extracts yesterday's records and presents them as today's revenue.
- If heavy aggregation queries run slow due to resource contention, the job silently misses its 8:00 AM executive delivery deadline without notifying on-call engineers.
- By the time the CFO discovers the report is missing or wrong during the morning executive sync, the data team is on the defensive.

---

## The Solution: Freshness ShortCircuit & SLA Miss Callbacks
This project implements production Airflow observability and SLA governance:
1. **Freshness Gate (`ShortCircuitOperator`)**: Evaluates upstream data timestamps. If data is >24 hours stale, the pipeline gracefully skips all downstream tasks rather than calculating bad numbers.
2. **Task-Level SLAs (`sla=timedelta(...)`)**: Establishes strict time budgets for extraction, FX conversion, and aggregation.
3. **Automated SLA Miss Hooks (`sla_miss_callback`)**: Automatically triggers Slack notifications and forensic logs if any task exceeds its SLA budget.
4. **Execution Timeouts (`execution_timeout=...`)**: Kills runaway hanging queries before they exhaust cluster resources.

```
                          Upstream Source Data
                                   │
                                   ▼
                   ┌───────────────────────────────┐
                   │ Task 1: freshness_check       │
                   │ (ShortCircuitOperator, sla=5m)│
                   └───────┬───────────────┬───────┘
                           │               │
                     [Data Fresh]    [Data Stale >24h]
                           │               │
                           ▼               ▼
           ┌───────────────────────────┐ ┌───────────────────────────┐
           │ Task 2: extract_raw_sales │ │ Graceful Short-Circuit    │
           │ (sla=15m, timeout=20m)    │ │ Downstream Tasks SKIPPED  │
           └───────────────┬───────────┘ └───────────────────────────┘
                           │
                           ▼
           ┌───────────────────────────┐
           │ Task 3: apply_fx_conv     │
           │ (BRL -> USD, sla=10m)     │
           └───────────────┬───────────┘
                           │
                           ▼
           ┌───────────────────────────┐
           │ Task 4: aggregate_revenue │ ──> [Breaches SLA?] ──> [sla_miss_callback]
           │ (sla=10m, timeout=15m)    │                         Alerts On-Call DE
           └───────────────┬───────────┘
                           │
                           ▼
           ┌───────────────────────────┐
           │ Task 5: generate_report   │
           │ (Persist JSON Artifact)   │
           └───────────────┬───────────┘
                           │
                           ▼
           ┌───────────────────────────┐
           │ Task 6: notify_cfo        │
           │ (Executive Dashboard Card)│
           └───────────────────────────┘
```

---

## Invariants & Features Demonstrated
| Capability | Airflow / Python Feature | Production Impact |
| :--- | :--- | :--- |
| **Data Freshness Guard** | `ShortCircuitOperator` | Halts execution before calculating reports on stale upstream tables |
| **SLA Breach Alerting** | `DAG(sla_miss_callback=on_sla_miss)` | Alerts on-call engineers within seconds of SLA breaches |
| **Runaway Query Protection** | `execution_timeout=timedelta(minutes=15)` | Kills hung database queries before blocking other tenant workloads |
| **Currency Normalization** | Daily FX rate conversion (BRL &rarr; USD) | Harmonizes multi-currency sales into corporate ledger standards |
| **Audit Artifact** | `revenue_report_{ds}.json` | Immutable audited ledger persisted for accounting verification |

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python airflow/03-sla-finance-pipeline/demo.py
```

### 2. Run Automated Pytest Suite
```bash
.venv/bin/pytest airflow/03-sla-finance-pipeline/tests/ -v
```
