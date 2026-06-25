# Project 1: "The API timed out at 3 AM and nobody knew until the morning dashboard was blank" (Retry-Aware ETL + Dead Letter Queue)

## The Real-World Problem
External vendor REST APIs, payment gateways, and third-party SaaS endpoints fail intermittently in production:
- Temporary network glitches or Cloudflare rate-limiting cause random connection timeouts.
- A single unhandled timeout fails an entire monolithic DAG at 3 AM.
- By 8 AM, the executive dashboard is blank, SLAs are breached, and data engineers must manually re-trigger jobs.
- Furthermore, when the pipeline *does* run, a handful of malformed records (null IDs, negative prices) cause downstream database crashes or silent metric corruption.

Failing 100,000 valid records because 7 rows have dirty data stops business operations; silently dropping the corrupt rows causes untraceable revenue discrepancies.

---

## The Solution: Resilient Retries & Dead Letter Queue Pattern
This project implements an enterprise-grade Airflow ingestion pattern combining:
1. **Exponential backoff retries** on transient network failures.
2. **Atomic row-level validation** separating clean vs corrupt payloads.
3. **Dead Letter Queue (DLQ)** isolation with complete forensic metadata (`order_id`, `failure_reason`, `dag_run_id`, timestamp).
4. **Structured Slack/Teams Block Kit alerting** providing immediate visibility.

```
                  Third-Party Vendor REST API
                              │
                              ▼
           ┌──────────────────────────────────────┐
           │   Task 1: extract_from_api           │
           │   • retries=3                        │
           │   • retry_delay=5s                   │
           │   • exponential_backoff=True         │
           └──────────────────┬───────────────────┘
                              │ (XCom Payload)
                              ▼
           ┌──────────────────────────────────────┐
           │   Task 2: validate_and_load          │
           │   Row-by-Row Semantic Integrity Gate │
           └──────────┬────────────────┬──────────┘
                      │                │
            [Valid Records]     [Corrupt Records]
                      │                │
                      ▼                ▼
        ┌──────────────────┐    ┌──────────────────────┐
        │  Staging SQLite  │    │  Dead Letter Queue   │
        │  (UPSERT / Safe) │    │  (Forensic Metadata) │
        └─────────────┬────┘    └──────────────┬───────┘
                      │                        │
                      └───────────┬────────────┘
                                  ▼
           ┌──────────────────────────────────────┐
           │   Task 3: send_summary_notification  │
           │   Slack Block Kit Channel Alert      │
           └──────────────────────────────────────┘
```

---

## Invariants & Features Demonstrated
| Capability | Airflow / Python Implementation | Production Impact |
| :--- | :--- | :--- |
| **Transient Retry Safety** | `retries=3`, `retry_exponential_backoff=True` | Absorbs temporary 504 Gateway Timeouts without human intervention |
| **Row Isolation** | Stream splitting into clean vs quarantine lists | Zero data loss: 99% valid data flows forward without blockage |
| **DLQ Store** | SQLite table `vendor_orders_dlq` | Captures raw payload + exact reason (`NULL_PRIMARY_KEY`, `NON_POSITIVE_PRICE`) |
| **UPSERT Idempotency** | `INSERT OR REPLACE INTO staging_vendor_orders` | Re-running the task 10 times results in identical table counts |
| **Operational Alerting** | Block Kit payload generator | Instant breakdown of extracted, staged, and quarantined counts |

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python airflow/01-retry-etl-dead-letter-queue/demo.py
```

### 2. Run Automated Pytest Suite
```bash
.venv/bin/pytest airflow/01-retry-etl-dead-letter-queue/tests/ -v
```
