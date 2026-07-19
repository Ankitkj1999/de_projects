# Project 4: "We re-ran last month's jobs and now every record is duplicated in the warehouse" (Backfill-Safe Idempotent Data Loader)

## The Real-World Problem
Data engineering teams regularly re-run historical DAG runs:
- Backfilling missing history after an ingestion schema fix.
- Re-triggering a DAG run after an upstream transient outage.
- Executing historical reprocessing requested by financial auditors.

In non-idempotent data pipelines built with simple `INSERT INTO warehouse`, re-running a job produces duplicate rows. If last month's 30 daily runs are backfilled, every metric, customer total, and revenue figure doubles overnight. Cleaning up duplicate rows in a 500M row production warehouse is painful, risky, and expensive.

---

## The Solution: Deterministic Ingestion & UPSERT Partitions
This project implements the foundational golden rule of data engineering: **all pipeline tasks must be strictly idempotent**.
1. **Logical Date (`{{ ds }}`) Partitioning**: Every extraction and loading operation is strictly keyed by Airflow's logical execution date (`ds`), never `datetime.now()`.
2. **Deterministic Extraction**: Seeding extraction RNG with the logical date string guarantees that re-running the same date produces the exact same records.
3. **Atomic UPSERT (`INSERT OR REPLACE`)**: Keyed on composite primary key `(order_id, partition_date)`. Re-running a day 1 time or 100 times yields the exact same row count.
4. **Idempotency Gate & Audit Ledger**: Automatically checks whether a partition has already been loaded, flags backfill runs, and logs compliance audit records to JSON.

```
                    Airflow Execution Date: {{ ds }}
                                  │
                                  ▼
                ┌───────────────────────────────────┐
                │ Task 1: check_partition_exists    │
                │ Queries sales_partitions for {{ds}}│
                └─────────────────┬─────────────────┘
                                  │
                                  ▼
                ┌───────────────────────────────────┐
                │ Task 2: extract_daily_partition   │
                │ Deterministic Extractor (Seed: ds)│
                └─────────────────┬─────────────────┘
                                  │
                                  ▼
                ┌───────────────────────────────────┐
                │ Task 3: upsert_to_warehouse       │
                │ INSERT OR REPLACE INTO partitions │
                │ (order_id, partition_date) PK     │
                └─────────────────┬─────────────────┘
                                  │
                                  ▼
                ┌───────────────────────────────────┐
                │ Task 4: verify_idempotency        │
                │ Assert: total_rows == unique_keys │
                └─────────────────┬─────────────────┘
                                  │
                                  ▼
                ┌───────────────────────────────────┐
                │ Task 5: log_audit_trail           │
                │ Lineage Ledger: is_backfill=True  │
                └───────────────────────────────────┘
```

---

## Comparison: Naive INSERT vs Idempotent UPSERT
| Dimension | Naive Pipeline (`INSERT INTO`) | Idempotent Pipeline (This Project) |
| :--- | :--- | :--- |
| **First Execution** | Loads 200 rows | Loads 200 rows |
| **Manual Retry / Re-run** | Loads 200 rows &rarr; **400 rows (Duplicated!)** | Updates 200 rows &rarr; **200 rows (Clean!)** |
| **Backfill 30 Days** | Duplicates entire month | Safely overwrites/updates partition |
| **Execution Date Usage** | Uses system `datetime.now()` | Uses Airflow `{{ ds }}` logical macro |
| **Forensic Audit** | Zero visibility on re-runs | Flags `is_backfill=True` in audit ledger |

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python airflow/04-idempotent-backfill-loader/demo.py
```

### 2. Run Automated Pytest Suite
```bash
.venv/bin/pytest airflow/04-idempotent-backfill-loader/tests/ -v
```
