# Project 4: "One bad row shouldn’t block 10M good ones" (Delta Lake Quarantine Pattern)

## The Real-World Problem
In modern high-throughput Lakehouse architectures (Databricks, AWS Glue, EMR, Snowflake), **all-or-nothing batch validation is a major bottleneck**:
- If an ingestion batch of 10,000,000 rows contains just 50 records with an unexpected status or negative amount, failing the entire job halts data pipelines for the entire company.
- Conversely, silently dropping bad rows (`df.filter(...)`) causes silent data loss, breaks financial reconciliation, and violates data governance and compliance audits.

---

## The Solution: The Delta Lake Quarantine Pattern
Instead of failing the pipeline or silently discarding data, this pattern:
1. Ingests raw data into the **Bronze** layer.
2. Applies row-level **Great Expectations** assertions in a vectorized pass.
3. Automatically branches the dataset:
   - **Silver Delta Table**: Valid rows are committed and made immediately available for downstream analytics and dbt models.
   - **Quarantine / Rejects Delta Table**: Invalid rows are routed to an isolated table, enriched with diagnostic metadata (`rejection_reason`, `failed_expectations`, `quarantined_at`, `pipeline_run_id`).
4. Enforces an automated **Circuit Breaker**: If `reject_rate > threshold` (e.g. >10%), the pipeline halts immediately to prevent writing runaway corrupt feeds to Silver.
5. Guarantees **Zero Data Loss**: $\text{Count(Silver)} + \text{Count(Quarantine)} = \text{Count(Bronze)}$.

```
                      Bronze Raw Records (10M Rows)
                                     │
                                     ▼
                      ┌─────────────────────────────┐
                      │    GX Vectorized Evaluator  │
                      │  • Non-null Primary Keys    │
                      │  • Status Enum Invariants   │
                      │  • Temporal Causality Rules │
                      └──────────────┬──────────────┘
                                     │
                 ┌───────────────────┴───────────────────┐
                 │                                       │
           [Valid Rows]                            [Invalid Rows]
                 │                                       │
                 ▼                                       ▼
    ┌───────────────────────────┐           ┌───────────────────────────┐
    │     Silver Delta Table    │           │   Quarantine Delta Table  │
    │   • Clean, trusted data   │           │   • Rejection Reason      │
    │   • Ready for downstream  │           │   • Failed Expectations   │
    │   • ACID Append           │           │   • Quarantined Timestamp │
    └───────────────────────────┘           └─────────────┬─────────────┘
                                                          │
                                                          ▼
                                            ┌───────────────────────────┐
                                            │      Circuit Breaker      │
                                            │ Reject Rate > Threshold?  │
                                            │ Yes -> Halt Ingestion     │
                                            │ No  -> Alert Data Ops     │
                                            └───────────────────────────┘
```

---

## Databricks & AWS Glue PySpark Implementation
In addition to the local Delta Lake engine, this project includes a ready-to-deploy PySpark notebook/script:
[pyspark_quarantine_pipeline.py](file:///home/ankit/Documents/Code/Projects/de_projects/data-quality/04-delta-lake-quarantine/notebooks/pyspark_quarantine_pipeline.py)
that runs natively on Spark clusters (Databricks Runtime, AWS Glue, Amazon EMR).

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python data-quality/04-delta-lake-quarantine/demo.py
```

### 2. Run the Test Suite
```bash
.venv/bin/pytest data-quality/04-delta-lake-quarantine/tests/ -v
```

