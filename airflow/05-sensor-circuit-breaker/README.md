# Project 5: "Our pipeline starts processing before the upstream file is even fully uploaded" (Event-Driven Sensor & Circuit Breaker)

## The Real-World Problem
In decoupled enterprise data architectures, upstream systems dump multi-gigabyte files (CSV, Parquet, JSON) into shared object storage (S3, ADLS, HDFS) at unpredictable times:
- If a downstream Airflow DAG triggers on a fixed cron schedule, it frequently begins reading while the upstream system is still streaming bytes.
- This results in corrupted DataFrame parsing, partial loads, and silent data loss.
- Furthermore, if the file never arrives (e.g. upstream pipeline crashed), a naive sensor polls indefinitely, exhausting Airflow worker task slots, before violently failing the DAG and causing false-alarm alert panic.

---

## The Solution: Two-Phase Marker Contract & Circuit Breaker
This project implements the industry-standard two-phase write pattern (used by Hadoop, Apache Spark, Databricks, and AWS Glue) combined with an automated Circuit Breaker:
1. **Upstream Atomic `_SUCCESS` Marker**: The upstream producer writes the complete data file first, and only upon completion atomically writes a companion `_SUCCESS_{ds}` metadata marker containing row counts and SHA-256 checksums.
2. **Downstream FileSensor**: The downstream DAG senses the `_SUCCESS` marker file — never the raw data file.
3. **Checksum Verification**: The processor validates that the incoming file's SHA-256 hash matches the marker.
4. **Circuit Breaker Branch (`BranchPythonOperator`)**: If the sensor times out (`soft_fail=True`), the pipeline gracefully trips the circuit breaker, logs an incident, and skips downstream load tasks rather than crashing.

```
                           UPSTREAM WRITER
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │ 1. Write CSV Data File   │
                     │    (orders_2024-01-15)   │
                     └────────────┬─────────────┘
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │ 2. Drop Atomic Marker    │
                     │    (_SUCCESS_2024-01-15) │
                     └────────────┬─────────────┘
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │ DOWNSTREAM SENSOR        │
                     │ wait_for_success_marker  │
                     │ (poke_interval, timeout) │
                     └────────────┬─────────────┘
                                  │
                                  ▼
                     ┌──────────────────────────┐
                     │ check_sensor_result      │
                     │ (BranchPythonOperator)   │
                     └──────┬────────────┬──────┘
                            │            │
                     [Marker Found] [Timeout Exceeded]
                            │            │
                            ▼            ▼
             ┌────────────────────┐ ┌────────────────────────┐
             │ process_orders     │ │ fire_circuit_breaker   │
             │ (Verify SHA-256)   │ │ (soft_fail, Log Event, │
             └─────────┬──────────┘ │  Skip Downstream ETL)  │
                       │            └────────────────────────┘
                       ▼
             ┌────────────────────┐
             │ load_to_warehouse  │
             │ (Commit to SQLite) │
             └────────────────────┘
```

---

## Invariants & Features Demonstrated
| Capability | Airflow / Python Feature | Production Impact |
| :--- | :--- | :--- |
| **Two-Phase Commit** | `_SUCCESS` marker file drop | Prevents reading partially-written files mid-upload |
| **Cryptographic Integrity** | SHA-256 hash checksum matching | Guarantees zero byte corruption or transmission tampering |
| **Slot Conservation** | Sensor timeout + `soft_fail=True` | Frees up Airflow worker slots instead of hanging indefinitely |
| **Graceful Degradation** | `BranchPythonOperator` Circuit Breaker | Logs operational incident and skips cleanly without red alerts |
| **Warehouse Isolation** | SQLite commit on verified batches only | Zero invalid data ever penetrates the warehouse layer |

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python airflow/05-sensor-circuit-breaker/demo.py
```

### 2. Run Automated Pytest Suite
```bash
.venv/bin/pytest airflow/05-sensor-circuit-breaker/tests/ -v
```
