# Project 2: "We onboard a new client every week and copy-pasting DAGs is becoming a nightmare" (Dynamic DAG Factory)

## The Real-World Problem
In growing B2B SaaS data platforms and enterprise consulting environments, new customers or business units onboard constantly:
- Each tenant requires an identical ETL sequence: Extract &rarr; Validate &rarr; Load &rarr; Notify.
- However, each tenant has different parameters: independent S3 paths, custom database target tables, different row volume thresholds, unique cron schedules, and dedicated Slack channels.
- Copy-pasting DAG files creates massive repository bloat, configuration drift, and maintenance headaches. When a core ETL step changes, engineers must edit 40 separate files.

---

## The Solution: Declarative Dynamic DAG Factory
This project implements a single, unified Airflow **DAG Factory** pattern driven entirely by declarative YAML:
1. `config/clients.yaml` defines tenant SLAs, schedules, validation thresholds, and storage targets.
2. The Airflow Scheduler parses the factory file and dynamically instantiates an independent DAG per tenant into `globals()`.
3. Adding a new client requires editing **only 1 YAML block** — zero new Python files, zero DAG template duplication.

```
                  config/clients.yaml
             (Declarative Tenant Contracts)
                           │
                           ▼
             ┌───────────────────────────┐
             │ Airflow Dynamic Factory   │
             │ Programmatic DAG Compiler │
             └─────────────┬─────────────┘
                           │ globals()[dag_id] = dag
        ┌──────────────────┼──────────────────┐
        │                  │                  │
        ▼                  ▼                  ▼
┌──────────────┐   ┌──────────────┐   ┌──────────────┐
│ etl_acme     │   │ etl_globex   │   │ etl_umbrella │
│ @daily       │   │ @daily       │   │ 0 4 * * *    │
│ table: acme  │   │ table: globex│   │ table: umbr  │
└───────┬──────┘   └───────┬──────┘   └───────┬──────┘
        │                  │                  │
        ▼                  ▼                  ▼
  [Extract Task]    [Extract Task]     [Extract Task]
        │                  │                  │
  [Validate Task]   [Validate Task]    [Validate Task]
        │                  │                  │
  [Load Task]       [Load Task]        [Load Task]
        │                  │                  │
  [Notify Task]     [Notify Task]      [Notify Task]
```

---

## Invariants & Features Demonstrated
| Capability | Implementation | Production Impact |
| :--- | :--- | :--- |
| **Declarative Onboarding** | `config/clients.yaml` | Non-engineers can configure new tenants in 2 minutes |
| **Parse-Time Generation** | Python `globals()[dag_id]` registration | Airflow UI automatically reflects new DAGs on scheduler cycle |
| **Tenant Isolation** | Per-tenant SQLite tables (`client_orders_{tenant}`) | Strict multi-tenant segregation, preventing cross-tenant leakage |
| **Dynamic Validation** | Tenant-specific `min_row_threshold` & `max_price_ceiling` | Tailored data quality checks per client volume profile |
| **SLA & Alert Routing** | Per-tenant Slack channel targeting (`#de-{tenant}-alerts`) | Alerts reach the relevant customer success and support channels |

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python airflow/02-dynamic-dag-factory/demo.py
```

### 2. Run Automated Pytest Suite
```bash
.venv/bin/pytest airflow/02-dynamic-dag-factory/tests/ -v
```
