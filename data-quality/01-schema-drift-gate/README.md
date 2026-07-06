# Project 1: "The vendor renamed a column and nobody noticed" (Schema Drift Gate)

## The Real-World Problem
Third-party vendor feeds, partner APIs, or upstream SaaS integrations frequently change schemas without prior notification:
- Columns are silently renamed (e.g. `order_purchase_timestamp` &rarr; `purchase_date`).
- Required fields are omitted.
- Unannounced tracking fields are appended.
- Categorical enum values drift (e.g. `PENDING_REVIEW` instead of `processing`).

When these feeds land directly into Bronze/Silver delta tables, downstream dbt models, partition schemes, and BI dashboards break silently or throw obscure runtime exceptions hours later.

---

## The Solution: Strict Pre-Ingestion Validation Gate
This project implements an automated Great Expectations pre-ingestion validation gate that sits between raw landing feeds and the Bronze storage layer.

```
Incoming CSV/JSON Feed
         │
         ▼
┌─────────────────────────────────┐
│   Schema Validation Gate (GX)   │
│ 1. Column Set & Strict Order    │
│ 2. Primary Key Non-Nullness     │
│ 3. Categorical Enum Whitelist   │
│ 4. Timestamp ISO Regex Check    │
└────────┬──────────────┬─────────┘
         │              │
     [Pass]           [Fail]
         │              │
         ▼              ▼
┌────────────────┐ ┌───────────────────────────┐
│  Bronze Layer  │ │  Halt Pipeline (Exit 1)   │
│  Valid Storage │ │  Generate Drift Incident  │
│  Ready for ETL │ │  Alert Data Engineering   │
└────────────────┘ └───────────────────────────┘
```

---

## Validation Invariants Checked
| Expectation | Field Checked | Rule | Failure Impact |
| :--- | :--- | :--- | :--- |
| `ExpectTableColumnsToMatchOrderedList` | Table-level | Exact list & ordering of 8 canonical columns | Halts on renamed, dropped, or swapped columns |
| `ExpectColumnValuesToNotBeNull` | `order_id`, `customer_id`, `order_status`, `order_purchase_timestamp` | 100% non-null required keys | Prevents orphan records |
| `ExpectColumnValuesToBeInSet` | `order_status` | Allowed set: `delivered`, `shipped`, `invoiced`, `processing`, `approved`, `canceled`, `unavailable` | Prevents corrupt status codes |
| `ExpectColumnValuesToMatchRegex` | `order_purchase_timestamp` | Strict ISO datetime pattern `YYYY-MM-DD HH:MM:SS` | Prevents unparseable date formats |

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python data-quality/01-schema-drift-gate/demo.py
```

### 2. Run Direct CLI Command
```bash
# Valid feed:
.venv/bin/python data-quality/01-schema-drift-gate/src/schema_gate.py data/raw/olist_orders_dataset.csv

# Drifted feed (Vendor renamed order_purchase_timestamp -> purchase_date):
.venv/bin/python data-quality/01-schema-drift-gate/src/schema_gate.py data/corrupt/orders_vendor_renamed_drift.csv
```

### 3. Run Automated Tests
```bash
.venv/bin/pytest data-quality/01-schema-drift-gate/tests/ -v
```

