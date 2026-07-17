# Project 2: "The dashboard is wrong but nothing failed" (Business Rules Validator)

## The Real-World Problem
Syntactic checks (types and nulls) pass without errors, ETL jobs finish green, but executive dashboards show invalid figures:
- Revenue reports show negative charges or inflated figures.
- Future-dated orders distort cohort and lifetime-value (LTV) analyses.
- Delivery lead times display negative durations (orders delivered before being purchased).
- Phantom SKUs appear in inventory reports because foreign keys broke upstream.
- Total order payment doesn't reconcile with the itemized cart sum.

These are **semantic business rule violations**. Silent logical corruption is the leading cause of lost executive trust in data teams.

---

## Injected Corruptions & Great Expectations Defense Matrix

| # | Business Failure Scenario | Injected Corruption | Great Expectations Detector | Business Impact Prevented |
| :--- | :--- | :--- | :--- | :--- |
| **1** | Negative prices / fees | Unit price = `-\$149.99` | `ExpectColumnValuesToBeBetween(min_value=0.01)` on `price` | Prevents distorted GMV & negative revenue lines |
| **2** | Future-dated orders | Purchase date = 4 months ahead | `ExpectColumnValuesToBeBetween(max_value=CURRENT_TIMESTAMP)` | Prevents false spikes in future forecast models |
| **3** | Temporal causality paradox | Delivery date 5 days before order date | `ExpectColumnPairValuesAToBeGreaterThanB(A="delivered", B="purchased")` | Prevents invalid SLA calculations and negative transit days |
| **4** | Phantom SKU (FK orphan) | `product_id = "ghost_sku_..."` | `ExpectColumnValuesToBeInSet(value_set=catalog_skus)` | Prevents unjoinable inventory records |
| **5** | Duplicate order ID | Ingestion retry duplicated an order | `ExpectColumnValuesToBeUnique(column="order_id")` | Prevents double-counting revenue and order counts |
| **6** | Financial mismatch | Payment = `\$9999.99`, Cart = `\$120.00` | Multi-table reconciliation assertion: `payment_value == sum(price + freight)` | Stops financial ledger reconciliation errors |

---

## How to Run

### 1. Run the Interactive Demo
```bash
.venv/bin/python data-quality/02-business-rules-validator/demo.py
```

### 2. Run the Unit & Corruption Detection Tests
```bash
.venv/bin/pytest data-quality/02-business-rules-validator/tests/ -v
```

