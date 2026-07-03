"""
src/validator.py — Parameterized Tenant Data Quality Validator
==============================================================
Real-World Problem:
    Each tenant has unique volume baselines and contractual business invariants.
    Hardcoding thresholds in pipeline code guarantees false positives for small clients
    and missed incidents for large clients.
    
    This module evaluates validation rules dynamically against client YAML configs:
      - Threshold checking (minimum expected row volume)
      - Key integrity (order_id uniqueness and non-nullness)
      - Range boundaries (positive pricing under contractual maximum ceiling)
"""

from __future__ import annotations

from typing import Any, Dict, List


def validate(client_config: Dict[str, Any], records: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validates tenant data against YAML-defined SLA invariants.
    """
    client_id = client_config["client_id"]
    min_rows = client_config.get("min_row_threshold", 50)
    max_price = float(client_config.get("max_price_ceiling", 100000.0))

    violations = []
    seen_ids = set()

    # Rule 1: Volume Threshold
    if len(records) < min_rows:
        violations.append({
            "rule": "MIN_ROW_THRESHOLD_BREACH",
            "expected_min": min_rows,
            "actual": len(records),
            "severity": "CRITICAL",
        })

    # Rule 2: Row-level checks
    for idx, r in enumerate(records):
        oid = r.get("order_id")
        if not oid:
            violations.append({"rule": "NULL_ORDER_ID", "row_index": idx, "severity": "CRITICAL"})
        elif oid in seen_ids:
            violations.append({"rule": "DUPLICATE_ORDER_ID", "order_id": oid, "severity": "CRITICAL"})
        else:
            seen_ids.add(oid)

        try:
            p = float(r.get("price", 0))
            if p <= 0:
                violations.append({"rule": "NON_POSITIVE_PRICE", "order_id": oid, "price": p, "severity": "CRITICAL"})
            elif p > max_price:
                violations.append({"rule": "PRICE_CEILING_EXCEEDED", "order_id": oid, "price": p, "severity": "WARNING"})
        except (ValueError, TypeError):
            violations.append({"rule": "INVALID_PRICE_TYPE", "order_id": oid, "severity": "CRITICAL"})

    critical_violations = [v for v in violations if v.get("severity") == "CRITICAL"]
    passed = len(critical_violations) == 0

    return {
        "client_id": client_id,
        "passed": passed,
        "row_count": len(records),
        "min_threshold": min_rows,
        "violations_count": len(violations),
        "critical_count": len(critical_violations),
        "violations": violations,
        "status": "PASSED" if passed else "FAILED",
    }
