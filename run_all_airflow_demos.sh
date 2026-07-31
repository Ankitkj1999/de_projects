#!/usr/bin/env bash
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PYTHON=".venv/bin/python"

echo "=========================================================================="
echo "    RUNNING ALL 5 AIRFLOW REAL-WORLD PROBLEM SOLVING PORTFOLIO DEMOS      "
echo "=========================================================================="
echo ""

echo ">>> [1/5] Running Project 01: Retry-Aware ETL + Dead Letter Queue..."
$PYTHON airflow/01-retry-etl-dead-letter-queue/demo.py
echo ""

echo ">>> [2/5] Running Project 02: Dynamic DAG Factory for Multi-Tenant Clients..."
$PYTHON airflow/02-dynamic-dag-factory/demo.py
echo ""

echo ">>> [3/5] Running Project 03: SLA-Monitored Financial Report Pipeline..."
$PYTHON airflow/03-sla-finance-pipeline/demo.py
echo ""

echo ">>> [4/5] Running Project 04: Backfill-Safe Idempotent Data Loader..."
$PYTHON airflow/04-idempotent-backfill-loader/demo.py
echo ""

echo ">>> [5/5] Running Project 05: Event-Driven Sensor + Circuit Breaker..."
$PYTHON airflow/05-sensor-circuit-breaker/demo.py
echo ""

echo "=========================================================================="
echo "    ✔ ALL 5 AIRFLOW PRODUCTION PORTFOLIO DEMOS COMPLETED SUCCESSFULLY!    "
echo "=========================================================================="
