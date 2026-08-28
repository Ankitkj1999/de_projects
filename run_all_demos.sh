#!/usr/bin/env bash
set -e

echo "=========================================================================="
echo "    DATA QUALITY ENGINEERING PORTFOLIO: 5 PRODUCTION SCENARIOS"
echo "=========================================================================="

PYTHON_BIN=".venv/bin/python"

if [ ! -f "$PYTHON_BIN" ]; then
    PYTHON_BIN="python3"
fi

echo -e "\n[0/5] Ensuring Clean & Corrupted Baseline Datasets..."
$PYTHON_BIN data/generate_olist_data.py
$PYTHON_BIN data/inject_corruptions.py

echo -e "\n[1/5] Running Project 1: Schema Drift Validation Gate..."
$PYTHON_BIN data-quality/01-schema-drift-gate/demo.py

echo -e "\n[2/5] Running Project 2: Business Rules Semantic Validator..."
$PYTHON_BIN data-quality/02-business-rules-validator/demo.py

echo -e "\n[3/5] Running Project 3: Volume & Freshness Anomaly Monitor..."
$PYTHON_BIN data-quality/03-volume-freshness-anomaly/demo.py

echo -e "\n[4/5] Running Project 4: Delta Lake Bronze -> Silver Quarantine Engine..."
$PYTHON_BIN data-quality/04-delta-lake-quarantine/demo.py

echo -e "\n[5/5] Running Project 5: Cold-Start Profiler, Data Docs & Alerting..."
$PYTHON_BIN data-quality/05-automated-table-profiling/demo.py

echo -e "\n=========================================================================="
echo "  ✔ ALL 5 DATA QUALITY DEMOS COMPLETED SUCCESSFULLY!"
echo "=========================================================================="

