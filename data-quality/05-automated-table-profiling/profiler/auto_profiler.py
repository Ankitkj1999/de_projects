"""
Project 5: Automated Table Profiler
Scans schema and statistical distributions across catalog tables to bootstrap starter GX suites.
"""

import uuid
from typing import Dict, Any, List
import pandas as pd
import numpy as np
import great_expectations as gx


class AutomatedTableProfiler:
    """
    Automated table profiler that analyzes schema, nullability,
    cardinality, and value distributions to generate starter expectation suites.
    """

    def profile_dataframe(self, df: pd.DataFrame, table_name: str) -> Dict[str, Any]:
        """
        Profiles a DataFrame and generates a candidate expectation suite dictionary.
        """
        row_count = len(df)
        columns_profile = {}

        candidate_expectations = []

        # 1. Table-level column set
        candidate_expectations.append({
            "type": "expect_table_columns_to_match_set",
            "kwargs": {"column_set": list(df.columns)},
            "tier": "structural",
            "confidence": 1.0,
        })

        # 2. Approximate Row Count Magnitude
        candidate_expectations.append({
            "type": "expect_table_row_count_to_be_between",
            "kwargs": {
                "min_value": max(1, int(row_count * 0.5)),
                "max_value": int(row_count * 2.0),
            },
            "tier": "volume",
            "confidence": 0.85,
        })

        for col in df.columns:
            series = df[col]
            null_count = series.isna().sum()
            null_pct = null_count / row_count if row_count > 0 else 0.0
            unique_count = series.nunique()
            is_unique = (unique_count == row_count) and (null_count == 0)

            col_info = {
                "dtype": str(series.dtype),
                "null_pct": round(null_pct, 4),
                "unique_count": unique_count,
                "is_candidate_pk": bool(is_unique),
            }

            # Check for candidate Primary Key
            if is_unique and "id" in col.lower():
                candidate_expectations.append({
                    "type": "expect_column_values_to_be_unique",
                    "kwargs": {"column": col},
                    "tier": "primary_key",
                    "confidence": 0.99,
                })
                candidate_expectations.append({
                    "type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col, "mostly": 1.0},
                    "tier": "primary_key",
                    "confidence": 1.0,
                })

            # Check for non-null required fields
            elif null_pct == 0.0:
                candidate_expectations.append({
                    "type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col, "mostly": 0.99},
                    "tier": "quality",
                    "confidence": 0.90,
                })
            elif null_pct < 0.05:
                candidate_expectations.append({
                    "type": "expect_column_values_to_not_be_null",
                    "kwargs": {"column": col, "mostly": 0.95},
                    "tier": "quality",
                    "confidence": 0.80,
                })

            # Low Cardinality Categoricals (candidate enums)
            if 1 < unique_count <= 12 and (pd.api.types.is_string_dtype(series) or series.dtype == "object"):
                distinct_vals = [str(v) for v in series.dropna().unique().tolist()]
                candidate_expectations.append({
                    "type": "expect_column_values_to_be_in_set",
                    "kwargs": {"column": col, "value_set": distinct_vals},
                    "tier": "categorical_enum",
                    "confidence": 0.95,
                })

            # Numeric columns range
            if pd.api.types.is_numeric_dtype(series):
                min_val = float(series.min())
                max_val = float(series.max())
                col_info["min"] = min_val
                col_info["max"] = max_val
                candidate_expectations.append({
                    "type": "expect_column_values_to_be_between",
                    "kwargs": {
                        "column": col,
                        "min_value": min_val,
                        "max_value": max_val,
                    },
                    "tier": "numeric_range",
                    "confidence": 0.70,  # Lower confidence: can be brittle
                })

            columns_profile[col] = col_info

        return {
            "table_name": table_name,
            "row_count": row_count,
            "columns": columns_profile,
            "raw_expectations": candidate_expectations,
        }
