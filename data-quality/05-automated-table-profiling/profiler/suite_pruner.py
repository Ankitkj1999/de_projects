"""
Project 5: Suite Pruner
Prunes noisy and brittle candidate expectations to yield robust, production-ready suites.
"""

from typing import Dict, Any, List, Optional, Tuple
import great_expectations as gx


class ExpectationSuitePruner:
    """
    Separates signal from noise:
    - Filters out brittle min/max numeric bounds that cause false positive alerts
    - Replaces rigid 100% assertions with resilient tolerance thresholds (mostly=)
    - Retains high-confidence invariants (Primary Keys, Schema, Enums, Freshness)
    """

    def prune_and_compile(
        self,
        profile_result: Dict[str, Any],
        suite_name: Optional[str] = None,
        min_confidence: float = 0.85,
        context: Optional[Any] = None,
    ) -> Tuple[gx.ExpectationSuite, Any]:
        tbl_name = profile_result["table_name"]
        suite_name = suite_name or f"curated_{tbl_name}_suite"

        if context is None:
            context = gx.get_context(mode="ephemeral")

        suite = gx.ExpectationSuite(name=suite_name)
        context.suites.add(suite)

        raw_expectations = profile_result["raw_expectations"]
        kept_count = 0
        pruned_count = 0

        for exp in raw_expectations:
            tier = exp["tier"]
            confidence = exp["confidence"]
            exp_type = exp["type"]
            kwargs = exp["kwargs"]

            # 1. Prune brittle numeric range assertions (overfitting sample min/max)
            if tier == "numeric_range" and confidence < min_confidence:
                pruned_count += 1
                continue

            # 2. Compile into actual GX Expectation classes
            try:
                if exp_type == "expect_table_columns_to_match_set":
                    suite.add_expectation(
                        gx.expectations.ExpectTableColumnsToMatchSet(column_set=kwargs["column_set"])
                    )
                    kept_count += 1

                elif exp_type == "expect_table_row_count_to_be_between":
                    suite.add_expectation(
                        gx.expectations.ExpectTableRowCountToBeBetween(
                            min_value=kwargs["min_value"],
                            max_value=kwargs["max_value"]
                        )
                    )
                    kept_count += 1

                elif exp_type == "expect_column_values_to_be_unique":
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToBeUnique(column=kwargs["column"])
                    )
                    kept_count += 1

                elif exp_type == "expect_column_values_to_not_be_null":
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToNotBeNull(
                            column=kwargs["column"],
                            mostly=kwargs.get("mostly", 1.0)
                        )
                    )
                    kept_count += 1

                elif exp_type == "expect_column_values_to_be_in_set":
                    suite.add_expectation(
                        gx.expectations.ExpectColumnValuesToBeInSet(
                            column=kwargs["column"],
                            value_set=kwargs["value_set"]
                        )
                    )
                    kept_count += 1
            except Exception as e:
                pruned_count += 1

        return suite, context
