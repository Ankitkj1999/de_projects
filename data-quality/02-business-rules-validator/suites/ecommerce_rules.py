"""
Project 2: Great Expectations Business Rules Suites
Defines the business logic invariants across Orders, Items, and Catalog referential checks.
"""

from datetime import datetime
from typing import List, Set
import great_expectations as gx


def build_orders_business_suite(suite_name: str = "orders_business_suite") -> gx.ExpectationSuite:
    """
    Suite enforcing semantic business logic on the Orders domain:
    1. Uniqueness of order IDs
    2. No future-dated orders
    3. Delivered date >= purchase timestamp (temporal causality)
    """
    suite = gx.ExpectationSuite(name=suite_name)
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Business Rule 1: Unique Order IDs (no duplicates)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(column="order_id")
    )

    # Business Rule 2: No future-dated orders
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="order_purchase_timestamp",
            min_value=datetime(2016, 1, 1),
            max_value=datetime.now(),
        )
    )

    # Business Rule 3: Delivered date >= Order purchase timestamp
    # Note: Only evaluates for delivered orders where customer delivery date is populated
    suite.add_expectation(
        gx.expectations.ExpectColumnPairValuesAToBeGreaterThanB(
            column_A="order_delivered_customer_date",
            column_B="order_purchase_timestamp",
            or_equal=True,
            ignore_row_if="either_value_is_missing",
        )
    )

    return suite


def build_order_items_business_suite(
    catalog_skus: Set[str],
    suite_name: str = "order_items_business_suite"
) -> gx.ExpectationSuite:
    """
    Suite enforcing semantic business logic on Line Items:
    1. Positive unit price (> 0.0)
    2. Non-negative freight (>= 0.0)
    3. Referential integrity: product_id must exist in active Catalog SKUs
    """
    suite = gx.ExpectationSuite(name=suite_name)

    # Business Rule 4: No zero or negative price
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="price",
            min_value=0.01,
            max_value=100000.0,
        )
    )

    # Business Rule 5: Non-negative freight
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="freight_value",
            min_value=0.0,
            max_value=5000.0,
        )
    )

    # Business Rule 6: Foreign key referential integrity - SKU exists in catalog
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="product_id",
            value_set=list(catalog_skus),
        )
    )

    return suite
