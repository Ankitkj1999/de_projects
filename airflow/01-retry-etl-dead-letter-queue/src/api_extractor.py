"""
src/api_extractor.py — Vendor Orders API Extractor
===================================================
Real-World Problem:
    E-commerce platforms (like Olist) integrate with dozens of third-party vendor
    APIs that expose order data through paginated REST endpoints. These APIs are
    notoriously unreliable — rate limits, timeouts at peak hours, and flaky network
    conditions make simple "call and hope" strategies fail in production.

    When the nightly ETL job silently swallowed an API timeout at 3 AM, the morning
    dashboard was blank. No alert fired. The fix: explicit retry logic with
    exponential backoff + a circuit-breaker pattern at the extraction layer.

This module simulates a paginated vendor API (3 pages, ~50 records each) using
Olist-style order data. It supports a `simulate_failure` flag to test retry logic
without spinning up a real HTTP server.
"""

import random
import uuid
import json
from datetime import datetime, timedelta

# Simulating requests.exceptions.Timeout for realistic retry testing
try:
    import requests.exceptions as req_exc
except ImportError:
    # Fallback if requests not available (shouldn't happen in this project)
    class _FakeReqExc:
        class Timeout(Exception):
            pass
    req_exc = _FakeReqExc()

import pandas as pd


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
ORDER_STATUSES = [
    "delivered", "shipped", "processing", "invoiced",
    "unavailable", "canceled", "approved"
]

CATEGORIES = [
    "electronics", "furniture", "clothing", "sports",
    "beauty", "books", "toys", "food"
]

RECORDS_PER_PAGE = 50
TOTAL_PAGES = 3

# Seed for reproducibility in demo mode
_DEMO_SEED = 42


class VendorAPIExtractor:
    """
    Simulates a paginated vendor REST API for order data extraction.

    In a real pipeline this class would wrap `requests.Session` calls with
    timeout/retry configuration. Here it generates synthetic Olist-style records
    to allow full pipeline testing without external dependencies.
    """

    def __init__(self, vendor_id: str = "olist-vendor-01", page_size: int = RECORDS_PER_PAGE):
        self.vendor_id = vendor_id
        self.page_size = page_size
        self._call_count = 0  # tracks invocation count for simulated failure

    def _generate_page(self, page_num: int, batch_date: str, rng: random.Random) -> list[dict]:
        """Generate a single page of synthetic order records."""
        base_dt = datetime.strptime(batch_date, "%Y-%m-%d")
        records = []

        for i in range(self.page_size):
            order_id = f"ORD-{batch_date.replace('-', '')}-P{page_num}-{i:03d}"
            price = round(rng.uniform(5.0, 2500.0), 2)
            freight = round(rng.uniform(2.0, 80.0), 2)
            purchase_offset = timedelta(hours=rng.randint(0, 23), minutes=rng.randint(0, 59))
            purchase_dt = base_dt + purchase_offset

            records.append({
                "order_id": order_id,
                "customer_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "order_status": rng.choice(ORDER_STATUSES),
                "price": price,
                "freight_value": freight,
                "product_category": rng.choice(CATEGORIES),
                "order_purchase_timestamp": purchase_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "vendor_id": self.vendor_id,
                "page_num": page_num,
            })
        return records

    def _fetch_page(self, page_num: int, batch_date: str, rng: random.Random) -> list[dict]:
        """
        Simulates an HTTP GET to /vendor/orders?page={page_num}&date={batch_date}.

        In production this would be:
            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()["records"]
        """
        self._call_count += 1
        return self._generate_page(page_num, batch_date, rng)

    def extract_vendor_orders(
        self,
        batch_date: str,
        simulate_failure: bool = False,
        seed: int | None = None,
    ) -> dict:
        """
        Extract all vendor orders for a given batch date across all pages.

        Parameters
        ----------
        batch_date : str
            Date string in YYYY-MM-DD format.
        simulate_failure : bool
            If True, raises requests.exceptions.Timeout on the FIRST call to
            simulate a network timeout. Subsequent calls succeed (retry scenario).
        seed : int | None
            Random seed for reproducibility. Uses _DEMO_SEED if None.

        Returns
        -------
        dict with keys:
            records           : list of order dicts
            page_count        : number of pages fetched
            total_records     : total record count
            extraction_timestamp : ISO timestamp of extraction
            vendor_id         : vendor identifier
        """
        rng = random.Random(seed if seed is not None else _DEMO_SEED)
        self._call_count = 0

        all_records: list[dict] = []

        for page_num in range(1, TOTAL_PAGES + 1):
            # Simulate timeout on the very first page fetch
            if simulate_failure and self._call_count == 0:
                raise req_exc.Timeout(
                    f"[Simulated] GET /vendor/orders?page={page_num}&date={batch_date} "
                    f"timed out after 30s — vendor API unresponsive"
                )
            page_records = self._fetch_page(page_num, batch_date, rng)
            all_records.extend(page_records)

        return {
            "records": all_records,
            "page_count": TOTAL_PAGES,
            "total_records": len(all_records),
            "extraction_timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "vendor_id": self.vendor_id,
            "batch_date": batch_date,
        }


# ---------------------------------------------------------------------------
# Convenience wrapper used by the Airflow PythonOperator
# ---------------------------------------------------------------------------
def extract_vendor_orders(batch_date: str, simulate_failure: bool = False) -> dict:
    """Module-level wrapper for use in Airflow PythonOperator callable."""
    extractor = VendorAPIExtractor()
    return extractor.extract_vendor_orders(batch_date, simulate_failure=simulate_failure)
