"""
src/sensor.py — Event-Driven Success Marker Sensor
===================================================
Real-World Problem:
    Downstream ETL jobs often trigger on cron schedules while upstream systems
    are still writing multi-gigabyte files. Reading files mid-write causes truncated
    reads, corrupt rows, and crashes.

    This module implements a sensor that polls for the atomic `_SUCCESS_{logical_date}`
    marker file before permitting downstream processing. It supports soft-fail and
    timeout limits to enable Circuit Breaker handling.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Dict, Optional


class SuccessMarkerSensor:
    """
    Polls landing storage for an atomic upstream _SUCCESS marker file.
    """

    def __init__(self, landing_dir: str = "data/processed/airflow/landing/"):
        self.landing_dir = Path(landing_dir)
        self.landing_dir.mkdir(parents=True, exist_ok=True)

    def poke(self, logical_date: str) -> Optional[Dict[str, Any]]:
        """
        Single poke check: returns marker metadata dict if marker exists, else None.
        """
        marker_file = self.landing_dir / f"_SUCCESS_{logical_date}"
        if not marker_file.exists():
            return None

        try:
            with open(marker_file, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {"status": "MARKER_FOUND_BUT_UNREADABLE"}

    def poll(
        self,
        logical_date: str,
        timeout_seconds: int = 300,
        poke_interval: int = 10,
        simulate_found_after: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Polls until the marker file is discovered or timeout_seconds is exceeded.
        """
        waited = 0
        while waited <= timeout_seconds:
            # If simulation flag provided
            if simulate_found_after is not None:
                if waited >= simulate_found_after:
                    meta = self.poke(logical_date)
                    return {
                        "found": True,
                        "marker_path": str(self.landing_dir / f"_SUCCESS_{logical_date}"),
                        "waited_seconds": waited,
                        "timed_out": False,
                        "metadata": meta or {},
                    }
            else:
                meta = self.poke(logical_date)
                if meta:
                    return {
                        "found": True,
                        "marker_path": str(self.landing_dir / f"_SUCCESS_{logical_date}"),
                        "waited_seconds": waited,
                        "timed_out": False,
                        "metadata": meta,
                    }

            waited += poke_interval

        return {
            "found": False,
            "marker_path": str(self.landing_dir / f"_SUCCESS_{logical_date}"),
            "waited_seconds": timeout_seconds,
            "timed_out": True,
            "metadata": {},
        }
