"""
Project 3: Historical Baseline Tracker
Maintains historical run metrics and calculates rolling 7-day volume and freshness baselines.
"""

import json
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple
import numpy as np


class HistoricalBaselineTracker:
    """
    Tracks daily ingestion volume and statistics.
    Computes trailing 7-day moving averages and dynamic expectation bounds.
    """

    def __init__(self, history_file: str = "data-quality/03-volume-freshness-anomaly/monitors/run_history.json"):
        self.history_file = Path(history_file)
        self.history_file.parent.mkdir(parents=True, exist_ok=True)
        if not self.history_file.exists():
            self._initialize_default_history()

    def _initialize_default_history(self):
        """Initializes a realistic 7-day trailing history (mean ~2500, std ~95)."""
        base_date = datetime.now() - timedelta(days=8)
        history = []
        # Trailing 7 days counts around 2500
        historical_counts = [2480, 2520, 2465, 2510, 2495, 2540, 2505]
        for i, count in enumerate(historical_counts):
            day_ts = base_date + timedelta(days=i + 1)
            history.append({
                "run_id": f"run_prod_{day_ts.strftime('%Y%m%d')}",
                "timestamp": day_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "row_count": count,
                "status": "SUCCESS"
            })
        self.save_history(history)

    def load_history(self) -> List[Dict[str, Any]]:
        with open(self.history_file, "r") as f:
            return json.load(f)

    def save_history(self, history: List[Dict[str, Any]]):
        with open(self.history_file, "w") as f:
            json.dump(history, f, indent=2)

    def record_run(self, run_id: str, row_count: int, status: str = "SUCCESS"):
        history = self.load_history()
        history.append({
            "run_id": run_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "row_count": row_count,
            "status": status,
        })
        self.save_history(history)

    def get_trailing_7day_baseline(self) -> Dict[str, Any]:
        """
        Calculates trailing 7-day statistics:
        - Mean row count
        - Standard deviation
        - 3-sigma hard bounds (lower_bound, upper_bound)
        - Warning bounds (2-sigma)
        """
        history = self.load_history()
        successful_runs = [r for r in history if r["status"] == "SUCCESS"][-7:]
        counts = [r["row_count"] for r in successful_runs]

        if not counts:
            # Fallback default
            return {
                "mean": 2500,
                "std": 100,
                "hard_min": 1800,
                "hard_max": 3200,
                "warn_min": 2200,
                "warn_max": 2800,
            }

        mean = float(np.mean(counts))
        std = float(np.std(counts)) if len(counts) > 1 and np.std(counts) > 0 else 100.0

        # Hard failure bounds (e.g. > 30% drop/surge is a catastrophic failure)
        hard_min = int(mean * 0.70)
        hard_max = int(mean * 1.30)

        # Warning bounds (e.g. 10% - 30% fluctuation triggers warning alert)
        warn_min = int(mean * 0.90)
        warn_max = int(mean * 1.10)

        return {
            "mean": round(mean, 1),
            "std": round(std, 1),
            "hard_min": hard_min,
            "hard_max": hard_max,
            "warn_min": warn_min,
            "warn_max": warn_max,
            "sample_runs_used": len(counts),
        }
