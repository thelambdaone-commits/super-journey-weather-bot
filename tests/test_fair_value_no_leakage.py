"""
Test for Fair Value Engine V3 - Anti-Leakage Check (TIMESTAMPTZ Aware).
"""

import pytest
from datetime import datetime, timezone, timedelta
from src.alpha.fair_value import FairValueEngine
from src.data.moat_manager import MoatManager
import polars as pl
from pathlib import Path


@pytest.fixture
def temp_moat(tmp_path):
    db_path = tmp_path / "test_moat.db"
    return MoatManager(str(db_path))


def test_no_leakage(temp_moat):
    """
    Ensure that get_latest_valid_forecasts only returns forecasts
    whose run_cycle is strictly before 'now'.
    """
    engine = FairValueEngine(temp_moat)
    city = "PARIS"
    target_time = datetime(2026, 7, 10, 0, 0, 0, tzinfo=timezone.utc)

    # 1. Add a PAST forecast (Valid)
    past_run = datetime.now(timezone.utc) - timedelta(hours=6)
    df_past = pl.DataFrame(
        [
            {
                "ingested_at": datetime.now(timezone.utc),
                "city": city,
                "model": "ecmwf",
                "run_cycle": past_run,
                "valid_time": target_time,
                "horizon_hours": 24,
                "temp_c": 25.0,
                "humidity": 0.0,
                "pressure": 0.0,
            }
        ]
    )
    temp_moat.save_forecasts(df_past)

    # 2. Add a FUTURE forecast (Leakage candidate)
    future_run = datetime.now(timezone.utc) + timedelta(hours=12)
    df_future = pl.DataFrame(
        [
            {
                "ingested_at": datetime.now(timezone.utc),
                "city": city,
                "model": "gfs",
                "run_cycle": future_run,
                "valid_time": target_time,
                "horizon_hours": 24,
                "temp_c": 35.0,
                "humidity": 0.0,
                "pressure": 0.0,
            }
        ]
    )
    temp_moat.save_forecasts(df_future)

    # 3. Retrieve
    valid_forecasts = temp_moat.get_latest_valid_forecasts(city, target_time)

    # Assertions
    assert len(valid_forecasts) == 1
    assert valid_forecasts["model"][0] == "ecmwf"

    print("✅ Anti-leakage test passed.")


if __name__ == "__main__":
    tm = MoatManager("data/test_leakage.db")
    test_no_leakage(tm)
