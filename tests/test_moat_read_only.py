from datetime import datetime, timezone

import polars as pl
import pytest

from src.data.moat_manager import MoatManager, MoatWriteError


def test_moat_read_only_allows_queries_and_rejects_writes(tmp_path):
    db_path = tmp_path / "moat.db"
    writer = MoatManager(str(db_path))
    writer.save_forecasts(pl.DataFrame([{
        "ingested_at": datetime.now(timezone.utc),
        "city": "PARIS",
        "model": "ecmwf",
        "run_cycle": datetime(2026, 4, 27, tzinfo=timezone.utc),
        "valid_time": datetime(2026, 4, 28, tzinfo=timezone.utc),
        "horizon_hours": 24,
        "temp_c": 22.0,
        "humidity": 0.0,
        "pressure": 0.0,
    }]))
    writer.close()

    reader = MoatManager(str(db_path), read_only=True)
    forecasts = reader.get_latest_valid_forecasts("PARIS", datetime(2026, 4, 28, tzinfo=timezone.utc))

    assert len(forecasts) == 1
    assert forecasts["model"][0] == "ecmwf"

    with pytest.raises(MoatWriteError):
        reader.save_quote("1", "PARIS", 0.4, 0.5, 0.5, 0.1, 1000.0, 0.01)
    reader.close()


def test_moat_writer_does_not_hold_persistent_duckdb_lock(tmp_path):
    db_path = tmp_path / "moat.db"
    writer = MoatManager(str(db_path))

    reader = MoatManager(str(db_path), read_only=True)

    assert writer.ready
    assert reader.ready
    writer.close()
    reader.close()
