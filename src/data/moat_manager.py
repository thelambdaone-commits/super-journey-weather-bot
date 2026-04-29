"""
Moat Manager V3.2 - Robust Data Layer.
Includes typed errors, parameterized queries, and strict audit trails.
"""

import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)


def _city_lookup_values(city: str) -> list[str]:
    """Return compatible city keys for historical name/slug storage."""
    raw = str(city)
    normalized = raw.lower()
    values = {raw, normalized, normalized.replace(" ", "-")}

    try:
        from ..weather.locations import LOCATIONS

        for slug, loc in LOCATIONS.items():
            if normalized in {slug.lower(), loc.name.lower()}:
                values.add(slug)
                values.add(loc.name)
                values.add(loc.name.lower())
    except (Exception,):
        logger.debug("Location lookup unavailable for city normalization", exc_info=True)

    return sorted(values)


class MoatError(Exception):
    """Base exception for Moat operations."""

    pass


class MoatConnectionError(MoatError):
    """Failed to connect to the DuckDB instance."""

    pass


class MoatWriteError(MoatError):
    """Failed to write data to the Moat."""

    pass


class MoatQueryError(MoatError):
    """Failed to query data from the Moat."""

    pass


class MoatManager:
    """
    Manages the local DuckDB instance with robust error handling and TIMESTAMPTZ.
    """

    def __init__(self, db_path: str = "data/weather_moat.db", read_only: bool = False):
        self.db_path = db_path
        self.read_only = read_only
        Path("data").mkdir(exist_ok=True)
        try:
            conn = duckdb.connect(self.db_path, read_only=read_only)
            if not read_only:
                self._bootstrap(conn)
            conn.close()
            self.ready = True
        except (Exception,) as e:
            mode = "read_only" if read_only else "read_write"
            logger.exception("CRITICAL: Failed to connect to DuckDB at %s (%s)", self.db_path, mode)
            self.ready = False
            raise MoatConnectionError(f"Could not open database at {db_path} ({mode})")

    def _connect(self):
        return duckdb.connect(self.db_path, read_only=self.read_only)

    def _bootstrap(self, conn):
        """Initialize the TIMESTAMPTZ billionaire schema."""
        try:
            # Forecast Runs Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS forecast_runs (
                    ingested_at TIMESTAMPTZ,      
                    city TEXT,
                    model TEXT,
                    run_cycle TIMESTAMPTZ,       
                    valid_time TIMESTAMPTZ,      
                    horizon_hours INTEGER,
                    temp_c DOUBLE,
                    humidity DOUBLE,
                    pressure DOUBLE
                )
            """)
            # Market Quotes Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS market_history (
                    quote_ts TIMESTAMPTZ,        
                    market_id TEXT,
                    city TEXT,
                    best_bid DOUBLE,
                    best_ask DOUBLE,
                    vwap_ask DOUBLE,
                    midpoint DOUBLE,
                    spread DOUBLE,
                    liquidity_usd DOUBLE,
                    tick_size DOUBLE,
                    volume_usd DOUBLE
                )
            """)
            # Calibration Events
            conn.execute("""
                CREATE TABLE IF NOT EXISTS calibration_events (
                    event_ts TIMESTAMPTZ,
                    city TEXT,
                    model TEXT,
                    error_c DOUBLE,
                    prob_predicted DOUBLE,
                    outcome_realized BOOLEAN,
                    regime TEXT
                )
            """)
        except (Exception,) as e:
            logger.exception("Failed to bootstrap Moat schema")
            raise MoatWriteError("Schema initialization failed")

    def save_forecasts(self, df: pl.DataFrame):
        if not self.ready:
            raise MoatConnectionError("Database not ready")
        if self.read_only:
            raise MoatWriteError("Cannot save forecasts from a read-only MoatManager")
        if df.is_empty():
            return

        try:
            # Note: Parameterized query for large data is handled via DuckDB's native polars integration
            conn = self._connect()
            try:
                conn.execute("INSERT INTO forecast_runs SELECT * FROM df")
            finally:
                conn.close()
            logger.info("[MOAT] Saved %d forecast points.", len(df))
        except (Exception,) as e:
            logger.exception("Failed to bulk insert forecasts")
            raise MoatWriteError("Forecast insertion failed")

    def save_quote(
        self,
        market_id: str,
        city: str,
        bid: float,
        ask: float,
        vwap: float,
        spread: float,
        liquidity: float,
        tick_size: float,
    ):
        if not self.ready:
            raise MoatConnectionError("Database not ready")
        if self.read_only:
            raise MoatWriteError("Cannot save quotes from a read-only MoatManager")
        try:
            ts = datetime.now(timezone.utc)
            mid = (bid + ask) / 2 if bid and ask else 0.5
            # PARAMETERIZED QUERY
            conn = self._connect()
            try:
                conn.execute(
                    """
                    INSERT INTO market_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                    (ts, market_id, city, bid, ask, vwap, mid, spread, liquidity, tick_size, 0.0),
                )
            finally:
                conn.close()
        except (Exception,) as e:
            logger.exception("Failed to save market quote for %s", city)
            raise MoatWriteError(f"Market quote persistence failed for {city}")

    def get_latest_valid_forecasts(self, city: str, target_time: datetime) -> pl.DataFrame:
        if not self.ready:
            return pl.DataFrame()
        try:
            # PARAMETERIZED QUERY - tolerate historical mixed-case city values.
            city_values = _city_lookup_values(city)
            city_placeholders = ", ".join(["?"] * len(city_values))
            query = """
                SELECT model, temp_c, run_cycle 
                FROM forecast_runs 
                WHERE LOWER(city) IN ({city_placeholders}) 
                AND valid_time = ?
                AND run_cycle < now()
                ORDER BY run_cycle DESC
            """.format(city_placeholders=city_placeholders)
            conn = self._connect()
            try:
                return conn.execute(query, (*[value.lower() for value in city_values], target_time)).pl()
            finally:
                conn.close()
        except (Exception,) as e:
            logger.exception("Failed to query valid forecasts for %s", city)
            raise MoatQueryError(f"Anti-leakage query failed for {city}")

    def get_recent_calibration_error(self, city: str, model: str, limit: int = 20) -> float:
        if not self.ready:
            return 2.0
        try:
            conn = self._connect()
            try:
                res = conn.execute(
                    """
                    SELECT AVG(ABS(error_c))
                    FROM (
                        SELECT error_c
                        FROM calibration_events
                        WHERE LOWER(city) = LOWER(?) AND model = ?
                        ORDER BY event_ts DESC
                        LIMIT ?
                    )
                """,
                    (city, model, limit),
                ).fetchone()
            finally:
                conn.close()
            return res[0] if res and res[0] is not None else 2.0
        except (Exception,) as e:
            logger.exception("Failed to fetch calibration error for %s", city)
            return 2.0

    def close(self) -> None:
        """Close the DuckDB connection held by this manager."""
        self.ready = False


_moat_instances = {}


def get_moat(db_path: str = "data/weather_moat.db", read_only: bool = False) -> MoatManager:
    """Return a process-local MoatManager for the requested access mode."""
    key = (str(db_path), bool(read_only))
    if key not in _moat_instances:
        _moat_instances[key] = MoatManager(db_path, read_only=read_only)
    return _moat_instances[key]
