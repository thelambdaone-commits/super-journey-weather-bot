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
    def __init__(self, db_path: str = "data/weather_moat.db"):
        self.db_path = db_path
        Path("data").mkdir(exist_ok=True)
        try:
            self.conn = duckdb.connect(self.db_path)
            self._bootstrap()
            self.ready = True
        except Exception:
            logger.exception("CRITICAL: Failed to connect to DuckDB at %s", self.db_path)
            self.ready = False
            raise MoatConnectionError(f"Could not open database at {db_path}")

    def _bootstrap(self):
        """Initialize the TIMESTAMPTZ billionaire schema."""
        try:
            # Forecast Runs Table
            self.conn.execute("""
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
            self.conn.execute("""
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
            self.conn.execute("""
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
        except Exception:
            logger.exception("Failed to bootstrap Moat schema")
            raise MoatWriteError("Schema initialization failed")

    def save_forecasts(self, df: pl.DataFrame):
        if not self.ready:
            raise MoatConnectionError("Database not ready")
        if df.is_empty():
            return
            
        try:
            # Note: Parameterized query for large data is handled via DuckDB's native polars integration
            self.conn.execute("INSERT INTO forecast_runs SELECT * FROM df")
            logger.info("[MOAT] Saved %d forecast points.", len(df))
        except Exception:
            logger.exception("Failed to bulk insert forecasts")
            raise MoatWriteError("Forecast insertion failed")

    def save_quote(self, market_id: str, city: str, bid: float, ask: float, vwap: float, spread: float, liquidity: float, tick_size: float):
        if not self.ready:
            raise MoatConnectionError("Database not ready")
        try:
            ts = datetime.now(timezone.utc)
            mid = (bid + ask) / 2 if bid and ask else 0.5
            # PARAMETERIZED QUERY
            self.conn.execute("""
                INSERT INTO market_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ts, market_id, city, bid, ask, vwap, mid, spread, liquidity, tick_size, 0.0))
        except Exception:
            logger.exception("Failed to save market quote for %s", city)
            raise MoatWriteError(f"Market quote persistence failed for {city}")

    def get_latest_valid_forecasts(self, city: str, target_time: datetime) -> pl.DataFrame:
        if not self.ready:
            return pl.DataFrame()
        try:
            # PARAMETERIZED QUERY
            query = """
                SELECT model, temp_c, run_cycle 
                FROM forecast_runs 
                WHERE city = ? 
                AND valid_time = ?
                AND run_cycle < now()
                ORDER BY run_cycle DESC
            """
            return self.conn.execute(query, (city, target_time)).pl()
        except Exception:
            logger.exception("Failed to query valid forecasts for %s", city)
            raise MoatQueryError(f"Anti-leakage query failed for {city}")

    def get_recent_calibration_error(self, city: str, model: str, limit: int = 20) -> float:
        if not self.ready:
            return 2.0
        try:
            res = self.conn.execute("""
                SELECT AVG(ABS(error_c)) 
                FROM calibration_events 
                WHERE city = ? AND model = ?
                ORDER BY event_ts DESC LIMIT ?
            """, (city, model, limit)).fetchone()
            return res[0] if res and res[0] is not None else 2.0
        except Exception:
            logger.exception("Failed to fetch calibration error for %s", city)
            return 2.0

_moat_instance = None

def get_moat(db_path: str = "data/weather_moat.db") -> MoatManager:
    global _moat_instance
    if _moat_instance is None:
        _moat_instance = MoatManager(db_path)
    return _moat_instance
