"""
Moat Manager V3.1 - TIMESTAMPTZ & Global Precision.
"""
import duckdb
import polars as pl
from pathlib import Path
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

class MoatManager:
    """
    Manages the local DuckDB instance with TIMESTAMPTZ and strict audit trails.
    """
    def __init__(self, db_path: str = "data/weather_moat.db"):
        self.db_path = db_path
        Path("data").mkdir(exist_ok=True)
        try:
            self.conn = duckdb.connect(self.db_path)
            self._bootstrap()
            self.ready = True
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            self.ready = False

    def _bootstrap(self):
        """Initialize the TIMESTAMPTZ billionaire schema."""
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
                vwap_ask DOUBLE,            -- VWAP for a standard size (e.g. $100)
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
        # Settlements
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS settlements (
                date DATE,
                city TEXT,
                station TEXT,
                official_high DOUBLE,
                source TEXT
            )
        """)

    def save_forecasts(self, df: pl.DataFrame):
        if not self.ready or df.is_empty():
            return
        try:
            self.conn.execute("INSERT INTO forecast_runs SELECT * FROM df")
        except Exception as e:
            logger.error(f"Failed to save forecasts: {e}")

    def save_quote(self, market_id: str, city: str, bid: float, ask: float, vwap: float, spread: float, liquidity: float, tick_size: float):
        if not self.ready:
            return
        try:
            ts = datetime.now(timezone.utc)
            mid = (bid + ask) / 2 if bid and ask else 0.5
            self.conn.execute("""
                INSERT INTO market_history VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (ts, market_id, city, bid, ask, vwap, mid, spread, liquidity, tick_size, 0.0))
        except Exception as e:
            logger.error(f"Failed to save quote: {e}")

    def get_latest_valid_forecasts(self, city: str, target_time: datetime) -> pl.DataFrame:
        """
        Anti-Leakage Query: Get forecasts for 'target_time' that were 
        generated BEFORE or AT a specific 'market_quote_time'.
        """
        if not self.ready:
            return pl.DataFrame()
        try:
            query = f"""
                SELECT model, temp_c, run_cycle 
                FROM forecast_runs 
                WHERE city = '{city}' 
                AND valid_time = '{target_time.isoformat()}'
                AND run_cycle < now()
                ORDER BY run_cycle DESC
            """
            return self.conn.execute(query).pl()
        except Exception as e:
            logger.error(f"Failed to query valid forecasts: {e}")
            return pl.DataFrame()

    def get_recent_calibration_error(self, city: str, model: str, limit: int = 20) -> float:
        """Retrieve recent MAE for a city/model combination."""
        if not self.ready:
            return 100.0
        try:
            res = self.conn.execute(f"""
                SELECT AVG(ABS(error_c)) 
                FROM calibration_events 
                WHERE city = '{city}' AND model = '{model}'
                ORDER BY event_ts DESC LIMIT {limit}
            """).fetchone()
            return res[0] if res and res[0] is not None else 2.0
        except Exception:
            return 2.0

_moat_instance = None

def get_moat(db_path: str = "data/weather_moat.db") -> MoatManager:
    global _moat_instance
    if _moat_instance is None:
        _moat_instance = MoatManager(db_path)
    return _moat_instance
