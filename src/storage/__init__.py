"""
Storage - persistence layer for state and markets.
"""
import json
import logging
import os
import tempfile
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List, Optional
from dataclasses import dataclass, asdict

logger = logging.getLogger(__name__)


@contextmanager
def _file_lock(path: Path):
    """Cross-process advisory lock using a sidecar lock file."""
    lock_path = Path(f"{path}.lock")
    lock_path.parent.mkdir(exist_ok=True)
    with open(lock_path, "a", encoding="utf-8") as lock_file:
        fcntl_module = None
        try:
            import fcntl

            fcntl_module = fcntl
            fcntl_module.flock(lock_file.fileno(), fcntl_module.LOCK_EX)
        except ImportError:
            logger.debug("fcntl unavailable; file lock disabled for %s", path)
        try:
            yield
        finally:
            if fcntl_module is not None:
                try:
                    fcntl_module.flock(lock_file.fileno(), fcntl_module.LOCK_UN)
                except OSError as unlock_exc:
                    logger.warning("Failed to unlock %s: %s", path, unlock_exc)


def _atomic_write(path: Path, content: str, encoding: str = "utf-8") -> None:
    """Write file atomically using temp file + rename."""
    path = Path(path)
    dir_path = path.parent
    
    # Write to temp file in same directory
    fd, tmp_path = tempfile.mkstemp(dir=dir_path, suffix='.tmp')
    try:
        with os.fdopen(fd, 'w', encoding=encoding) as f:
            f.write(content)
        # Atomic rename
        os.replace(tmp_path, path)
    except (Exception,) as e:
        # Clean up temp file on error
        try:
            os.unlink(tmp_path)
        except (Exception,) as cleanup_exc:
            logger.warning("Failed to remove temp file %s: %s", tmp_path, cleanup_exc)
        raise


@dataclass
class State:
    """Bot state."""
    balance: float = 10000.0
    starting_balance: float = 10000.0
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    daily_pnl: float = 0.0
    peak_balance: float = 10000.0
    last_report_ts: float = 0.0
    drift_status: str = "stable"
    last_heartbeat: float = 0.0  # Timestamp of last scan cycle (heartbeat)


@dataclass
class Market:
    """Weather market data."""
    city: str = ""
    city_name: str = ""
    date: str = ""
    unit: str = "C"
    station: str = ""
    event_end_date: str = ""
    hours_at_discovery: float = 0.0
    status: str = "open"  # open, closed, resolved
    position: Optional[Dict] = None
    paper_position: Optional[Dict] = None
    actual_temp: Optional[float] = None
    resolved_outcome: Optional[str] = None  # win, loss
    pnl: Optional[float] = None
    resolved_at: Optional[str] = None  # ISO timestamp when market was resolved
    signal_state: Optional[Dict] = None
    paper_state: Optional[Dict] = None
    last_analysis: Optional[Dict] = None
    forecast_snapshots: List[Dict] = None
    market_snapshots: List[Dict] = None
    all_outcomes: List[Dict] = None
    created_at: str = ""
    
    def __post_init__(self):
        if self.forecast_snapshots is None:
            self.forecast_snapshots = []
        if self.market_snapshots is None:
            self.market_snapshots = []
        if self.all_outcomes is None:
            self.all_outcomes = []
        if self.signal_state is None:
            self.signal_state = {}
        if self.paper_state is None:
            self.paper_state = {}
        if self.last_analysis is None:
            self.last_analysis = {}


class Storage:
    """Storage layer."""
    
    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.markets_dir = self.data_dir / "markets"
        self.markets_dir.mkdir(exist_ok=True)
        self.state_file = self.data_dir / "state.json"
        self.calibration_file = self.data_dir / "calibration.json"
    
    # === State ===
    def load_state(self) -> State:
        """Load bot state."""
        if self.state_file.exists():
            with _file_lock(self.state_file):
                data = json.loads(self.state_file.read_text(encoding="utf-8"))
            # Filter data to match State fields for backward compatibility
            import dataclasses
            fields = {f.name for f in dataclasses.fields(State)}
            filtered = {k: v for k, v in data.items() if k in fields}
            return State(**filtered)
        return State()
    
    def save_state(self, state: State):
        """Save bot state atomically."""
        content = json.dumps(asdict(state), indent=2, ensure_ascii=False)
        with _file_lock(self.state_file):
            _atomic_write(self.state_file, content, encoding="utf-8")
    
    # === Markets ===
    def market_path(self, city: str, date: str) -> Path:
        """Get market file path."""
        return self.markets_dir / f"{city}_{date}.json"
    
    def load_market(self, city: str, date: str) -> Optional[Market]:
        """Load market data with field filtering."""
        path = self.market_path(city, date)
        if path.exists():
            try:
                with _file_lock(path):
                    data = json.loads(path.read_text(encoding="utf-8"))
                import dataclasses
                fields = {f.name for f in dataclasses.fields(Market)}
                filtered = {k: v for k, v in data.items() if k in fields}
                return Market(**filtered)
            except (Exception,) as e:
                logger.warning("Error loading market %s_%s: %s", city, date, e)
                return None
        return None
    
    def save_market(self, market: Market):
        """Save market data atomically."""
        path = self.market_path(market.city, market.date)
        data = asdict(market)
        content = json.dumps(data, indent=2, ensure_ascii=False)
        with _file_lock(path):
            _atomic_write(path, content, encoding="utf-8")
    
    def load_all_markets(self) -> List[Market]:
        """Load all markets with field filtering."""
        import dataclasses
        fields = {f.name for f in dataclasses.fields(Market)}
        markets = []
        for f in self.markets_dir.glob("*.json"):
            try:
                with _file_lock(f):
                    data = json.loads(f.read_text(encoding="utf-8"))
                filtered = {k: v for k, v in data.items() if k in fields}
                markets.append(Market(**filtered))
            except (Exception,) as e:
                logger.warning("Skipping unreadable market file %s: %s", f, e)
        return markets
    
    # === Calibration ===
    def load_calibration(self) -> Dict:
        """Load calibration data."""
        if self.calibration_file.exists():
            with _file_lock(self.calibration_file):
                return json.loads(self.calibration_file.read_text(encoding="utf-8"))
        return {}
    
    def save_calibration(self, cal: Dict):
        """Save calibration data."""
        with _file_lock(self.calibration_file):
            _atomic_write(self.calibration_file, json.dumps(cal, indent=2), encoding="utf-8")


# Global storage instance
_storage: Optional[Storage] = None


def get_storage(data_dir: str = "data") -> Storage:
    """Get global storage instance."""
    global _storage
    if _storage is None or str(_storage.data_dir) != data_dir:
        _storage = Storage(data_dir)
    return _storage
