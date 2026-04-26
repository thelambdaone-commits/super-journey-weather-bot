"""
Signal Quality Layer (SQL) - Production-grade signal filtering and ranking.
"""
from __future__ import annotations
import time
import json
import hashlib
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
from pathlib import Path

@dataclass
class Signal:
    market_id: str
    city: str
    direction: str  # "BUY" / "SELL"
    price: float
    edge: float
    confidence: float
    calibration: float
    market_stability: float
    timestamp: float

    @classmethod
    def from_dict(cls, city: str, signal_dict: dict) -> Signal:
        """Helper to convert a standard signal dictionary to a Signal dataclass."""
        ml = signal_dict.get("ml", {})
        confidence = float(ml.get("confidence", 0.0))
        
        # Calibration: based on historical MAE (Mean Absolute Error)
        mae = float(ml.get("mae", 1.5))
        calibration = max(0.0, 1.0 - (mae / 4.0)) # 0% if MAE >= 4 degrees
        
        # Market Stability: based on spread (0.10 spread is considered highly unstable)
        spread = float(signal_dict.get("spread", 0.05))
        market_stability = max(0.0, 1.0 - (spread / 0.10))
        
        return cls(
            market_id=signal_dict["market_id"],
            city=city,
            direction="BUY",
            price=float(signal_dict["entry_price"]),
            edge=float(signal_dict.get("ev", 0.0)),
            confidence=confidence,
            calibration=calibration,
            market_stability=market_stability,
            timestamp=time.time()
        )

class SignalQualityLayer:
    """
    Quality control layer for trading signals.
    Handles hard filters, anti-duplicate logic, and quality scoring.
    """

    def __init__(self, config, data_dir: str = "data"):
        self.config = config
        self.state_path = Path(data_dir) / "signals_state.json"
        self.state = self._load_state()

        # Thresholds from config (with defaults)
        self.MIN_CONFIDENCE = getattr(config, "signal_min_confidence", 0.75)
        self.MIN_EDGE = getattr(config, "signal_min_ev", 0.05)
        self.COOLDOWN_HOURS = getattr(config, "signal_city_cooldown_hours", 8)
        self.STALE_SECONDS = 7200  # 2 hours

    def _load_state(self) -> Dict[str, Any]:
        """Load persistent state for duplicates and cooldowns."""
        if self.state_path.exists():
            try:
                return json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {
            "last_signals": {},
            "hashes": []
        }

    def _save_state(self):
        """Persist state to disk."""
        self.state_path.write_text(
            json.dumps(self.state, indent=2, ensure_ascii=False),
            encoding="utf-8"
        )

    def _fingerprint(self, signal: Signal) -> str:
        """Create a unique hash for a signal to detect exact duplicates."""
        raw = f"{signal.market_id}:{signal.city}:{signal.direction}"
        return hashlib.sha256(raw.encode()).hexdigest()

    def validate_hard_rules(self, signal: Signal) -> Optional[str]:
        """Apply baseline filters (GATES)."""
        if signal.confidence < self.MIN_CONFIDENCE:
            return "low_confidence"

        if signal.edge < self.MIN_EDGE:
            return "low_edge"

        if signal.price <= 0:
            return "invalid_price"

        if (time.time() - signal.timestamp) > self.STALE_SECONDS:
            return "stale_market"

        return None

    def is_duplicate(self, signal: Signal) -> bool:
        """Check for duplicates or city-level cooldowns."""
        from src.trading.idempotence import get_idempotence_manager
        
        # 1. Exact market duplicate check (Window: 24h)
        key = f"{signal.market_id}:{signal.city}:{signal.direction}"
        if get_idempotence_manager().is_duplicate("signal", key, window_seconds=24 * 3600):
            return True

        # 2. City cooldown check
        city_key = f"city_cooldown:{signal.city}"
        if get_idempotence_manager().is_duplicate("city_cooldown", city_key, window_seconds=self.COOLDOWN_HOURS * 3600):
            return True

        return False

    def compute_quality(self, signal: Signal) -> float:
        """
        Compute a comprehensive quality score (0.0 to 1.0).
        Ranking intelligence for signals.
        """
        return round(
            0.4 * signal.confidence +
            0.3 * signal.edge +
            0.2 * signal.calibration +
            0.1 * signal.market_stability,
            4
        )

    def validate(self, signal: Signal) -> Dict[str, Any]:
        """Main validation pipeline."""
        # 1. HARD RULES
        reason = self.validate_hard_rules(signal)
        if reason:
            return {
                "accepted": False,
                "reason": reason,
                "quality": 0.0
            }

        # 2. DUPLICATES & COOLDOWNS
        if self.is_duplicate(signal):
            return {
                "accepted": False,
                "reason": "duplicate_or_cooldown",
                "quality": 0.0
            }

        # 3. QUALITY SCORE
        quality = self.compute_quality(signal)

        return {
            "accepted": True,
            "reason": "ok",
            "quality": quality
        }

    def commit(self, signal: Signal):
        """Update state after a signal is accepted and sent."""
        key = f"{signal.market_id}:{signal.city}:{signal.direction}"
        city_key = f"city_cooldown:{signal.city}"
        
        now = time.time()
        self.state["last_signals"][key] = now
        self.state["last_signals"][city_key] = now
        
        self.state["hashes"].append(self._fingerprint(signal))
        # Keep only last 1000 hashes
        self.state["hashes"] = self.state["hashes"][-1000:]
        
        self._save_state()

    def process(self, signal: Signal) -> Dict[str, Any]:
        """Pipeline entry point: validate and commit if accepted."""
        result = self.validate(signal)
        if result["accepted"]:
            self.commit(signal)
        return result
