"""
Signal Quality Layer - Ranking intelligence and Top 1% Audit.
"""
from __future__ import annotations
import time
import json
import hashlib
import logging
import numpy as np
from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional
from pathlib import Path
from ..utils.feature_flags import is_enabled
from ..ml.bayesian_model import get_bayesian_model
from ..ml.anomaly_detector import get_anomaly_detector
from .sentiment import get_sentiment_analyzer

logger = logging.getLogger(__name__)

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
    question: str = ""
    features: Optional[dict] = None

    @classmethod
    def from_dict(cls, city: str, signal_dict: dict) -> Signal:
        """Helper to convert a standard signal dictionary to a Signal dataclass."""
        ml = signal_dict.get("ml", {})
        confidence = float(ml.get("confidence", 0.0))
        
        # Calibration: based on historical MAE (Mean Absolute Error)
        mae = float(ml.get("mae", 1.5))
        calibration = max(0.0, 1.0 - (mae / 4.0)) # 0% if MAE >= 4 degrees
        
        # Market Stability: based on spread
        spread = float(signal_dict.get("spread", 0.05))
        market_stability = max(0.0, 1.0 - (spread / 0.10))
        
        return cls(
            market_id=signal_dict["market_id"],
            city=city,
            direction="BUY",
            price=float(signal_dict.get("entry_price", 0.5)),
            edge=float(signal_dict.get("ev", 0.0)),
            confidence=confidence,
            calibration=calibration,
            market_stability=market_stability,
            timestamp=time.time(),
            question=signal_dict.get("question", ""),
            features=signal_dict.get("features")
        )

class SignalQualityLayer:
    """
    Evaluates signal quality using Top 1% AI briques (Bayesian, Autoencoder, Sentiment).
    """

    def __init__(self, config, data_dir: str = "data"):
        self.config = config
        self.bayesian_model = get_bayesian_model(data_dir)
        self.anomaly_detector = get_anomaly_detector(data_dir)
        self.sentiment_analyzer = get_sentiment_analyzer(config)

        # Thresholds
        self.MIN_CONFIDENCE = getattr(config, "signal_min_confidence", 0.70)
        self.MIN_EDGE = getattr(config, "signal_min_edge", 0.05)
        self.STALE_SECONDS = 300
        self.COOLDOWN_HOURS = 12

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

        # 1. Anomaly Autoencoder (#5)
        if is_enabled("ANOMALY_DETECTION_V2") and self.anomaly_detector.fitted and signal.features:
            try:
                from ..ml.dataset import row_to_features
                X = row_to_features(signal.features)
                is_ano, err = self.anomaly_detector.is_anomalous(X)
                if is_ano:
                    logger.warning(f"[ANOMALY-AE] Reject: error {err:.4f}")
                    return "reconstruction_anomaly"
            except Exception as e:
                logger.error(f"Anomaly check failed: {e}")

        return None

    def compute_quality(self, signal: Signal) -> float:
        """
        Compute a comprehensive quality score (0.0 to 1.0).
        Includes Bayesian Uncertainty (#1) and Sentiment Boost (#6).
        """
        # 2. Bayesian Epistemic Uncertainty (#1)
        bayesian_penalty = 0.0
        if is_enabled("BAYESIAN_UNCERTAINTY") and self.bayesian_model.fitted and signal.features:
            try:
                from ..ml.dataset import row_to_features
                X = row_to_features(signal.features).reshape(1, -1)
                _, std = self.bayesian_model.predict(X)
                # Penalize up to 30% if uncertainty is high
                bayesian_penalty = min(0.3, float(std[0]) * 0.5)
            except Exception as e:
                logger.error(f"Bayesian scoring failed: {e}")

        # 3. Sentiment Boost (#6)
        sentiment_boost = 0.0
        if is_enabled("SENTIMENT_WEIGHTED_SIGNALS"):
            try:
                sentiment_boost = self.sentiment_analyzer.analyze_signal(signal.city, signal.question)
            except Exception:
                pass

        # Final Weighted Score
        score = (
            0.4 * signal.confidence +
            0.3 * signal.edge +
            0.2 * signal.calibration +
            0.1 * signal.market_stability +
            0.2 * sentiment_boost -
            bayesian_penalty
        )
        return round(max(0.0, min(1.0, score)), 4)

    def is_duplicate(self, signal: Signal) -> bool:
        """Check for duplicates or city-level cooldowns."""
        from src.trading.idempotence import get_idempotence_manager
        
        key = f"{signal.market_id}:{signal.city}"
        if get_idempotence_manager().is_duplicate("signal", key, window_seconds=24 * 3600):
            return True

        city_key = f"city_cooldown:{signal.city}"
        if get_idempotence_manager().is_duplicate("city_cooldown", city_key, window_seconds=self.COOLDOWN_HOURS * 3600):
            return True

        return False
