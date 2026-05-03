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
from ..alpha.fair_value import FairValueError, get_fair_value_engine

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
    best_ask: float = 0.5
    vwap_ask: float = 0.5
    spread: float = 0.05
    question: str = ""
    features: Optional[dict] = None

    @classmethod
    def from_dict(cls, city: str, signal_dict: dict) -> Signal:
        """Helper to convert a standard signal dictionary to a Signal dataclass."""
        ml = signal_dict.get("ml", {})
        confidence = float(ml.get("confidence", 0.0))

        # Calibration: based on historical MAE (Mean Absolute Error)
        mae = float(ml.get("mae", 1.5))
        calibration = max(0.0, 1.0 - (mae / 4.0))  # 0% if MAE >= 4 degrees

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
            best_ask=float(signal_dict.get("best_ask", signal_dict.get("entry_price", 0.5))),
            vwap_ask=float(signal_dict.get("vwap_ask", signal_dict.get("best_ask", 0.5))),
            spread=float(signal_dict.get("spread", 0.05)),
            question=signal_dict.get("question", ""),
            features=signal_dict.get("features"),
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
        self.fair_value_engine = get_fair_value_engine()

        # Thresholds
        self.MIN_CONFIDENCE = getattr(config, "signal_min_confidence", 0.50)
        self.MIN_EDGE = getattr(config, "signal_min_edge", 0.02)
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
            except (Exception,) as e:
                logger.error(f"Anomaly check failed: {e}")

        return None

    def validate(self, signal: Signal) -> dict:
        """Validate signal through quality layer. Returns dict with 'accepted' and 'score'."""
        # 1. Hard rules (gates)
        hard_reason = self.validate_hard_rules(signal)
        if hard_reason:
            return {"accepted": False, "reason": hard_reason, "score": 0.0}

        # 2. Compute quality score
        quality_score = self.compute_quality(signal)

        # 3. Check minimum quality threshold
        min_quality = getattr(self.config, "signal_min_quality_score", 0.40)

        return {
            "accepted": quality_score >= min_quality,
            "score": quality_score,
            "reason": f"quality_{quality_score:.2f}" if quality_score < min_quality else "ok",
        }

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
            except (Exception,) as e:
                logger.error(f"Bayesian scoring failed: {e}")

        # 3. Sentiment Boost (#6)
        sentiment_boost = 0.0
        if is_enabled("SENTIMENT_WEIGHTED_SIGNALS"):
            try:
                sentiment_boost = self.sentiment_analyzer.analyze_signal(signal.city, signal.question)
            except (Exception,) as e:
                pass

        # 6. Fair Value Alpha (#7 - V3 Improvement)
        alpha_bonus = 0.0
        if is_enabled("V3_FAIR_VALUE"):
            try:
                # Target time is the resolution date (simplified to current date + horizon)
                # In real code we'd use the actual market resolution date
                from datetime import datetime, timedelta, timezone

                target_dt = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(
                    days=1
                )

                fair_v = self.fair_value_engine.calculate_fair_value(signal.city, signal.price, target_dt)

                # PR #4: Calibration Gate (temporairement désactivé pour debug)
                # if not self.fair_value_engine.check_calibration_gate(signal.city, "ensemble", fair_v - signal.vwap_ask):
                #     return 0.0

                # Use VWAP edge (against average execution price)
                exec_edge = self.fair_value_engine.get_vwap_edge(fair_v, signal.vwap_ask, signal.spread)

                if exec_edge > 0.05:
                    alpha_bonus = min(0.15, exec_edge * 0.5)
                    logger.info(f"[V3-ALPHA] Found VWAP mispricing for {signal.city}: {exec_edge:.2%}")
                else:
                    if fair_v - signal.vwap_ask > 0.05:
                        logger.warning(f"[V3-BLOCK] Alpha blocked by VWAP/Spread: {signal.spread:.3f} > edge")
            except FairValueError as e:
                logger.info("V3 Alpha unavailable for %s: %s", signal.city, e)
            except (Exception,) as e:
                logger.exception("V3 Alpha check crashed for %s", signal.city)

        # Final Weighted Score (poids normalisés pour sum=1.0)
        # Normaliser edge (EV) dans [0, 1] : 0.10 EV = 1.0, donc edge_norm = min(1.0, edge / 0.10)
        edge_norm = min(1.0, signal.edge / 0.10)
        score = (
            0.35 * signal.confidence
            + 0.25 * edge_norm
            + 0.15 * signal.calibration
            + 0.10 * signal.market_stability
            + 0.15 * sentiment_boost
            + alpha_bonus
            - bayesian_penalty
        )
        logger.info(f"[QUALITY] {signal.city}: conf={signal.confidence:.2f} edge={signal.edge:.2f}->norm={edge_norm:.2f} -> score={score:.2f}")
        return round(max(0.0, min(1.0, score)), 4)

    def commit(self, signal: Signal) -> None:
        """Persist signal cooldown/idempotence markers after an emitted signal."""
        from ..trading.idempotence import get_idempotence_manager

        manager = get_idempotence_manager()
        manager.is_duplicate("signal", f"{signal.market_id}:{signal.city}", window_seconds=24 * 3600)
        manager.is_duplicate("city_cooldown", f"city_cooldown:{signal.city}", window_seconds=self.COOLDOWN_HOURS * 3600)

    def is_duplicate(self, signal: Signal) -> bool:
        """Check for duplicates or city-level cooldowns."""
        from ..trading.idempotence import get_idempotence_manager

        key = f"{signal.market_id}:{signal.city}"
        if get_idempotence_manager().is_duplicate("signal", key, window_seconds=24 * 3600):
            return True

        city_key = f"city_cooldown:{signal.city}"
        if get_idempotence_manager().is_duplicate("city_cooldown", city_key, window_seconds=self.COOLDOWN_HOURS * 3600):
            return True

        return False


# Audit: Includes fee and slippage awareness
