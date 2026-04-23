"""
Price Trajectory Builder: Reconstructs temporal market trajectories from price history.
Builds features for time-series ML models.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional


@dataclass
class TrajectoryFeatures:
    """Features extracted from a price trajectory."""

    market_id: str
    start_price: float
    end_price: float
    price_change: float
    price_change_pct: float

    mean_price: float
    std_price: float
    max_price: float
    min_price: float

    volatility: float
    trend_score: float
    reversal_score: float

    volume_total: float
    volume_traded: float

    duration_hours: float
    n_ticks: int

    early_confidence: float
    late_confidence: float
    price_stability: float


class PriceTrajectoryBuilder:
    """Build price trajectories and extract temporal features."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.price_history_dir = self.data_dir / "price_history"

    def build_trajectory(
        self,
        price_history: list[dict],
        market_id: str,
    ) -> TrajectoryFeatures:
        """Build trajectory features from price history ticks."""

        if not price_history:
            return self._empty_trajectory(market_id)

        prices = [t.get("price", 0.5) for t in price_history]
        volumes = [t.get("volume", 0) for t in price_history]
        timestamps = [t.get("timestamp", 0) for t in price_history]

        if not prices:
            return self._empty_trajectory(market_id)

        start_price = prices[0]
        end_price = prices[-1]
        price_change = end_price - start_price
        price_change_pct = (price_change / start_price) if start_price > 0 else 0

        mean_price = sum(prices) / len(prices)
        variance = sum((p - mean_price) ** 2 for p in prices) / len(prices)
        std_price = math.sqrt(variance)
        max_price = max(prices)
        min_price = min(prices)

        volatility = std_price / mean_price if mean_price > 0 else 0

        trend = self._compute_trend(prices)
        trend_score = trend
        reversal_score = self._compute_reversal(prices)

        volume_total = sum(volumes)
        volume_traded = sum(v for v in volumes if v > 0)

        if len(timestamps) > 1:
            duration = max(timestamps) - min(timestamps)
            duration_hours = duration / 3600
        else:
            duration_hours = 0

        n_ticks = len(prices)

        early_idx = min(n_ticks // 4, 5)
        late_idx = max(n_ticks - 5, 0)
        early_confidence = 1 - abs(start_price - mean_price) / mean_price if mean_price > 0 else 0.5
        late_confidence = 1 - abs(end_price - mean_price) / mean_price if mean_price > 0 else 0.5

        stability = 1 - (std_price / mean_price) if mean_price > 0 else 0

        return TrajectoryFeatures(
            market_id=market_id,
            start_price=start_price,
            end_price=end_price,
            price_change=price_change,
            price_change_pct=price_change_pct,
            mean_price=mean_price,
            std_price=std_price,
            max_price=max_price,
            min_price=min_price,
            volatility=volatility,
            trend_score=trend_score,
            reversal_score=reversal_score,
            volume_total=volume_total,
            volume_traded=volume_traded,
            duration_hours=duration_hours,
            n_ticks=n_ticks,
            early_confidence=early_confidence,
            late_confidence=late_confidence,
            price_stability=stability,
        )

    def _compute_trend(self, prices: list[float]) -> float:
        """Compute price trend score (-1 to 1)."""
        if len(prices) < 2:
            return 0.0

        n = len(prices)
        half = n // 2

        early_mean = sum(prices[:half]) / half
        late_mean = sum(prices[half:]) / (n - half)

        if early_mean == 0:
            return 0.0

        trend = (late_mean - early_mean) / early_mean
        return max(-1, min(1, trend))

    def _compute_reversal(self, prices: list[float]) -> float:
        """Compute reversal score (high = reversal detected)."""
        if len(prices) < 4:
            return 0.0

        quarter = len(prices) // 4
        first_q = prices[:quarter]
        last_q = prices[-quarter:]

        first_mean = sum(first_q) / len(first_q)
        last_mean = sum(last_q) / len(last_q)
        mid_idx = len(prices) // 2
        mid_mean = sum(prices[mid_idx-2:mid_idx+2]) / 4

        if first_mean == 0 or last_mean == 0:
            return 0.0

        reversal = (first_mean - mid_mean) / first_mean + (mid_mean - last_mean) / mid_mean
        return max(0, reversal)

    def _empty_trajectory(self, market_id: str) -> TrajectoryFeatures:
        """Return empty trajectory for missing data."""
        return TrajectoryFeatures(
            market_id=market_id,
            start_price=0.5,
            end_price=0.5,
            price_change=0,
            price_change_pct=0,
            mean_price=0.5,
            std_price=0,
            max_price=0.5,
            min_price=0.5,
            volatility=0,
            trend_score=0,
            reversal_score=0,
            volume_total=0,
            volume_traded=0,
            duration_hours=0,
            n_ticks=0,
            early_confidence=0.5,
            late_confidence=0.5,
            price_stability=1,
        )

    def load_price_history(self, market_id: str) -> list[dict]:
        """Load price history from file."""
        path = self.price_history_dir / f"{market_id}_prices.jsonl"
        if not path.exists():
            return []

        history = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                history.append(json.loads(line))
        return history

    def build_trajectory_from_file(self, market_id: str) -> TrajectoryFeatures:
        """Build trajectory directly from file."""
        history = self.load_price_history(market_id)
        return self.build_trajectory(history, market_id)

    def build_all_trajectories(self, market_ids: list[str]) -> list[TrajectoryFeatures]:
        """Build trajectories for multiple markets."""
        trajectories = []
        for market_id in market_ids:
            traj = self.build_trajectory_from_file(market_id)
            trajectories.append(traj)
        return trajectories

    def to_dict(self, trajectory: TrajectoryFeatures) -> dict[str, Any]:
        """Convert trajectory to dict for storage."""
        return {
            "market_id": trajectory.market_id,
            "start_price": trajectory.start_price,
            "end_price": trajectory.end_price,
            "price_change": trajectory.price_change,
            "price_change_pct": trajectory.price_change_pct,
            "mean_price": trajectory.mean_price,
            "std_price": trajectory.std_price,
            "max_price": trajectory.max_price,
            "min_price": trajectory.min_price,
            "volatility": trajectory.volatility,
            "trend_score": trajectory.trend_score,
            "reversal_score": trajectory.reversal_score,
            "volume_total": trajectory.volume_total,
            "volume_traded": trajectory.volume_traded,
            "duration_hours": trajectory.duration_hours,
            "n_ticks": trajectory.n_ticks,
            "early_confidence": trajectory.early_confidence,
            "late_confidence": trajectory.late_confidence,
            "price_stability": trajectory.price_stability,
        }


class TemporalFeatureExtractor:
    """Extract time-series features for ML."""

    FEATURE_COLS = [
        "price_change",
        "volatility",
        "trend_score",
        "reversal_score",
        "price_stability",
        "duration_hours",
        "early_confidence",
        "late_confidence",
    ]

    def extract(self, trajectory: TrajectoryFeatures) -> dict[str, float]:
        """Extract features from trajectory."""
        return {
            "price_change": trajectory.price_change,
            "price_change_pct": trajectory.price_change_pct,
            "volatility": trajectory.volatility,
            "trend_score": trajectory.trend_score,
            "reversal_score": trajectory.reversal_score,
            "price_stability": trajectory.price_stability,
            "duration_hours": trajectory.duration_hours,
            "early_confidence": trajectory.early_confidence,
            "late_confidence": trajectory.late_confidence,
            "volume_traded": trajectory.volume_traded,
        }

    def extract_batch(self, trajectories: list[TrajectoryFeatures]) -> list[dict[str, float]]:
        """Extract features from multiple trajectories."""
        return [self.extract(t) for t in trajectories]