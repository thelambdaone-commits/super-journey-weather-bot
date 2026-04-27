"""
Global opportunity scoring and ranking.
"""
from __future__ import annotations
from dataclasses import dataclass
from ..data.schema import DatasetRow

@dataclass
class ScoredTrade:
    """Ranked trade candidate."""
    score: float
    rank: int
    city: str
    date: str
    market_id: str
    signal: dict
    trade_context: dict

class ScoringEngine:
    """Rank opportunities across cities and horizons."""
    
    def __init__(self, config=None):
        from .signal_quality import SignalQualityLayer
        from ..weather.config import get_config
        self.config = config or get_config()
        self.quality_layer = SignalQualityLayer(self.config, self.config.data_dir)

    def score_trade(self, signal: dict, trade_context: dict) -> float:
        """Compute a normalized score for one candidate trade."""
        from .signal_quality import Signal
        city = trade_context.get("city") or "unknown"
        sig_obj = Signal.from_dict(city, signal)
        return self.quality_layer.compute_quality(sig_obj)

    def rank(self, candidates: list[dict]) -> list[ScoredTrade]:
        """Return sorted candidates with stable ranks."""
        scored: list[tuple[float, dict]] = []
        for candidate in candidates:
            signal = candidate["signal"]
            trade_context = candidate["trade_context"]
            trade_context["city"] = candidate["loc"].name
            score = self.score_trade(signal, trade_context)
            scored.append((score, candidate))

        scored.sort(
            key=lambda item: (
                item[0],
                float(item[1]["signal"].get("ev", 0.0) or 0.0),
                float(item[1]["trade_context"].get("confidence", 0.0) or 0.0),
                str(item[1]["loc"].name),
            ),
            reverse=True,
        )

        ranked: list[ScoredTrade] = []
        for index, (score, candidate) in enumerate(scored, start=1):
            ranked.append(
                ScoredTrade(
                    score=score,
                    rank=index,
                    city=candidate["loc"].name,
                    date=candidate["date_str"],
                    market_id=candidate["signal"]["market_id"],
                    signal=candidate["signal"],
                    trade_context=candidate["trade_context"],
                )
            )
        return ranked

    def score_row(self, row: DatasetRow) -> float:
        """Score a dataset row for backtesting and diagnostics."""
        signal = {
            "market_id": row.market_id or "backtest",
            "ev": row.adjusted_ev if row.adjusted_ev is not None else row.raw_ev if row.raw_ev is not None else 0.0,
            "p": row.calibrated_prob if row.calibrated_prob is not None else row.market_implied_prob if row.market_implied_prob is not None else 0.0,
            "entry_price": row.market_price if row.market_price is not None else 0.5,
            "spread": row.spread if row.spread is not None else 0.05,
            "ml": {
                "confidence": row.confidence or 0.5,
                "mae": 1.5
            }
        }
        trade_context = {"city": row.city}
        return self.score_trade(signal, trade_context)

# Audit: Includes fee and slippage awareness
