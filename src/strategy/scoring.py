"""
Global opportunity scoring and ranking.
"""
from __future__ import annotations

from dataclasses import dataclass

from ..data.schema import DatasetRow


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _label_to_factor(label: str | None) -> float:
    if label == "EXTREME":
        return 1.0
    if label == "VOLATILE":
        return 0.72
    return 0.45


def _risk_penalty(signal: dict, trade_context: dict) -> float:
    spread = float(signal.get("spread", 0.0) or 0.0)
    uncertainty = float(signal.get("edge_penalties", {}).get("uncertainty", 0.0))
    horizon_text = str(trade_context.get("horizon", ""))
    horizon_penalty = 0.08 if horizon_text.startswith("D+3") or horizon_text.startswith("D+4") else 0.04
    spread_penalty = _clamp(spread / 0.08) * 0.22
    uncertainty_penalty = _clamp(uncertainty / 0.20) * 0.28
    return round(spread_penalty + uncertainty_penalty + horizon_penalty, 4)


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

    def score_trade(self, signal: dict, trade_context: dict) -> float:
        """Compute a normalized score for one candidate trade."""
        edge = _clamp(max(float(signal.get("ev", 0.0) or 0.0), 0.0) / 0.25)
        confidence = _clamp(float(trade_context.get("confidence", 0.0) or 0.0))
        liquidity = _clamp(float(trade_context.get("liquidity", 0.0) or 0.0) / 5000.0)
        regime = _label_to_factor(trade_context.get("regime_label"))
        market_edge = _clamp(max(float(signal.get("p", 0.0) or 0.0) - float(signal.get("entry_price", 0.0) or 0.0), 0.0) / 0.20)
        risk_penalty = _risk_penalty(signal, trade_context)

        score = (
            0.36 * edge
            + 0.24 * confidence
            + 0.16 * liquidity
            + 0.12 * regime
            + 0.12 * market_edge
            - risk_penalty
        )
        return round(_clamp(score), 4)

    def rank(self, candidates: list[dict]) -> list[ScoredTrade]:
        """Return sorted candidates with stable ranks."""
        scored: list[tuple[float, dict]] = []
        for candidate in candidates:
            signal = candidate["signal"]
            trade_context = candidate["trade_context"]
            score = self.score_trade(signal, trade_context)
            scored.append((score, candidate))

        scored.sort(
            key=lambda item: (
                item[0],
                float(item[1]["signal"].get("ev", 0.0) or 0.0),
                float(item[1]["trade_context"].get("confidence", 0.0) or 0.0),
                str(item[1]["city"]),
            ),
            reverse=True,
        )

        ranked: list[ScoredTrade] = []
        for index, (score, candidate) in enumerate(scored, start=1):
            ranked.append(
                ScoredTrade(
                    score=score,
                    rank=index,
                    city=candidate["city"],
                    date=candidate["date"],
                    market_id=candidate["signal"]["market_id"],
                    signal=candidate["signal"],
                    trade_context=candidate["trade_context"],
                )
            )
        return ranked

    def score_row(self, row: DatasetRow) -> float:
        """Score a dataset row for backtesting and diagnostics."""
        signal = {
            "ev": row.adjusted_ev if row.adjusted_ev is not None else row.raw_ev if row.raw_ev is not None else 0.0,
            "p": row.calibrated_prob if row.calibrated_prob is not None else row.market_implied_prob if row.market_implied_prob is not None else 0.0,
            "entry_price": row.market_price if row.market_price is not None else 0.0,
            "spread": row.spread if row.spread is not None else 0.0,
            "edge_penalties": {
                "uncertainty": 0.0 if row.forecast_spread is None else min(float(row.forecast_spread) / 20.0, 0.20),
            },
        }
        trade_context = {
            "confidence": row.confidence if row.confidence is not None else 0.0,
            "liquidity": row.liquidity if row.liquidity is not None else 0.0,
            "regime_label": "EXTREME"
            if (row.forecast_spread is not None and row.forecast_spread >= 4.0)
            else "VOLATILE"
            if (row.forecast_spread is not None and row.forecast_spread >= 2.0)
            else "STABLE",
            "horizon": row.forecast_horizon or "",
        }
        return self.score_trade(signal, trade_context)
