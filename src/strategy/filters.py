"""
Trading filters for market selection.

Each filter returns:
    {
        "passed": bool,
        "reason": str,
        "metrics": dict
    }
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional
import logging

logger = logging.getLogger(__name__)


@dataclass
class FilterResult:
    """Result from a single filter check."""
    passed: bool
    reason: str
    metrics: Dict[str, Any]


class VolumeFilter:
    """Filter markets by minimum volume."""

    def __init__(self, min_volume: float):
        self.min_volume = min_volume

    def check(self, outcome: dict, config: Any = None) -> FilterResult:
        volume = float(outcome.get("volume", 0))
        passed = volume >= self.min_volume
        return FilterResult(
            passed=passed,
            reason="" if passed else f"volume_too_low ({volume:.0f} < {self.min_volume})",
            metrics={"volume": volume, "min_volume": self.min_volume},
        )


class SpreadFilter:
    """Filter markets by maximum spread."""

    def __init__(self, max_spread: float):
        self.max_spread = max_spread

    def check(self, outcome: dict, config: Any = None) -> FilterResult:
        spread = float(outcome.get("spread", 0.0))
        passed = spread <= self.max_spread
        return FilterResult(
            passed=passed,
            reason="" if passed else f"spread_too_high ({spread:.1%} > {self.max_spread:.1%})",
            metrics={"spread": spread, "max_spread": self.max_spread},
        )


class LiquidityFilter:
    """Filter markets by orderbook depth at ask price (point 3)."""

    def __init__(self, min_depth_usd: float):
        self.min_depth_usd = min_depth_usd

    def check(self, outcome: dict, orderbook: Optional[dict] = None, config: Any = None) -> FilterResult:
        bid = float(outcome.get("bid", 0.0))
        ask = float(outcome.get("ask", 1.0))
        if ask <= bid or ask < 0.01:
            return FilterResult(
                passed=False,
                reason="crossed_or_invalid_book",
                metrics={"bid": bid, "ask": ask},
            )
        ob = orderbook or outcome.get("orderbook")
        depth = self._depth_at_price(ob, ask) if ob else float(outcome.get("volume", 0))
        passed = depth >= self.min_depth_usd
        if not passed:
            # Instead of reject, suggest size reduction
            from src.strategy.sizing import reduce_size_to_liquidity
            intended = float(outcome.get("intended_size", 10.0))
            adjusted, reason = reduce_size_to_liquidity(intended, ob, ask, self.min_depth_usd)
            if adjusted > 0:
                outcome["adjusted_size"] = adjusted
                outcome["liquidity_note"] = reason
                return FilterResult(
                    passed=True,
                    reason=f"size_reduced ({reason})",
                    metrics={"depth_usd": depth, "min_depth_usd": self.min_depth_usd, "ask": ask, "adjusted_size": adjusted},
                )
        return FilterResult(
            passed=passed,
            reason="" if passed else f"depth_too_low ({depth:.1f} < {self.min_depth_usd})",
            metrics={"depth_usd": depth, "min_depth_usd": self.min_depth_usd, "ask": ask},
        )

    def _depth_at_price(self, orderbook: dict, price: float) -> float:
        """Calculate available liquidity at or better than price (for buys)."""
        asks = orderbook.get("asks", [])
        depth = 0.0
        for level in asks:
            if isinstance(level, dict):
                level_price = float(level.get("price", 0.0))
                level_size = float(level.get("size", 0.0))
            else:
                level_price = float(level[0])
                level_size = float(level[1])
            if level_price <= price + 1e-6:
                depth += level_size * level_price
        return depth


class EVFilter:
    """Filter trades by minimum net EV."""

    def __init__(self, min_edge: float, require_positive_net_ev: bool = True):
        self.min_edge = min_edge
        self.require_positive_net_ev = require_positive_net_ev

    def check(self, net_ev: float, gross_edge: float, config: Any = None) -> FilterResult:
        passed = gross_edge >= self.min_edge
        if self.require_positive_net_ev:
            passed = passed and net_ev > 0
        reason = ""
        if not passed:
            if gross_edge < self.min_edge:
                reason = f"edge_too_low ({gross_edge:.1%} < {self.min_edge:.1%})"
            elif net_ev <= 0:
                reason = f"net_ev_negative ({net_ev:.4f})"
        return FilterResult(
            passed=passed,
            reason=reason,
            metrics={"gross_edge": gross_edge, "net_ev": net_ev, "min_edge": self.min_edge},
        )


class AntiCrossedBookFilter:
    """Reject crossed or invalid orderbooks."""
    
    def check(self, outcome: dict, config: Any = None) -> FilterResult:
        bid = float(outcome.get("bid", 0.0))
        ask = float(outcome.get("ask", 1.0))
        if bid <= 0 or ask <= 0:
            return FilterResult(passed=False, reason="missing_bid_ask", metrics={"bid": bid, "ask": ask})
        if ask <= bid:
            return FilterResult(passed=False, reason="crossed_book", metrics={"bid": bid, "ask": ask})
        # LOWERED from 0.01 to 0.001 to allow very low prices (Polymarket often has 0.005)
        if ask < 0.001:
            return FilterResult(passed=False, reason="price_too_low", metrics={"ask": ask})
        return FilterResult(passed=True, reason="", metrics={"bid": bid, "ask": ask})


class ConfidenceFilter:
    """Filter by minimum model confidence."""

    def __init__(self, min_confidence: float = 0.15):
        self.min_confidence = min_confidence

    def check(self, features: dict, config: Any = None) -> FilterResult:
        confidence = features.get("confidence")
        if confidence is None:
            return FilterResult(passed=True, reason="", metrics={"confidence": None})
        passed = float(confidence) >= self.min_confidence
        return FilterResult(
            passed=passed,
            reason="" if passed else f"confidence_too_low ({confidence:.1%} < {self.min_confidence:.1%})",
            metrics={"confidence": confidence, "min_confidence": self.min_confidence},
        )


class SourceContradictionFilter:
    """Skip when GFS and ECMWF disagree significantly."""

    def check(self, features: dict, config: Any = None) -> FilterResult:
        ecmwf = features.get("ecmwf_max")
        gfs = features.get("gfs_max")
        if ecmwf is not None and gfs is not None:
            diff = abs(float(ecmwf) - float(gfs))
            if diff > 5.0:
                return FilterResult(
                    passed=False,
                    reason=f"source_contradiction (diff={diff:.1f}°C)",
                    metrics={"ecmwf": ecmwf, "gfs": gfs, "diff": diff},
                )
        return FilterResult(passed=True, reason="", metrics={"ecmwf": ecmwf, "gfs": gfs})


def run_all_filters(outcome: dict, features: dict, orderbook: Optional[dict],
                    net_ev: float, gross_edge: float, config) -> Dict[str, Any]:
    """Run filters with relaxed thresholds for opportunistic trading."""
    results = {}
    filter_classes = [
        ("volume", VolumeFilter(getattr(config, "min_volume", 1))),
        ("spread", SpreadFilter(getattr(config, "max_spread", 0.25))),
        ("liquidity", LiquidityFilter(getattr(config, "min_orderbook_depth_usd", 0.5))),
        ("ev", EVFilter(getattr(config, "min_edge", 0.01), getattr(config, "require_positive_net_ev", False))),
        ("confidence", ConfidenceFilter(getattr(config, "min_confidence", 0.01))),
    ]

    all_passed = True
    reasons = []
    for name, filt in filter_classes:
        if isinstance(filt, LiquidityFilter):
            res = filt.check(outcome, orderbook)  # Pass orderbook directly
        elif isinstance(filt, EVFilter):
            res = filt.check(net_ev, gross_edge)
        elif isinstance(filt, ConfidenceFilter):
            res = filt.check(features)
        else:
            res = filt.check(outcome)
        results[name] = res
        if not res.passed:
            all_passed = False
            reasons.append(f"{name}: {res.reason}")
    return {
        "passed": all_passed,
        "rejected_reason": "; ".join(reasons) if reasons else "",
        "filter_results": results,
    }
