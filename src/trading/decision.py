"""
Trade decision module.

Supports 6 actions: BUY / SKIP / WAIT / REPRICE / CANCEL / REDUCE_SIZE
Distinction:
- SKIP = not interesting (negative EV, low volume, etc.)
- WAIT = good edge but bad liquidity/price (bid/ask moved)
- REPRICE = edge still good but ask moved (recalculate size)
- REDUCE_SIZE = insufficient depth (reduce size instead of cancelling)
- CANCEL = cancel remaining unfilled portion of a PARTIAL order
"""

from __future__ import annotations
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from datetime import datetime
import logging


@dataclass
class TradeDecision:
    """Complete trade decision with all context."""

    # Market context
    market_id: str
    event_slug: str
    location: str
    date: str
    outcome: str

    # Probabilities & pricing
    model_probability: float
    market_bid: float
    market_ask: float
    entry_price: float
    spread: float
    volume: float

    # Edge & EV
    gross_edge: float
    net_ev: float

    # Suggested size
    suggested_size: float

    # Decision
    action: str  # BUY / SKIP / WAIT / REPRICE / CANCEL / REDUCE_SIZE
    passed_filters: bool
    rejected_reason: str = ""

    # Metadata
    timestamp: float = field(default_factory=lambda: datetime.utcnow().timestamp())
    order_id: Optional[str] = None
    fill_state: Optional[str] = None  # PENDING / PARTIAL / FILLED / etc.

    # Additional context
    extra: Dict[str, Any] = field(default_factory=dict)

    def should_trade(self) -> bool:
        """True if action is BUY."""
        return self.action == "BUY"

    def is_terminal(self) -> bool:
        """Decision reached a final state (no further action needed)."""
        return self.action in ("SKIP", "CANCEL")  # BUY is NOT terminal (order may be pending/partial)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary for JSONL logging."""
        return {
            "timestamp": datetime.utcfromtimestamp(self.timestamp).isoformat(),
            "market_id": self.market_id,
            "event_slug": self.event_slug,
            "location": self.location,
            "date": self.date,
            "outcome": self.outcome,
            "model_probability": self.model_probability,
            "market_bid": self.market_bid,
            "market_ask": self.market_ask,
            "entry_price": self.entry_price,
            "spread": self.spread,
            "volume": self.volume,
            "gross_edge": self.gross_edge,
            "net_ev": self.net_ev,
            "suggested_size": self.suggested_size,
            "action": self.action,
            "passed_filters": self.passed_filters,
            "rejected_reason": self.rejected_reason,
            "order_id": self.order_id,
            "fill_state": self.fill_state,
            **self.extra,
        }


class DecisionEngine:
    """
    Evaluates all factors and produces a TradeDecision.
    """

    def __init__(self, config, filter_runner=None, edge_calculator=None, sizer=None):
        self.config = config
        # Default to real implementations if not provided
        if filter_runner is None:
            from ..strategy.filters import run_all_filters
            self.filter_runner = run_all_filters
        else:
            self.filter_runner = filter_runner
            
        if edge_calculator is None:
            # Import pure functions from refactored edge.py
            from ..strategy.edge import gross_edge, net_ev, estimate_fee, estimate_slippage
            from dataclasses import dataclass
            
            @dataclass
            class EdgeEst:
                gross_edge: float
                net_ev: float
                
            def edge_calc(prob, ask, bid, vol, size, orderbook):
                fee = estimate_fee(ask, size, None)
                slip = estimate_slippage(orderbook or {}, size)
                net = net_ev(prob, ask, fee, slip)
                gross = gross_edge(prob, ask)
                return EdgeEst(gross_edge=gross, net_ev=net)
            
            self.edge_calculator = edge_calc
        else:
            self.edge_calculator = edge_calculator
            
        if sizer is None:
            from ..strategy.sizing import final_position_size
            self.sizer = final_position_size
        else:
            self.sizer = sizer

    def evaluate(self, context: Dict[str, Any]) -> TradeDecision:
        """
        Full evaluation: filters -> edge -> sizing -> decision.
        Context must contain: outcome, features, orderbook, model_probability, etc.
        """
        outcome = context.get("outcome", {})
        features = context.get("features", {})
        orderbook = context.get("orderbook")
        model_prob = context.get("model_probability", 0.5)
        market_id = outcome.get("market_id", "")
        token_id = outcome.get("token_id", "")

        # Get bid/ask
        bid = float(outcome.get("bid", outcome.get("best_bid", 0.0)) or 0.0)
        ask = float(outcome.get("ask", outcome.get("best_ask", 0.0)) or 0.0)
        volume = float(outcome.get("volume", 0))
        spread = ask - bid if ask > bid else 0.0
        filter_outcome = dict(outcome)
        filter_outcome.update({"bid": bid, "ask": ask, "spread": spread})

        # Calculate edge
        if self.edge_calculator:
            edge_est = self.edge_calculator(model_prob, ask, bid, volume, context.get("size", 0), orderbook)
            gross = edge_est.gross_edge
            net = edge_est.net_ev
        else:
            from ..strategy.edge import gross_edge, net_ev, estimate_fee, estimate_slippage
            from ..weather.config import get_config
            config = get_config()
            gross = gross_edge(model_prob, ask)
            fee = estimate_fee(ask, context.get("size", 0), config)
            slip = estimate_slippage(orderbook or {}, context.get("size", 0))
            net = net_ev(model_prob, ask, fee, slip)

        # Run filters
        filter_result = {"passed": True, "rejected_reason": "", "filter_results": {}}
        if self.filter_runner:
            # filter_runner is run_all_filters function
            filter_result = self.filter_runner(
                filter_outcome, features, orderbook, net, gross, self.config
            )
        else:
            # Fallback: run_all_filters directly
            from ..strategy.filters import run_all_filters
            filter_result = run_all_filters(
                filter_outcome, features, orderbook, net, gross, self.config
            )

        # Sizing
        suggested_size = 0.0
        if filter_result["passed"] and self.sizer:
            sizing = self.sizer(
                model_prob,
                ask,
                context.get("bankroll", 10000),
                self.config,
                context.get("current_market_exposure", 0.0),
                context.get("daily_pnl", 0.0),
            )
            suggested_size = sizing.final_size
        elif filter_result["passed"]:
            from ..strategy.sizing import final_position_size
            from ..weather.config import get_config
            config = get_config()
            sizing = final_position_size(model_prob, ask, context.get("bankroll", 10000), config)
            suggested_size = sizing.final_size

        # Determine action
        action = "SKIP"
        reason = filter_result.get("rejected_reason", "unknown")
        if filter_result["passed"]:
            if suggested_size > 0:
                action = "BUY"
            else:
                action = "SKIP"
                reason = getattr(sizing, "reason", "size_capped_to_zero") if "sizing" in locals() else "size_capped_to_zero"
        elif "depth_too_low" in reason:
            action = "REDUCE_SIZE"

        return TradeDecision(
            market_id=market_id,
            event_slug=context.get("event_slug", ""),
            location=context.get("location", ""),
            date=context.get("date", ""),
            outcome=outcome.get("question", ""),
            model_probability=model_prob,
            market_bid=bid,
            market_ask=ask,
            entry_price=ask,
            spread=spread,
            volume=volume,
            gross_edge=gross,
            net_ev=net,
            suggested_size=suggested_size,
            action=action,
            passed_filters=filter_result["passed"],
            rejected_reason="" if action == "BUY" else reason,
        )


def log_decision_jsonl(decision: TradeDecision, filepath: str = "logs/trades.jsonl") -> None:
    """Append decision to JSONL log (for paper trading and audit)."""
    import json
    from pathlib import Path
    try:
        Path(filepath).parent.mkdir(parents=True, exist_ok=True)
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(json.dumps(decision.to_dict()) + "\n")
    except (Exception,) as e:
        logging.getLogger(__name__).warning(f"Failed to log decision: {e}")
