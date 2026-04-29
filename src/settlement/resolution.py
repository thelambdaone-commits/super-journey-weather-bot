"""
Settlement and resolution module.

Fetches official results from Polymarket, handles cancelled,
invalid, ambiguous, or unresolved markets.
Updates later_result in logs/trades.jsonl.
"""

from __future__ import annotations
import json
import logging
from typing import Optional, Dict, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# Resolution outcomes
RESOLUTION_YES = "YES"
RESOLUTION_NO = "NO"
RESOLUTION_CANCELLED = "CANCELLED"
RESOLUTION_INVALID = "INVALID"
RESOLUTION_AMBIGUOUS = "AMBIGUOUS"
RESOLUTION_PENDING = "PENDING"


def fetch_market_resolution(market_id: str, config=None) -> Dict[str, Any]:
    """
    Fetch resolution data from Polymarket Gamma API.
    Returns dict with: outcome, resolution_price, resolved_at, etc.
    """
    try:
        import requests
        resp = requests.get(
            f"https://gamma-api.polymarket.com/markets/{market_id}",
            timeout=(3, 5),
        )
        if not resp.ok:
            logger.warning(f"Failed to fetch market {market_id}: HTTP {resp.status_code}")
            return {"outcome": RESOLUTION_PENDING, "error": f"HTTP {resp.status_code}"}

        data = resp.json()
        closed = data.get("closed", False)
        if not closed:
            return {"outcome": RESOLUTION_PENDING}

        # Parse outcomePrices: [YES_price, NO_price]
        try:
            prices_str = data.get("outcomePrices", "[0.5, 0.5]")
            prices = json.loads(prices_str)
            yes_price = float(prices[0]) if len(prices) > 0 else 0.5
            no_price = float(prices[1]) if len(prices) > 1 else 0.5
        except (json.JSONDecodeError, ValueError, IndexError):
            return {"outcome": RESOLUTION_AMBIGUOUS, "error": "invalid_outcome_prices"}

        # Determine outcome
        if yes_price >= 0.95:
            outcome = RESOLUTION_YES
        elif yes_price <= 0.05:
            outcome = RESOLUTION_NO
        elif 0.45 <= yes_price <= 0.55:
            outcome = RESOLUTION_CANCELLED  # Likely cancelled/refunded
        else:
            outcome = RESOLUTION_AMBIGUOUS

        return {
            "outcome": outcome,
            "yes_price": yes_price,
            "no_price": no_price,
            "resolved_at": data.get("endDate"),
            "question": data.get("question", ""),
            "market_id": market_id,
        }

    except (Exception,) as e:
        logger.error(f"Error fetching resolution for {market_id}: {e}")
        return {"outcome": RESOLUTION_PENDING, "error": str(e)}


def update_trades_jsonl(market_id: str, outcome: str, filepath: str = "logs/trades.jsonl") -> int:
    """
    Update later_result in trades.jsonl for a given market_id.
    Returns number of records updated.
    """
    try:
        import pathlib
        path = pathlib.Path(filepath)
        if not path.exists():
            return 0

        lines = path.read_text(encoding="utf-8").splitlines()
        updated_lines = []
        updated_count = 0

        for line in lines:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                if record.get("market_id") == market_id:
                    record["later_result"] = outcome
                    record["resolved_at"] = datetime.utcnow().isoformat()
                    updated_count += 1
                updated_lines.append(json.dumps(record))
            except json.JSONDecodeError:
                updated_lines.append(line)  # Keep malformed lines

        path.write_text("\n".join(updated_lines) + "\n", encoding="utf-8")
        logger.info(f"Updated {updated_count} trade records for market {market_id}")
        return updated_count

    except (Exception,) as e:
        logger.error(f"Failed to update trades JSONL: {e}")
        return 0


class ResolutionEngine:
    """
    Handles resolution checking and PnL calculation.
    """

    def __init__(self, config=None):
        self.config = config

    def resolve_position(
        self, market_id: str, token_id: str, side: str, size: float, entry_price: float
    ) -> Dict[str, Any]:
        """
        Resolve a position and calculate PnL.
        """
        resolution = fetch_market_resolution(market_id, self.config)
        outcome = resolution.get("outcome", RESOLUTION_PENDING)

        pnl = 0.0
        if outcome == RESOLUTION_YES and side == "BUY":
            # Won: get $1 per share minus fees
            pnl = size * (1.0 - entry_price)
        elif outcome == RESOLUTION_NO and side == "BUY":
            # Lost: lose entry cost
            pnl = -size * entry_price
        elif outcome == RESOLUTION_CANCELLED:
            # Refunded: get entry cost back
            pnl = 0.0
        elif outcome in (RESOLUTION_INVALID, RESOLUTION_AMBIGUOUS):
            # Often refunded
            pnl = 0.0

        return {
            "market_id": market_id,
            "token_id": token_id,
            "side": side,
            "size": size,
            "entry_price": entry_price,
            "outcome": outcome,
            "pnl": pnl,
            "resolution_details": resolution,
        }

    def batch_resolve(self, open_positions: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
        """Resolve multiple positions."""
        results = []
        for pos in open_positions:
            result = self.resolve_position(
                market_id=pos.get("market_id", ""),
                token_id=pos.get("token_id", ""),
                side=pos.get("side", "BUY"),
                size=pos.get("size", 0.0),
                entry_price=pos.get("entry_price", 0.0),
            )
            results.append(result)
            # Update trades JSONL
            if result["outcome"] != RESOLUTION_PENDING:
                update_trades_jsonl(result["market_id"], result["outcome"])
        return results
