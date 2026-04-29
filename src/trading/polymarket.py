"""
Polymarket API client.
"""

import json
import re
import time
import requests
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

GAMMA_HOST = "https://gamma-api.polymarket.com"
CLOB_HOST = "https://clob.polymarket.com"
STALE_TOKEN_TTL_SECONDS = 24 * 60 * 60
STALE_TOKEN_FILE = Path("data/stale_clob_tokens.json")
ORDERBOOK_SNAPSHOT_FILE = Path("data/orderbook_snapshots.jsonl")
TRANSIENT_CLOB_STATUS = {408, 425, 429, 500, 502, 503, 504}
_stale_tokens: dict[str, float] | None = None


def get_fee_rate(token_id: str = None, market_id: str = None, config = None) -> float:
    """
    Get fee rate from Polymarket API.
    Tries GET /fee-rate endpoint, falls back to config.estimated_fee_bps / 10000.
    Returns fee as decimal (e.g., 0.001 = 0.1%).
    """
    try:
        params = {}
        if token_id:
            params["token_id"] = token_id
        elif market_id:
            params["market"] = market_id
        resp = requests.get(f"{GAMMA_HOST}/fee-rate", params=params, timeout=(3, 5))
        if resp.ok:
            data = resp.json()
            rate = float(data.get("feeRate", data.get("fee_rate", 0))) / 10000.0 if "feeRate" in data or "fee_rate" in data else None
            if rate is not None:
                logger.debug(f"Fee rate from API: {rate:.4%}")
                return rate
    except (Exception,) as e:
        logger.debug(f"Could not fetch fee-rate: {e}")

    # Fallback to config
    if config and hasattr(config, 'estimated_fee_bps'):
        return config.estimated_fee_bps / 10000.0
    return 0.001  # Default 0.1%


def log_orderbook_snapshot(token_id: str, market_id: str, orderbook: dict) -> None:
    """
    Log full orderbook snapshot to JSONL for future backtesting.
    Called every time we fetch an orderbook (point 2: start logging NOW).
    """
    if not orderbook or not token_id:
        return
    snapshot = {
        "timestamp": datetime.utcnow().isoformat(),
        "token_id": str(token_id),
        "market_id": str(market_id or ""),
        "bids": orderbook.get("bids", []),
        "asks": orderbook.get("asks", []),
        "last_trade_price": orderbook.get("last_trade_price"),
        "tick_size": orderbook.get("tick_size"),
    }
    try:
        ORDERBOOK_SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
        with open(ORDERBOOK_SNAPSHOT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(snapshot) + "\n")
    except (Exception,) as e:
        logger.warning(f"Failed to log orderbook snapshot: {e}")


def get_polymarket_event(city_slug: str, month: str, day: int, year: int) -> Optional[Dict]:
    """Get Polymarket event for city/date."""
    slug = f"highest-temperature-in-{city_slug}-on-{month}-{day}-{year}"
    try:
        r = requests.get(f"https://gamma-api.polymarket.com/events?slug={slug}", timeout=(5, 8))
        data = r.json()
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
    except (Exception,) as e:
        logger.error(f"Error fetching Polymarket event for {city_slug}: {e}")
    return None


def get_market(market_id: str) -> Optional[Dict]:
    """Get market data."""
    try:
        r = requests.get(f"https://gamma-api.polymarket.com/markets/{market_id}", timeout=(3, 5))
        return r.json()
    except (Exception,) as e:
        logger.error(f"Error fetching Polymarket market {market_id}: {e}")
        return None


def _as_float(value, default: float | None = None) -> float | None:
    """Parse numeric API fields without letting malformed values leak into pricing."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _load_stale_tokens() -> dict[str, float]:
    global _stale_tokens
    if _stale_tokens is not None:
        return _stale_tokens
    try:
        data = json.loads(STALE_TOKEN_FILE.read_text(encoding="utf-8"))
        _stale_tokens = {str(token): float(ts) for token, ts in data.items()}
    except (Exception,):
        _stale_tokens = {}
    return _stale_tokens


def _save_stale_tokens(tokens: dict[str, float]) -> None:
    STALE_TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    STALE_TOKEN_FILE.write_text(json.dumps(tokens, indent=2, sort_keys=True), encoding="utf-8")


def prune_stale_tokens(now: float | None = None) -> int:
    """Remove stale-token suppressions older than the TTL."""
    global _stale_tokens
    now = now or time.time()
    tokens = _load_stale_tokens()
    before = len(tokens)
    tokens = {token: ts for token, ts in tokens.items() if now - ts < STALE_TOKEN_TTL_SECONDS}
    removed = before - len(tokens)
    if removed or tokens != _load_stale_tokens():
        _stale_tokens = tokens
        _save_stale_tokens(tokens)
    return removed


def is_stale_token(token_id: str, now: float | None = None) -> bool:
    """Return True when a CLOB token was recently proven invalid."""
    if not token_id:
        return False
    now = now or time.time()
    tokens = _load_stale_tokens()
    ts = tokens.get(str(token_id))
    if ts is None:
        return False
    if now - ts >= STALE_TOKEN_TTL_SECONDS:
        prune_stale_tokens(now)
        return False
    return True


def mark_stale_token(token_id: str, now: float | None = None) -> None:
    """Suppress repeated CLOB requests for token IDs returning permanent 404s."""
    if not token_id:
        return
    tokens = _load_stale_tokens()
    token = str(token_id)
    first_seen = token not in tokens
    tokens[token] = now or time.time()
    _save_stale_tokens(tokens)
    if first_seen:
        logger.warning("CLOB token %s marked stale for 24h after HTTP 404", token)


def get_orderbook(token_id: str, market_id: str = None, max_attempts: int = 3, backoff_s: float = 0.25) -> Optional[Dict]:
    """Fetch the CLOB orderbook for one outcome token. Logs snapshot automatically."""
    token_id = str(token_id or "")
    if not token_id:
        return None
    prune_stale_tokens()
    if is_stale_token(token_id):
        return None

    attempts = max(1, int(max_attempts))
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(
                f"{CLOB_HOST}/book",
                params={"token_id": token_id},
                timeout=(3, 5),
            )
            if response.status_code == 404:
                mark_stale_token(token_id)
                return None
            if response.status_code in TRANSIENT_CLOB_STATUS:
                if attempt < attempts:
                    time.sleep(backoff_s * (2 ** (attempt - 1)))
                    continue
                logger.warning(
                    "CLOB book unavailable for token %s after %d attempts: HTTP %s",
                    token_id,
                    attempts,
                    response.status_code,
                )
                return None
            if not response.ok:
                logger.warning("CLOB book unavailable for token %s: HTTP %s", token_id, response.status_code)
                return None
            data = response.json()
            if not isinstance(data, dict):
                logger.warning("CLOB book response for token %s was not a JSON object", token_id)
                return None
            # Log snapshot for future backtesting (point 2: start logging NOW)
            log_orderbook_snapshot(token_id, market_id or "", data)
            return data
        except requests.RequestException as exc:
            if attempt < attempts:
                time.sleep(backoff_s * (2 ** (attempt - 1)))
                continue
            logger.warning("CLOB book request failed for token %s after %d attempts: %s", token_id, attempts, exc)
            return None
        except (ValueError, TypeError) as exc:
            logger.warning("CLOB book parse failed for token %s: %s", token_id, exc)
            return None

    return None


def get_vwap_for_size(orderbook: dict, target_usd: float, side: str = "ask") -> float:
    """
    Calculate the Volume-Weighted Average Price (VWAP) for a target USD size.
    side: 'ask' for buying, 'bid' for selling.
    """
    levels = orderbook.get("asks" if side == "ask" else "bids", [])
    if not levels:
        return 0.5

    accum_cost = 0.0
    accum_shares = 0.0
    remaining_usd = target_usd

    for level in levels:
        if isinstance(level, dict):
            price = float(level.get("price", 0.0))
            size_shares = float(level.get("size", 0.0))
        else:
            price_str, size_str = level
            price = float(price_str)
            size_shares = float(size_str)
        if price <= 0 or size_shares <= 0:
            continue
        level_usd = price * size_shares

        if remaining_usd <= level_usd:
            # We fill the remainder in this level
            fill_shares = remaining_usd / price
            accum_shares += fill_shares
            accum_cost += remaining_usd
            remaining_usd = 0
            break
        else:
            # We take the whole level
            accum_shares += size_shares
            accum_cost += level_usd
            remaining_usd -= level_usd

    if accum_shares == 0:
        first = levels[0] if levels else None
        if isinstance(first, dict):
            return float(first.get("price", 0.5))
        return float(first[0]) if first else 0.5

    # Return average price per share
    vwap = accum_cost / accum_shares

    # If we couldn't fill the whole size, we add a penalty
    if remaining_usd > 0:
        vwap *= 1.1

    return round(vwap, 4)


def refresh_outcome_orderbook(outcome: Dict) -> bool:
    """
    Replace indicative Gamma prices with executable CLOB bid/ask.

    Gamma's outcomePrices are display/implied prices, not an executable spread.
    Trading decisions must use the YES token orderbook: best ask to buy, best bid
    to mark/exit, and ask-bid spread for liquidity/friction filters.
    """
    book = get_orderbook(str(outcome.get("token_id") or ""), market_id=str(outcome.get("market_id") or ""))
    if not book:
        outcome["orderbook_status"] = "missing"
        return False

    bids = book.get("bids") or []
    asks = book.get("asks") or []
    bid_prices = [_as_float(level.get("price")) for level in bids if isinstance(level, dict)]
    ask_prices = [_as_float(level.get("price")) for level in asks if isinstance(level, dict)]
    bid_prices = [price for price in bid_prices if price is not None]
    ask_prices = [price for price in ask_prices if price is not None]

    if not bid_prices or not ask_prices:
        outcome["orderbook_status"] = "empty_side"
        return False

    best_bid = max(bid_prices)
    best_ask = min(ask_prices)
    if best_bid <= 0 or best_ask <= 0 or best_bid > best_ask:
        outcome["orderbook_status"] = "invalid_crossed"
        return False

    midpoint = (best_bid + best_ask) / 2
    bid_size = next(
        (_as_float(level.get("size"), 0.0) for level in bids if _as_float(level.get("price")) == best_bid), 0.0
    )
    ask_size = next(
        (_as_float(level.get("size"), 0.0) for level in asks if _as_float(level.get("price")) == best_ask), 0.0
    )

    outcome.update(
        {
            "bid": round(best_bid, 4),
            "ask": round(best_ask, 4),
            "price": round(midpoint, 4),
            "spread": round(best_ask - best_bid, 4),
            "best_bid_size": round(float(bid_size or 0.0), 4),
            "best_ask_size": round(float(ask_size or 0.0), 4),
            "last_trade_price": _as_float(book.get("last_trade_price")),
            "tick_size": _as_float(book.get("tick_size")),
            "min_order_size": _as_float(book.get("min_order_size")),
            "orderbook_status": "ok",
        }
    )
    return True


def check_market_resolved(market_id: str) -> Optional[bool]:
    """Check if market is resolved. Returns True/False or None."""
    data = get_market(market_id)
    if not data:
        return None

    closed = data.get("closed", False)
    if not closed:
        return None

    try:
        prices = json.loads(data.get("outcomePrices", "[0.5,0.5]"))
        if not isinstance(prices, list) or len(prices) < 1:
            return None
        # outcomePrices = [YES_price, NO_price]
        yes_price = float(prices[0])
        no_price = float(prices[1]) if len(prices) > 1 else 0.5
        if yes_price >= 0.95:
            return True
        elif yes_price <= 0.05:
            return False
    except (json.JSONDecodeError, ValueError, IndexError) as e:
        logger.error(f"Error parsing outcome prices for {market_id}: {e}")
    return None


def parse_temp_range(question: str) -> Optional[Tuple[float, float]]:
    """Parse temperature range from question."""
    if not question:
        return None

    num = r"(-?\d+(?:\.\d+)?)"

    # "X or below"
    if "or below" in question.lower():
        m = re.search(num + r"[°]?[FC] or below", question, re.IGNORECASE)
        if m:
            low = float(m.group(1))
            return (-999.0, low)

    # "X or higher"
    if "or higher" in question.lower():
        m = re.search(num + r"[°]?[FC] or higher", question, re.IGNORECASE)
        if m:
            high = float(m.group(1))
            return (high, 999.0)

    # "between X-Y"
    m = re.search(r"between " + num + r"-" + num + r"[°]?[FC]", question, re.IGNORECASE)
    if m:
        low = float(m.group(1))
        high = float(m.group(2))
        return (low, high)

    # "be X on"
    m = re.search(r"be " + num + r"[°]?[FC] on", question, re.IGNORECASE)
    if m:
        v = float(m.group(1))
        return (v, v)

    return None


def hours_to_resolution(end_date_str: str) -> float:
    """Calculate hours until resolution."""
    try:
        end = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return max(0.0, (end - datetime.now(timezone.utc)).total_seconds() / 3600)
    except (Exception,) as e:
        return 999.0


def get_outcomes(event: Dict) -> List[Dict]:
    """Extract outcomes from event."""
    outcomes = []

    for market in event.get("markets", []):
        question = market.get("question", "")
        mid = str(market.get("id", ""))
        volume = float(market.get("volume", 0))
        rng = parse_temp_range(question)

        if not rng:
            continue

        try:
            prices_str = market.get("outcomePrices", "[0.5,0.5]")
            prices = json.loads(prices_str)
            if not isinstance(prices, list) or len(prices) < 1:
                continue
            token_ids = json.loads(market.get("clobTokenIds", "[]"))
            yes_token_id = str(token_ids[0]) if token_ids else ""
            # Gamma outcomePrices are indicative/display prices. They are kept as
            # fallbacks only; executable bid/ask is loaded from the CLOB orderbook
            # by refresh_outcome_orderbook before any trading decision.
            yes_price = float(prices[0])
            no_price = float(prices[1]) if len(prices) > 1 else 0.5
            ask = yes_price
            bid = yes_price
            spread = 1.0
        except (json.JSONDecodeError, ValueError, IndexError) as e:
            logger.error(f"Skipping market {mid} due to price error: {e}")
            continue

        outcomes.append(
            {
                "question": question,
                "market_id": mid,
                "token_id": yes_token_id,
                "range": rng,
                "yes_price": round(yes_price, 4),
                "no_price": round(no_price, 4),
                "ask": round(ask, 4),
                "bid": round(bid, 4),
                "price": round(yes_price, 4),
                "spread": round(spread, 4),
                "volume": round(volume, 0),
            }
        )

    outcomes.sort(key=lambda x: x["range"][0])
    return outcomes


# Audit: Includes fee and slippage awareness
