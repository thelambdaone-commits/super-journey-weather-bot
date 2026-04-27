"""
Polymarket API client.
"""
import json
import re
import time
import requests
import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CLOB_HOST = "https://clob.polymarket.com"


def get_polymarket_event(city_slug: str, month: str, day: int, year: int) -> Optional[Dict]:
    """Get Polymarket event for city/date."""
    slug = f"highest-temperature-in-{city_slug}-on-{month}-{day}-{year}"
    try:
        r = requests.get(
            f"https://gamma-api.polymarket.com/events?slug={slug}",
            timeout=(5, 8)
        )
        data = r.json()
        if data and isinstance(data, list) and len(data) > 0:
            return data[0]
    except Exception as e:
        logger.error(f"Error fetching Polymarket event for {city_slug}: {e}")
    return None


def get_market(market_id: str) -> Optional[Dict]:
    """Get market data."""
    try:
        r = requests.get(
            f"https://gamma-api.polymarket.com/markets/{market_id}",
            timeout=(3, 5)
        )
        return r.json()
    except Exception as e:
        logger.error(f"Error fetching Polymarket market {market_id}: {e}")
        return None


def _as_float(value, default: float | None = None) -> float | None:
    """Parse numeric API fields without letting malformed values leak into pricing."""
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def get_orderbook(token_id: str) -> Optional[Dict]:
    """Fetch the CLOB orderbook for one outcome token."""
    if not token_id:
        return None
    try:
        response = requests.get(
            f"{CLOB_HOST}/book",
            params={"token_id": str(token_id)},
            timeout=(3, 5),
        )
        if not response.ok:
            logger.warning("CLOB book unavailable for token %s: HTTP %s", token_id, response.status_code)
            return None
        data = response.json()
        if not isinstance(data, dict):
            return None
        return data
    except Exception as exc:
        logger.warning("Error fetching CLOB book for token %s: %s", token_id, exc)
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
    
    for price_str, size_str in levels:
        price = float(price_str)
        size_shares = float(size_str)
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
        return float(levels[0][0]) if levels else 0.5
        
    # Return average price per share
    vwap = accum_cost / accum_shares
    
    # If we couldn't fill the whole size, we add a penalty
    if remaining_usd > 0:
        vwap *= 1.1 
        
    return round(vwap, 4)

def refresh_outcome_orderbook(event_id: str, outcome_name: str) -> dict:
    """
    Replace indicative Gamma prices with executable CLOB bid/ask.

    Gamma's outcomePrices are display/implied prices, not an executable spread.
    Trading decisions must use the YES token orderbook: best ask to buy, best bid
    to mark/exit, and ask-bid spread for liquidity/friction filters.
    """
    book = get_orderbook(str(outcome.get("token_id") or ""))
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
    bid_size = next((_as_float(level.get("size"), 0.0) for level in bids if _as_float(level.get("price")) == best_bid), 0.0)
    ask_size = next((_as_float(level.get("size"), 0.0) for level in asks if _as_float(level.get("price")) == best_ask), 0.0)

    outcome.update({
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
    })
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
    
    num = r'(-?\d+(?:\.\d+)?)'
    
    # "X or below"
    if "or below" in question.lower():
        m = re.search(num + r'[°]?[FC] or below', question, re.IGNORECASE)
        if m:
            low = float(m.group(1))
            return (-999.0, low)
    
    # "X or higher"
    if "or higher" in question.lower():
        m = re.search(num + r'[°]?[FC] or higher', question, re.IGNORECASE)
        if m:
            high = float(m.group(1))
            return (high, 999.0)
    
    # "between X-Y"
    m = re.search(r'between ' + num + r'-' + num + r'[°]?[FC]', question, re.IGNORECASE)
    if m:
        low = float(m.group(1))
        high = float(m.group(2))
        return (low, high)

    # "be X on"
    m = re.search(r'be ' + num + r'[°]?[FC] on', question, re.IGNORECASE)
    if m:
        v = float(m.group(1))
        return (v, v)
    
    return None


def hours_to_resolution(end_date_str: str) -> float:
    """Calculate hours until resolution."""
    try:
        end = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
        return max(0.0, (end - datetime.now(timezone.utc)).total_seconds() / 3600)
    except Exception:
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
        
        outcomes.append({
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
        })
    
    outcomes.sort(key=lambda x: x["range"][0])
    return outcomes
