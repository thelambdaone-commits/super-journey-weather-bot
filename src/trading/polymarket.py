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
        yes_price = float(prices[0])
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
            bid = float(prices[0])
            ask = float(prices[1]) if len(prices) > 1 else bid
        except (json.JSONDecodeError, ValueError, IndexError) as e:
            logger.error(f"Skipping market {mid} due to price error: {e}")
            continue
        
        outcomes.append({
            "question": question,
            "market_id": mid,
            "range": rng,
            "bid": round(bid, 4),
            "ask": round(ask, 4),
            "price": round(bid, 4),
            "spread": round(ask - bid, 4),
            "volume": round(volume, 0),
        })
    
    outcomes.sort(key=lambda x: x["range"][0])
    return outcomes