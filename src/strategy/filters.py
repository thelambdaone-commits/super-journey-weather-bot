"""
Strategy filters with stricter thresholds.
"""

# Stricter thresholds for live trading
MIN_EV = 0.06  # 6% minimum EV
MAX_SPREAD = 0.03  # 3% maximum spread
MIN_VOLUME = 5000  # $5000 minimum volume
MIN_CONFIDENCE = 0.30  # 30% minimum confidence


def should_skip_outcome(config, outcome: dict, features: dict, adjusted_ev: float) -> bool:
    """Return True when the candidate market should be skipped."""
    
    # Volume filter (strict)
    volume = float(outcome.get("volume", 0))
    if volume > 0 and volume < MIN_VOLUME:
        return True
    
    # Price filter
    if float(outcome.get("ask", 1.0)) >= config.max_price:
        return True
    
    # Spread filter (strict)
    spread = float(outcome.get("spread", 0.0))
    if spread > MAX_SPREAD:
        return True
    
    # EV filter (strict)
    if adjusted_ev < MIN_EV:
        return True
    
    return False


def get_validation_report() -> dict:
    """Get current validation thresholds."""
    return {
        "min_ev": MIN_EV,
        "max_spread": MAX_SPREAD,
        "min_volume": MIN_VOLUME,
        "min_confidence": MIN_CONFIDENCE,
    }