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
    min_ev = max(float(getattr(config, "min_ev", MIN_EV)), MIN_EV)
    max_spread = min(float(getattr(config, "max_slippage", MAX_SPREAD)), MAX_SPREAD)
    min_volume = max(float(getattr(config, "min_volume", MIN_VOLUME)), MIN_VOLUME)
    
    # Volume filter (strict)
    volume = float(outcome.get("volume", 0))
    if volume > 0 and volume < min_volume:
        return True
    
    # Price filter
    if float(outcome.get("ask", 1.0)) >= config.max_price:
        return True
    
    # Spread filter (strict)
    spread = float(outcome.get("spread", 0.0))
    if spread > max_spread:
        return True
    
    # EV filter (strict)
    if adjusted_ev < min_ev:
        return True

    confidence = features.get("confidence")
    if confidence is not None and float(confidence) < MIN_CONFIDENCE:
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
