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
    
    # 1. Anti-Crossed Book Guard (Crucial for realistic Paper Trading)
    bid = float(outcome.get("bid", 0.0))
    ask = float(outcome.get("ask", 1.0))
    if ask <= bid or ask <= 0.001:
        return True

    # 2. Volume filter (strict)
    volume = float(outcome.get("volume", 0))
    if volume > 0 and volume < min_volume:
        return True
    
    # 3. Price filter
    if ask >= config.max_price:
        return True
    
    # 4. Spread filter (strict)
    spread = float(outcome.get("spread", 0.0))
    if spread > max_spread:
        return True
    
    # 5. EV filter (strict)
    if adjusted_ev < min_ev:
        return True

    # 6. Confidence filter
    confidence = features.get("confidence")
    if confidence is not None and float(confidence) < MIN_CONFIDENCE:
        return True
    
    # 7. Volatility / Bucket Width Filter (Phase 1.2)
    # Don't bet if the bucket is too narrow relative to the forecast uncertainty
    sigma = features.get("sigma")
    t_low, t_high = outcome.get("range", (0, 0))
    if sigma is not None and t_low is not None and t_high is not None:
        bucket_width = t_high - t_low
        # If bucket is just a single degree, require sigma to be low
        if bucket_width <= 1.0 and sigma > 1.5:
            return True

    # 8. Source Contradiction Filter (Phase 1.2)
    # If GFS and ECMWF disagree significantly, avoid the market
    ecmwf = features.get("ecmwf_max")
    gfs = features.get("gfs_max")
    if ecmwf is not None and gfs is not None:
        diff = abs(float(ecmwf) - float(gfs))
        if diff > 5.0: # 5 degree threshold for high-uncertainty
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
