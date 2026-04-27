"""
Feature flags management for safe deployments and fallbacks.
"""
import os
import logging

logger = logging.getLogger(__name__)

def is_enabled(flag_name: str, default: bool = False) -> bool:
    """Check if a feature flag is enabled in .env."""
    env_key = f"ENABLE_{flag_name.upper()}"
    val = os.environ.get(env_key)
    if val is None:
        return default
    return val.lower() in ("true", "1", "yes", "on")

def safe_execute(flag_name: str, func, *args, **kwargs):
    """
    Execute a function only if its feature flag is enabled.
    Returns the result or None if disabled/failed.
    """
    if not is_enabled(flag_name):
        return None
        
    try:
        return func(*args, **kwargs)
    except (Exception,) as e:
        logger.error(f"[FEATURE-FAIL] {flag_name}: {e}")
        return None
