"""
Centralized rate limiter for Polymarket APIs.

Polymarket uses Cloudflare sliding window rate limits.
Features: cache, retry with exponential backoff, circuit breaker.
"""

from __future__ import annotations
import time
import logging
import threading
from typing import Dict, Optional, Callable, Any
from datetime import datetime, timedelta
from functools import wraps

logger = logging.getLogger(__name__)


class RateLimiter:
    """
    Token bucket rate limiter with sliding window tracking.
    Also provides circuit breaker pattern for persistent failures.
    """

    def __init__(self, max_calls: int = 60, window_seconds: int = 60,
                 max_retries: int = 5, backoff_base: float = 0.25):
        self.max_calls = max_calls
        self.window_seconds = window_seconds
        self.max_retries = max_retries
        self.backoff_base = backoff_base

        # Sliding window: list of timestamps
        self._calls: Dict[str, list[float]] = {}
        self._lock = threading.Lock() if False else None  # Simple for now

        # Circuit breaker
        self._failure_counts: Dict[str, int] = {}
        self._circuit_open: Dict[str, float] = {}  # timestamp when circuit opened
        self.circuit_timeout = 60.0  # Seconds before trying again

    def _clean_old_calls(self, key: str, now: float) -> None:
        """Remove calls outside the window."""
        if key not in self._calls:
            return
        cutoff = now - self.window_seconds
        self._calls[key] = [t for t in self._calls[key] if t > cutoff]

    def can_call(self, key: str = "global") -> tuple[bool, float]:
        """
        Check if a call can be made.
        Returns (allowed, retry_after_seconds).
        """
        now = time.time()
        if key not in self._calls:
            self._calls[key] = []

        self._clean_old_calls(key, now)

        # Check circuit breaker
        if key in self._circuit_open:
            if now - self._circuit_open[key] < self.circuit_timeout:
                return False, self.circuit_timeout - (now - self._circuit_open[key])
            else:
                del self._circuit_open[key]
                self._failure_counts[key] = 0

        if len(self._calls[key]) >= self.max_calls:
            retry_after = self._calls[key][0] + self.window_seconds - now
            return False, max(retry_after, 0.0)

        return True, 0.0

    def record_call(self, key: str = "global") -> None:
        """Record a successful call."""
        now = time.time()
        if key not in self._calls:
            self._calls[key] = []
        self._calls[key].append(now)
        # Reset failure count on success
        self._failure_counts[key] = 0

    def record_failure(self, key: str = "global") -> None:
        """Record a failure. May open circuit breaker."""
        self._failure_counts[key] = self._failure_counts.get(key, 0) + 1
        if self._failure_counts[key] >= 5:  # Open circuit after 5 failures
            self._circuit_open[key] = time.time()
            logger.warning(f"Circuit breaker opened for {key}")

    def get_wait_time(self, key: str = "global") -> float:
        """Get time to wait before next call is allowed."""
        allowed, retry_after = self.can_call(key)
        return 0.0 if allowed else retry_after


# Global rate limiter instance
_global_limiter: Optional[RateLimiter] = None


def get_rate_limiter() -> RateLimiter:
    """Get or create the global rate limiter."""
    global _global_limiter
    if _global_limiter is None:
        _global_limiter = RateLimiter()
    return _global_limiter


def rate_limited(
    key: str = "global",
    max_calls: int = 60,
    window_seconds: int = 60,
    max_retries: int = 5,
):
    """
    Decorator for rate-limited functions.
    Uses sliding window and exponential backoff.
    """
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            limiter = get_rate_limiter()
            for attempt in range(max_retries + 1):
                allowed, wait_time = limiter.can_call(key)
                if not allowed:
                    logger.warning(
                        f"Rate limited on {key}. Waiting {wait_time:.1f}s "
                        f"(attempt {attempt + 1}/{max_retries + 1})"
                    )
                    time.sleep(wait_time + 0.01)
                    continue

                try:
                    result = func(*args, **kwargs)
                    limiter.record_call(key)
                    return result
                except Exception as e:
                    limiter.record_failure(key)
                    if attempt < max_retries:
                        backoff = (2 ** attempt) * 0.25
                        logger.warning(
                            f"Call failed: {e}. Retrying in {backoff:.1f}s..."
                        )
                        time.sleep(backoff)
                    else:
                        raise
            return None
        return wrapper
    return decorator


class RequestThrottler:
    """
    Throttler for requests library.
    Tracks order rate for live trading (max_orders_per_minute).
    """

    def __init__(self, max_per_minute: int = 10):
        self.max_per_minute = max_per_minute
        self._order_timestamps: list[float] = []

    def wait_if_needed(self) -> None:
        """Block until we can place another order."""
        now = time.time()
        # Remove timestamps older than 60 seconds
        cutoff = now - 60.0
        self._order_timestamps = [t for t in self._order_timestamps if t > cutoff]

        if len(self._order_timestamps) >= self.max_per_minute:
            wait_time = self._order_timestamps[0] + 60.0 - now
            if wait_time > 0:
                logger.info(
                    f"Order rate limit reached ({self.max_per_minute}/min). "
                    f"Waiting {wait_time:.1f}s..."
                )
                time.sleep(wait_time)

    def record_order(self) -> None:
        """Record an order placement."""
        self._order_timestamps.append(time.time())
