"""Shared Open-Meteo HTTP throttling."""
from __future__ import annotations

import logging
import threading
import time
from typing import Any

import requests

logger = logging.getLogger(__name__)

MIN_INTERVAL_SECONDS = 0.25  # 4 req/sec = 240/min (API allows 600/min, conservative margin)
MAX_429_RETRIES = 2
DEFAULT_429_BACKOFF_SECONDS = 60.0

_lock = threading.Lock()
_last_request_at = 0.0
_cooldown_until = 0.0


def _retry_after_seconds(response: requests.Response) -> float:
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            return max(float(retry_after), MIN_INTERVAL_SECONDS)
        except ValueError:
            pass
    return DEFAULT_429_BACKOFF_SECONDS


def _cooldown_response(remaining_s: float) -> requests.Response:
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = str(max(1, int(remaining_s)))
    response.headers["Content-Type"] = "application/json"
    response._content = b'{"error":"open_meteo_rate_limited"}'
    response.url = "open-meteo://cooldown"
    return response


def rate_limited_get(url: str, **kwargs: Any) -> requests.Response:
    """Run a GET request through the global Open-Meteo throttle."""
    global _cooldown_until, _last_request_at
    max_429_retries = int(kwargs.pop("max_429_retries", MAX_429_RETRIES))

    for attempt in range(max_429_retries + 1):
        with _lock:
            now = time.monotonic()
            if now < _cooldown_until:
                remaining_s = _cooldown_until - now
                logger.warning("Open-Meteo cooldown active; skipping request for %.1fs", remaining_s)
                return _cooldown_response(remaining_s)

            elapsed = now - _last_request_at
            wait_s = MIN_INTERVAL_SECONDS - elapsed
            if wait_s > 0:
                time.sleep(wait_s)

            _last_request_at = time.monotonic()
            response = requests.get(url, **kwargs)

            if response.status_code != 429:
                return response

            backoff_s = _retry_after_seconds(response)
            if attempt >= max_429_retries:
                _cooldown_until = time.monotonic() + backoff_s
                logger.warning("Open-Meteo 429 retry limit reached; cooling down for %.1fs", backoff_s)
                return response

            logger.warning("Open-Meteo returned 429; backing off for %.1fs", backoff_s)
            time.sleep(backoff_s)

    return response
