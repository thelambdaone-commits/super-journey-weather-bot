"""
Health and status reporting for trading engine.
"""
from __future__ import annotations
import requests
from .types import EngineFeedback
from ..ai import get_groq_client

def probe_http_api(name: str, url: str, timeout: tuple[int, int] = (4, 6)) -> tuple[str, str, float]:
    """Probe a public HTTP API and return status and latency."""
    import time
    start = time.time()
    try:
        response = requests.get(url, timeout=timeout)
        latency = (time.time() - start) * 1000
        return name, "connected" if response.ok else f"http_{response.status_code}", latency
    except Exception:
        return name, "unreachable", 0.0

def get_api_statuses(config, feedback: EngineFeedback) -> list[tuple[str, str, float]]:
    """Build startup API connection statuses with latency."""
    statuses = []
    tg_ok = "connected" if feedback.verify_notifications() else "error"
    statuses.append(("telegram", tg_ok, 0.0))
    statuses.append(("groq", "connected" if get_groq_client() is not None else "missing", 0.0))
    statuses.append(probe_http_api("polymarket", "https://gamma-api.polymarket.com/events?limit=1"))
    statuses.append(probe_http_api("open_meteo", "https://api.open-meteo.com/v1/forecast?latitude=40.7&longitude=-73.8&daily=temperature_2m_max&forecast_days=1"))
    return statuses

def render_api_statuses(api_statuses: list[tuple[str, str, float]]) -> str:
    """Render API statuses with emojis and latency for Telegram."""
    lines = []
    for name, status, lat in api_statuses:
        emoji = "🟢" if status == "connected" else "🔴"
        # Format name: open_meteo -> Open Meteo
        display_name = name.replace("_", " ").title()
        latency_str = f" ({lat:.0f}ms)" if lat > 0 else ""
        lines.append(f"{emoji} *{display_name}*: `{status}`{latency_str}")
    return "\n".join(lines)
