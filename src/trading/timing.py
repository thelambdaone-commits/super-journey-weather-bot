"""
Trading timing - Align scans with weather model runs.

Weather models update at specific times (00z, 06z, 12z, 18z).
The market is slow to adjust - we strike 30min after each run
to exploit the pricing inefficiency window.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional


MODEL_RUN_HOURS = (0, 6, 12, 18)
SCAN_DELAY_MINUTES = 30


def _scan_times_for_day(day: datetime) -> list[datetime]:
    return [
        day.replace(hour=run_hour, minute=0, second=0, microsecond=0)
        + timedelta(minutes=SCAN_DELAY_MINUTES)
        for run_hour in MODEL_RUN_HOURS
    ]


def get_next_model_run(now: datetime | None = None) -> datetime:
    """
    Returns the next weather model run time.
    
    Weather models run at:
    - 00z (midnight UTC)
    - 06z (6am UTC)
    - 12z (noon UTC)  
    - 18z (6pm UTC)
    
    We scan 30 minutes after each run to:
    1. Allow forecasts to be published
    2. Exploit the market's slow adjustment
    """
    now = now or datetime.now(timezone.utc)
    for scan_time in _scan_times_for_day(now):
        if scan_time > now:
            return scan_time
    
    # Next day 00:30
    tomorrow = now + timedelta(days=1)
    return tomorrow.replace(hour=0, minute=SCAN_DELAY_MINUTES, second=0, microsecond=0)


def get_latest_model_run(now: datetime | None = None) -> datetime:
    """Return the latest scan time at or before ``now``."""
    now = now or datetime.now(timezone.utc)
    candidates = [scan_time for scan_time in _scan_times_for_day(now) if scan_time <= now]
    if candidates:
        return candidates[-1]
    yesterday = now - timedelta(days=1)
    return _scan_times_for_day(yesterday)[-1]


def should_scan_now(last_scan: Optional[datetime] = None, 
                    min_interval_minutes: int = 180,
                    now: datetime | None = None) -> bool:
    """
    Determine if we should scan now.
    
    Args:
        last_scan: Last scan time (None = never scanned)
        min_interval_minutes: Minimum minutes between scans (default 180 = 3h)
    
    Returns:
        True if we should scan now
    """
    now = now or datetime.now(timezone.utc)
    if last_scan is None:
        return True
    
    latest_scan = get_latest_model_run(now)
    model_window_end = latest_scan + timedelta(minutes=30)
    if latest_scan <= now <= model_window_end and last_scan < latest_scan:
        return True
    
    # Or if minimum interval has passed
    time_since_last = now - last_scan
    return time_since_last.total_seconds() >= (min_interval_minutes * 60)


def get_opportunity_window(now: datetime | None = None) -> tuple[datetime, datetime]:
    """
    Returns (window_start, window_end) for the current opportunity window.
    
    The window is 2 hours after each model run (30min to 2.5h after run).
    After that, the market has typically adjusted.
    """
    now = now or datetime.now(timezone.utc)
    latest_scan = get_latest_model_run(now)
    if latest_scan <= now <= latest_scan + timedelta(hours=2):
        window_start = latest_scan
    else:
        window_start = get_next_model_run(now)
    window_end = window_start + timedelta(hours=2)
    return window_start, window_end


def is_in_opportunity_window(now: datetime | None = None) -> bool:
    """Check if we're currently in the opportunity window."""
    now = now or datetime.now(timezone.utc)
    window_start, window_end = get_opportunity_window(now)
    return window_start <= now <= window_end


def format_timing_report() -> str:
    """Format a timing report for diagnostics."""
    now = datetime.now(timezone.utc)
    next_run = get_next_model_run()
    window_start, window_end = get_opportunity_window()
    
    lines = [
        f"\n{'=' * 50}",
        "TIMING REPORT",
        f"{'=' * 50}",
        f"Current UTC time: {now.strftime('%Y-%m-%d %H:%M:%S')}",
        f"Next model run scan: {next_run.strftime('%Y-%m-%d %H:%M:%S')} UTC",
        f"Opportunity window: {window_start.strftime('%H:%M')} - {window_end.strftime('%H:%M')} UTC",
        f"In window now: {'YES' if is_in_opportunity_window() else 'NO'}",
        f"{'=' * 50}\n",
    ]
    return "\n".join(lines)
