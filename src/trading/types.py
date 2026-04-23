"""
Trading engine types and protocols.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Protocol, Optional

@dataclass
class RuntimeModes:
    """Effective runtime flags."""
    paper_mode: bool
    live_trade: bool
    signal_mode: bool
    tui_mode: bool

@dataclass
class ScanResult:
    """Single scan summary."""
    new_trades: int = 0
    closed: int = 0
    resolved: int = 0

class EngineFeedback(Protocol):
    """Decoupled feedback channel for engine events."""
    def emit(self, message: str) -> None: ...
    def notify_started(self, mode: str, cities: int, scan_minutes: int) -> None: ...
    def notify_stopped(self, reason: str) -> None: ...
    def notify_health(self, message: str) -> None: ...
    def notify_trade_open(self, city: str, date_str: str, bucket: str, price: float, 
                        ev: float, cost: float, source: str, note: str = "") -> None: ...
    def notify_signal(self, city: str, date_str: str, bucket: str, price: float, 
                    ev: float, cost: float, source: str, horizon: str, 
                    question: str, market_id: str, note: str = "", 
                    calibrated_prob: float | None = None, market_prob: float | None = None,
                    uncertainty: float | None = None, signal_type: str | None = None,
                    quality: float | None = None, priority: str | None = None,
                    emoji: str | None = None, confidence_score: float | None = None,
                    source_bias: float | None = None, trade_context: dict | None = None) -> None: ...
    def notify_trade_win(self, city: str, date_str: str, bucket: str, pnl: float, 
                       temp: str, balance: float) -> None: ...
    def notify_trade_loss(self, city: str, date_str: str, bucket: str, pnl: float, 
                        balance: float) -> None: ...
    def notify_hourly_report(self, summary: dict, city_signals: list[dict]) -> None: ...
    def notify_gem_alert(self, signal: dict) -> None: ...
    def verify_notifications(self) -> bool: ...

class NullFeedback:
    """Safe no-op feedback implementation."""
    def emit(self, message: str) -> None: print(message)
    def verify_notifications(self) -> bool: return True
    def notify_started(self, mode: str, cities: int, scan_minutes: int) -> None: pass
    def notify_stopped(self, reason: str) -> None: pass
    def notify_health(self, message: str) -> None: pass
    def notify_trade_open(self, *args, **kwargs) -> None: pass
    def notify_signal(self, *args, **kwargs) -> None: pass
    def notify_trade_win(self, *args, **kwargs) -> None: pass
    def notify_trade_loss(self, *args, **kwargs) -> None: pass
    def notify_hourly_report(self, *args, **kwargs) -> None: pass
    def notify_gem_alert(self, *args, **kwargs) -> None: pass
