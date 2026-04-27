"""
Notification Formatter - Bridge to Premium V2 Telegram Templates.
"""
from __future__ import annotations
from . import telegram_control_center as tg

def format_weather_signal(payload: dict) -> str:
    """Wrapper for legacy calls."""
    return f"🚀 SIGNAL: {payload.get('city')} {payload.get('market_name')} | Edge: {payload.get('edge'):+.1f}%"

def format_signal_for_telegram(signal_dict: dict) -> bool:
    """
    Bridge from scanner/engine to the Premium Signal template.
    """
    try:
        tg.send_signal(
            city=signal_dict.get("city", "Unknown"),
            market=f"{'ABOVE' if signal_dict.get('side') == 'ABOVE' else 'BELOW'} {signal_dict.get('threshold')}°C",
            fair_value=signal_dict.get("p", 0.5),
            market_odds=signal_dict.get("entry_price", 0.5),
            edge=signal_dict.get("ev", 0.0),
            confidence=signal_dict.get("ml", {}).get("tier", "LOW"),
            size_pct=signal_dict.get("kelly", 0.0) * 100,
            reason=signal_dict.get("reasons", None)
        )
        return True
    except (Exception):
        return False

def format_daily_recap(stats: dict) -> bool:
    """Bridge to Daily Recap."""
    return tg.send_daily_report(stats)

def format_weekly_recap(stats: dict) -> bool:
    """Bridge to Weekly Recap."""
    return tg.send_weekly_report(stats)

def format_trust_update(trade_result: dict) -> bool:
    """Bridge to Trust Update."""
    return tg.send_trust_update(
        city=trade_result.get("city", "Unknown"),
        market=trade_result.get("market", "Unknown"),
        result=trade_result.get("result", "LOST"),
        pnl=trade_result.get("pnl_pct", 0.0)
    )
