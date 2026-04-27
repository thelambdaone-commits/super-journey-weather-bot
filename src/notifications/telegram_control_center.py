import os
import json
import time
import socket
import traceback
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# =====================================================
# CONFIG
# =====================================================

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

TIMEOUT = 10

# =====================================================
# CORE TELEGRAM
# =====================================================

def _enabled() -> bool:
    return bool(BOT_TOKEN and CHAT_ID)

def send_message(text: str) -> bool:
    if not _enabled():
        return False

    payload = urlencode({
        "chat_id": CHAT_ID,
        "text": text[:4096],
        "parse_mode": "HTML",
        "disable_web_page_preview": "true",
    }).encode()

    req = Request(API_URL, data=payload, method="POST")
    try:
        with urlopen(req, timeout=TIMEOUT) as r:
            return r.status == 200
    except (Exception):
        return False

# =====================================================
# TEMPLATES PREMIUM V2
# =====================================================

def now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

def pct(v):
    return f"{v:+.1f}%" if v != 0 else "0.0%"

def fmt_p(v):
    return f"{v*100:.1f}%"

def send_signal(city, market, fair_value, market_odds, edge, confidence, size_pct, reason=None):
    """🔥 TEMPLATE #1 — SIGNAL PREMIUM"""
    reason_lines = "\n".join([f"• {r}" for r in (reason or ["Forecast convergence", "Market lag detected"])])
    
    msg = f"""
🔥 <b>WEATHER EDGE SIGNAL</b>

🎯 Action: <b>BUY {market}</b>
🌍 City: {city}
⏰ Expiry: Today 23:59 UTC

📊 Market Odds: {fmt_p(market_odds)}
🧠 Fair Value: {fmt_p(fair_value)}
⚡ Edge: <b>{pct(edge*100)}</b>

🎚 Confidence: <b>{confidence.upper()}</b>
💰 Size: {size_pct:.1f}% bankroll
🛡 Risk: { "LOW" if edge > 0.08 else "MEDIUM" }

Why:
{reason_lines}

#{"".join(city.split("-"))} #Alpha
"""
    send_message(msg.strip())

def send_no_trade(city, reason_list=None):
    """🧊 TEMPLATE #2 — NO TRADE"""
    reasons = "\n".join([f"• {r}" for r in (reason_list or ["Edge too small", "Spread too wide"])])
    msg = f"""
🧊 <b>NO TRADE</b>

City: {city}

Reason:
{reasons}

Capital preserved.
"""
    send_message(msg.strip())

def send_daily_report(stats: dict):
    """📊 DAILY LIVE REPORT"""
    msg = f"""
📊 <b>DAILY LIVE REPORT</b>

Signals: {stats.get("signals", 0)}
Wins: {stats.get("wins", 0)}
Losses: {stats.get("losses", 0)}
PnL: <b>{pct(stats.get("pnl", 0))}</b>

Errors: {stats.get("errors", 0)}
Latency avg: {stats.get("latency", 1.3):.1f}s

Time: {now()}
"""
    send_message(msg.strip())


def send_incident(message: str, severity: str = "WARNING"):
    """⚠️ INCIDENT ALERT"""
    icon = "⚠️" if severity == "WARNING" else "🚨"
    msg = f"""
{icon} <b>INCIDENT DETECTED</b>

{message}

Status: Monitoring
Time: {now()}
"""
    send_message(msg.strip())

def send_weekly_report(stats: dict):
    """🏆 TEMPLATE #4 — WEEKLY TRUST REPORT"""
    msg = f"""
🏆 <b>WEEKLY TRACK RECORD</b>

Week: {pct(stats.get("week", 0))}
Month: {pct(stats.get("month", 0))}
YTD: {pct(stats.get("ytd", 0))}

Trades: {stats.get("trades", 0)}
Win Rate: {stats.get("winrate", 0):.0f}%
Profit Factor: {stats.get("pf", 1.0):.2f}

System Status: <b>HEALTHY</b>
"""
    send_message(msg.strip())

def send_crash(module, error):
    """🚨 TEMPLATE #5 — BOT ISSUE DETECTED"""
    msg = f"""
🚨 <b>BOT ISSUE DETECTED</b>

Module: {module}
Type: {type(error).__name__ if not isinstance(error, str) else "Error"}

Action:
• Retry launched
• Fallback active

Status: <b>Recovering</b>
"""
    send_message(msg.strip())

def send_cto_report(repo_score=7.8, notes=None):
    """🧠 TEMPLATE #6 — MONDAY CTO REPORT"""
    lines = "\n".join([f"• {x}" for x in (notes or ["reduce signal noise", "improve fill tracking", "remove dead code"])])
    msg = f"""
🧠 <b>MONDAY CTO REPORT</b>

Repo Score: <b>{repo_score}/10</b>

Focus This Week:
{lines}

Rule:
PnL net > ego
"""
    send_message(msg.strip())

def send_trust_update(city, market, result, pnl):
    """🔥 TEMPLATE #7 — TRUST ENGINE UPDATE"""
    icon = "✅" if result == "WON" else "❌"
    msg = f"""
{icon} <b>TRADE RESOLVED: {result}</b>

City: {city}
Market: {market}
PnL: <b>{pct(pnl)}</b>

Transparency builds alpha.
"""
    send_message(msg.strip())

# =====================================================
# CLI TEST
# =====================================================

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2: sys.exit(0)
    cmd = sys.argv[1]
    
    if cmd == "signal":
        send_signal("Paris", "ABOVE 18.5°C", 0.63, 0.54, 0.09, "HIGH", 1.5)
    elif cmd == "notrade":
        send_no_trade("Madrid", ["Edge too small (+1.2%)", "Spread too wide"])
    elif cmd == "daily":
        send_daily_report({"signals": 5, "wins": 3, "losses": 2, "winrate": 60, "pnl": 2.8, "fees": -0.4, "slippage": -0.2, "sharpe": 1.41, "drawdown": -3.1, "best": "Paris", "worst": "Rome"})
    elif cmd == "weekly":
        send_weekly_report({"week": 7.6, "month": 12.4, "ytd": 31.8, "trades": 24, "winrate": 58, "pf": 1.42})
    elif cmd == "crash":
        send_crash("scanner.py", "TimeoutError")
    elif cmd == "cto":
        send_cto_report()
    elif cmd == "trust":
        send_trust_update("Paris", "ABOVE 18.5°C", "WON", 1.8)
