#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
dashboard.py — TUI Dashboard for WeatherBot
======================================
Terminal UI using Textual.

Usage:
    python dashboard.py          # run dashboard
    python dashboard.py --paper  # paper mode
"""

import sys
from dataclasses import asdict
from datetime import datetime
from pathlib import Path

from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widgets import Header, Footer, DataTable, Static, Log
from textual import work

sys.path.insert(0, str(Path(__file__).parent))

PAPER_MODE = "--paper" in sys.argv or "-p" in sys.argv

from src.weather.config import get_config
if not get_config().dashboard_enabled:
    print("Dashboard is disabled in configuration.")
    sys.exit(0)

from src.storage import get_storage
from src.weather.locations import LOCATIONS


def load_state() -> dict:
    """Load current state as a plain dict."""
    return asdict(get_storage().load_state())


def load_all_markets() -> list[dict]:
    """Load markets as plain dicts for the dashboard."""
    return [asdict(market) for market in get_storage().load_all_markets()]


def load_cal() -> dict:
    """Load stored calibration data."""
    return get_storage().load_calibration()


def get_dashboard_data() -> dict:
    """Build dashboard stats from persisted state."""
    state = load_state()
    markets = load_all_markets()
    open_pos = [m for m in markets if m.get("position") and m["position"].get("status") == "open"]
    resolved = [m for m in markets if m.get("status") == "resolved" and m.get("pnl") is not None]

    balance = state.get("balance", 0)
    start = state.get("starting_balance", balance)
    ret_pct = (balance - start) / start * 100 if start else 0
    wins = state.get("wins", 0)
    losses = state.get("losses", 0)
    total = wins + losses

    market_rows = []
    for market in markets:
        pos = market.get("position")
        current_price = None
        if pos:
            mid = pos.get("market_id")
            for outcome in market.get("all_outcomes", []):
                if outcome.get("market_id") == mid:
                    current_price = outcome.get("price", outcome.get("bid"))
                    break

        market_rows.append({
            "city": market.get("city_name") or LOCATIONS.get(market.get("city", ""), market.get("city", "")).name,
            "date": market.get("date"),
            "unit": market.get("unit"),
            "status": market.get("status", "open"),
            "position": {
                "bucket_low": pos.get("bucket_low") if pos else None,
                "bucket_high": pos.get("bucket_high") if pos else None,
                "entry_price": pos.get("entry_price") if pos else None,
                "price": current_price,
            } if pos else None,
            "pnl": market.get("pnl"),
            "resolved_outcome": market.get("resolved_outcome"),
        })

    return {
        "balance": balance,
        "starting_balance": start,
        "return_pct": round(ret_pct, 2),
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / total * 100, 0) if total else 0,
        "total_trades": total,
        "open_positions": len(open_pos),
        "resolved_count": len(resolved),
        "paper_mode": PAPER_MODE,
        "markets": market_rows,
    }


_cal = load_cal()


class WeatherDashboard(App):
    """TUI Dashboard for WeatherBot."""

    CSS = """
    Screen {
        background: $surface;
    }
    #header-bar {
        height: 3;
        background: $primary;
        color: $text;
    }
    #stats {
        height: 3;
        background: $surface-darken-1;
        dock: top;
    }
    #table-container {
        height: 1fr;
        margin: 1 0;
    }
    DataTable {
        height: 100%;
        margin: 0 1;
    }
    #log-container {
        height: 8;
        border-top: $primary;
        border: solid $primary;
        margin: 1 1 0 1;
    }
    .stat-label {
        width: 16;
        color: $text-muted;
    }
    .stat-value {
        width: 12;
        color: $text;
    }
    .positive { color: $success; }
    .negative { color: $error; }
    """

    BINDINGS = [
        ("q", "quit", "Quit"),
        ("r", "refresh", "Refresh"),
        ("p", "toggle_paper", "Paper Mode"),
    ]

    def __init__(self):
        super().__init__()

    def compose(self) -> ComposeResult:
        yield Header()

        with Vertical():
            with Horizontal(id="stats"):
                yield Static("Balance:", classes="stat-label")
                yield Static("$0", id="balance")
                yield Static("P&L:", classes="stat-label")
                yield Static("$0", id="pnl")
                yield Static("Win Rate:", classes="stat-label")
                yield Static("0%", id="winrate")
                yield Static("Open:", classes="stat-label")
                yield Static("0", id="open")
                yield Static("Trades:", classes="stat-label")
                yield Static("0", id="trades")
                yield Static("(PAPER)", id="paper-mode") if PAPER_MODE else Static("")

            with Vertical(id="table-container"):
                yield DataTable(id="markets-table")

            with Vertical(id="log-container"):
                yield Log(id="log")

        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#markets-table", DataTable)
        table.add_columns("City", "Date", "Bucket", "In", "Out", "P&L", "Status")

        self.refresh_data()
        self.set_interval(30, self.refresh_data)

    @work(exclusive=True, thread=True)
    def refresh_data(self) -> None:
        try:
            data = get_dashboard_data()
            state = load_state()

            balance = data.get("balance", 0)
            start = data.get("starting_balance", 0)
            pnl = balance - start
            ret_pct = data.get("return_pct", 0)
            win_rate = data.get("win_rate", 0)
            open_count = data.get("open_positions", 0)
            total = data.get("total_trades", 0)

            self.query_one("#balance", Static).update(f"${balance:,.0f}")

            pnl_str = f"{'+'if pnl>=0 else ''}{pnl:,.0f} ({ret_pct:+.1f}%)"
            pnl_widget = self.query_one("#pnl", Static)
            pnl_widget.update(pnl_str)
            pnl_widget.add_class("positive" if pnl >= 0 else "negative")

            wr_str = f"{win_rate:.0f}%"
            wr_widget = self.query_one("#winrate", Static)
            wr_widget.update(wr_str)
            wr_widget.add_class("positive" if win_rate >= 50 else "negative")

            self.query_one("#open", Static).update(str(open_count))
            self.query_one("#trades", Static).update(str(total))

            table = self.query_one("#markets-table", DataTable)
            table.clear()

            markets = data.get("markets", [])
            for m in markets:
                city = m.get("city", "")
                date = m.get("date", "")
                unit = m.get("unit", "C")
                unit_sym = "F" if unit == "F" else "C"
                pos = m.get("position")
                status = m.get("status", "open")
                pnl = m.get("pnl")

                if pos:
                    bucket = f"{pos.get('bucket_low')}-{pos.get('bucket_high')}{unit_sym}"
                    entry = f"${pos.get('entry_price', 0):.2f}"
                    current = f"${pos.get('price', 0):.2f}"
                else:
                    bucket = "-"
                    entry = "-"
                    current = "-"

                if pnl is not None:
                    pnl_str = f"{'+'if pnl>=0 else ''}{pnl:.2f}"
                    if pnl >= 0:
                        pnl_str = f"[green]{pnl_str}[/]"
                    else:
                        pnl_str = f"[red]{pnl_str}[/]"
                else:
                    pnl_str = "-" if status == "open" else "$0"

                status_icon = "●" if status == "open" else "○"
                table.add_row(city, date, bucket, entry, current, pnl_str, status_icon)

            log = self.query_one("#log", Log)
            log.write_line(f"[{datetime.now().strftime('%H:%M:%S')}] Dashboard refreshed")

        except Exception as e:
            log = self.query_one("#log", Log)
            log.write_line(f"[ERROR] {e}")

    def action_refresh(self) -> None:
        self.refresh_data()

    def action_toggle_paper(self) -> None:
        global PAPER_MODE
        PAPER_MODE = not PAPER_MODE
        pm = self.query_one("#paper-mode", Static)
        pm.update("(PAPER)" if PAPER_MODE else "")
        self.refresh_data()


if __name__ == "__main__":
    app = WeatherDashboard()
    app.run()
