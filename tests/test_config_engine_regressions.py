import json
from dataclasses import fields
from types import SimpleNamespace
from unittest import mock

import pytest

from src.data.metrics import calculate_audit_metrics
from src.trading.engine import TradingEngine
from src.trading.scanner import MarketScanner
from src.trading.timing import get_opportunity_window, is_in_opportunity_window, should_scan_now
from src.weather.config import Config


def test_signal_min_confidence_is_defined_once():
    names = [field.name for field in fields(Config)]

    assert names.count("signal_min_confidence") == 1
    assert Config().signal_min_confidence == 0.05


def test_engine_exposure_limit_uses_configured_total_limit_when_present():
    engine = TradingEngine.__new__(TradingEngine)
    config = Config()
    config.max_total_exposure = 123.0
    engine.config = config

    assert engine._max_total_exposure(10_000.0) == 123.0


def test_engine_exposure_limit_falls_back_to_balance_percentage():
    engine = TradingEngine.__new__(TradingEngine)
    config = Config()
    config.max_total_exposure = None
    config.max_market_exposure_pct = 0.05
    engine.config = config

    assert engine._max_total_exposure(10_000.0) == 500.0


def test_engine_ml_sample_count_supports_non_dict_models():
    engine = TradingEngine.__new__(TradingEngine)
    engine.ml_model = type("Model", (), {"samples": 42})()

    assert engine._ml_sample_count() == 42


def test_model_timing_recognizes_current_post_run_window():
    from datetime import datetime, timezone

    now = datetime(2026, 5, 2, 6, 45, tzinfo=timezone.utc)
    last_scan = datetime(2026, 5, 2, 3, 0, tzinfo=timezone.utc)

    assert should_scan_now(last_scan, now=now) is True
    assert is_in_opportunity_window(now) is True
    assert get_opportunity_window(now)[0] == datetime(2026, 5, 2, 6, 30, tzinfo=timezone.utc)


def test_model_timing_does_not_scan_before_window_if_interval_not_elapsed():
    from datetime import datetime, timezone

    now = datetime(2026, 5, 2, 6, 20, tzinfo=timezone.utc)
    last_scan = datetime(2026, 5, 2, 5, 0, tzinfo=timezone.utc)

    assert should_scan_now(last_scan, now=now) is False


def test_scanner_resolve_pending_markets_settles_paper_positions_without_live_counters():
    market = SimpleNamespace(
        status="open",
        position=None,
        paper_position={
            "bucket_low": 20.0,
            "bucket_high": 22.0,
            "status": "open",
        },
        unit="C",
        actual_temp=21.0,
        city_name="Paris",
        date="2026-05-02",
    )
    state = SimpleNamespace(wins=0, losses=0)
    result = SimpleNamespace(resolved=0)
    saved = []
    notified = {}
    engine = SimpleNamespace(
        storage=SimpleNamespace(
            load_all_markets=lambda: [market],
            save_market=lambda saved_market: saved.append(saved_market),
        ),
        resolve_market=lambda resolved_market, balance: (
            balance,
            True,
            12.34,
        ),
        feedback=SimpleNamespace(
            notify_trade_win=lambda *args: notified.update({"win": args}),
            notify_trade_loss=lambda *args: notified.update({"loss": args}),
        ),
        emit=lambda message: None,
    )

    scanner = MarketScanner.__new__(MarketScanner)
    scanner.engine = engine
    scanner.resolve_pending_markets(500.0, state, result)

    assert result.resolved == 1
    assert state.wins == 0
    assert state.losses == 0
    assert len(saved) == 1
    assert "win" in notified


def test_metrics_mark_drift_insufficient_when_trade_sample_is_small():
    trades = [{"pnl": 10.0, "unix_ts": 1.0}, {"pnl": -5.0, "unix_ts": 2.0}]

    metrics = calculate_audit_metrics(trades, starting_balance=10_000.0)

    assert metrics.drift_status == "insufficient_data"


def test_hourly_report_filters_stale_tiny_price_watch_signals():
    config = Config()
    config.report_signal_max_age_hours = 48
    config.report_min_signal_price = 0.01
    captured = {}

    engine = TradingEngine.__new__(TradingEngine)
    engine.config = config
    engine.start_time = 0
    engine.error_count = 0
    engine.latency_sum = 0
    engine.latency_count = 0
    engine.risk_manager = SimpleNamespace(
        get_risk_summary=lambda markets: {
            "total_exposure": 0.0,
            "active_cities": 0,
            "diversification_index": 0,
        }
    )
    engine.feedback = SimpleNamespace(
        notify_hourly_report=lambda summary, city_signals: captured.update(
            {"summary": summary, "city_signals": city_signals}
        )
    )
    engine.storage = SimpleNamespace(
        load_state=lambda: SimpleNamespace(
            balance=10_000.0,
            starting_balance=10_000.0,
            peak_balance=10_000.0,
            last_report_ts=0,
        ),
        save_state=lambda state: None,
            load_all_markets=lambda: [
                SimpleNamespace(
                    city="old",
                city_name="Old",
                date="2026-04-26",
                status="open",
                pnl=None,
                resolved_outcome=None,
                position=None,
                signal_state={},
                last_analysis={
                    "ev": 1000.0,
                    "price": 0.0005,
                    "conf": 0.95,
                    "ts": "2026-04-26T08:00:00+00:00",
                    },
                ),
                SimpleNamespace(
                    city="paper",
                    city_name="Paper",
                    date="2026-05-01",
                    status="open",
                    pnl=None,
                    resolved_outcome=None,
                    position=None,
                    paper_position={
                        "ev": 9.0,
                        "p": 0.40,
                        "entry_price": 0.10,
                        "cost": 5.0,
                        "status": "open",
                        "opened_at": "2026-04-30T08:00:00+00:00",
                        "ml": {"confidence": 0.80},
                    },
                    signal_state={},
                    last_analysis={},
                ),
                SimpleNamespace(
                    city="fresh",
                    city_name="Fresh",
                date="2026-05-01",
                status="open",
                pnl=None,
                resolved_outcome=None,
                position=None,
                signal_state={
                    "ev": 0.20,
                    "entry_price": 0.10,
                    "ml_conf": 0.70,
                    "ts": "2026-04-30T08:00:00+00:00",
                },
                last_analysis={},
            ),
        ],
    )

    with (
        mock.patch("src.trading.health.get_api_statuses", return_value=[]),
        mock.patch("src.trading.engine.time.time", return_value=1_777_520_000),
        mock.patch("src.trading.engine.datetime") as fake_datetime,
        mock.patch("src.notifications.telegram_control_center.send_daily_report"),
    ):
        from datetime import datetime, timezone

        fake_datetime.now.return_value = datetime(2026, 4, 30, 8, 18, 29, tzinfo=timezone.utc)
        fake_datetime.fromisoformat.side_effect = datetime.fromisoformat
        engine.send_full_audit_report()

    assert [item["city"] for item in captured["city_signals"]] == ["Paper"]
    assert captured["city_signals"][0]["risk"] == "PAPER"
    assert captured["city_signals"][0]["edge"] == pytest.approx(30.0)
    assert captured["summary"]["mode"] == "paper"
    assert captured["summary"]["exposure"] == 0.0
    assert captured["summary"]["paper_exposure"] == 5.0
    assert captured["summary"]["drift"] == "insufficient_data"


def test_hourly_report_formats_paper_only_exposure():
    from src.notifications import TelegramNotifier

    notifier = TelegramNotifier.__new__(TelegramNotifier)
    captured = {}
    notifier.send = lambda message, parse_mode=None: captured.update(
        {"message": message, "parse_mode": parse_mode}
    ) or True

    summary = {
        "mode": "paper",
        "pnl_total": -40.76,
        "pnl_pct": -0.41,
        "pf": 0.69,
        "sharpe": -0.50,
        "avg_win": 45.64,
        "avg_loss": 14.67,
        "drawdown": 1.21,
        "exposure": 0.0,
        "paper_exposure": 65.0,
        "drift": "stable",
        "uptime": "20h 16m",
        "api_status": "telegram: connected",
    }

    assert notifier.notify_hourly_report(summary, []) is True

    message = captured["message"]
    assert "→ Mode: `PAPER`" in message
    assert "Expo Paper: `65.00$`" in message
    assert "Expo Live" not in message


def test_health_report_flags_partial_legacy_gains_losses(tmp_path):
    from types import SimpleNamespace

    from src.trading.engine import TradingEngine

    history_file = tmp_path / "paper_trades.jsonl"
    history_file.write_text(
        json.dumps(
            {
                "historical_reconstructed": True,
                "estimated": True,
                "wins": 1,
                "losses": 6,
                "pnl_total": -120.76,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    paper_account = SimpleNamespace(file_path=tmp_path / "paper_account.json")
    engine = SimpleNamespace(paper_account=paper_account)
    paper_stats = SimpleNamespace(
        wins=1,
        losses=6,
        total_gains=0.0,
        total_losses=0.0,
        total_pnl=-120.76,
    )

    assert (
        TradingEngine._gains_pertes_display(engine, paper_stats)
        == "→ Wins: `1` | Losses: `6`\n"
        "→ Gains: `incomplet` | Pertes: `incomplet`\n"
        "→ PnL réalisé: `-$120.76`"
    )


def test_health_report_formats_detailed_gross_gains_losses(tmp_path):
    history_file = tmp_path / "paper_trades.jsonl"
    history_file.write_text(
        json.dumps({"won": True, "pnl": 50.0})
        + "\n"
        + json.dumps({"won": False, "pnl": -170.76})
        + "\n",
        encoding="utf-8",
    )
    paper_account = SimpleNamespace(file_path=tmp_path / "paper_account.json")
    engine = SimpleNamespace(paper_account=paper_account)
    paper_stats = SimpleNamespace(
        wins=1,
        losses=1,
        total_gains=50.0,
        total_losses=170.76,
        total_pnl=-120.76,
    )

    assert (
        TradingEngine._gains_pertes_display(engine, paper_stats)
        == "→ Wins: `1` | Losses: `1`\n"
        "→ Gains: `$50.00` | Pertes: `$170.76`\n"
        "→ PnL réalisé: `-$120.76`"
    )


def test_health_report_formats_complete_when_legacy_is_backfilled(tmp_path):
    history_file = tmp_path / "paper_trades.jsonl"
    history_file.write_text(
        json.dumps(
            {
                "historical_reconstructed": True,
                "estimated": True,
                "wins": 1,
                "losses": 1,
                "pnl_total": -120.76,
            }
        )
        + "\n"
        + json.dumps({"market_id": "win-1", "city": "paris", "date": "2026-05-03", "pnl": 50.0})
        + "\n"
        + json.dumps({"market_id": "loss-1", "city": "london", "date": "2026-05-03", "pnl": -170.76})
        + "\n",
        encoding="utf-8",
    )
    paper_account = SimpleNamespace(file_path=tmp_path / "paper_account.json")
    engine = SimpleNamespace(paper_account=paper_account)
    paper_stats = SimpleNamespace(
        wins=1,
        losses=1,
        total_gains=50.0,
        total_losses=170.76,
        total_pnl=-120.76,
    )

    assert (
        TradingEngine._gains_pertes_display(engine, paper_stats)
        == "→ Wins: `1` | Losses: `1`\n"
        "→ Gains: `$50.00` | Pertes: `$170.76`\n"
        "→ PnL réalisé: `-$120.76`"
    )
