import json

from src.reporting.paper_report import format_paper_report


def test_paper_report_computes_settled_metrics(tmp_path):
    logs_dir = tmp_path / "logs"
    data_dir = tmp_path / "data"
    markets_dir = data_dir / "markets"
    logs_dir.mkdir()
    markets_dir.mkdir(parents=True)

    (logs_dir / "paper_trades.json").write_text(
        json.dumps(
            [
                {
                    "city": "Paris",
                    "date": "2026-04-29",
                    "horizon": "D+0",
                    "entry_price": 0.40,
                    "cost": 20.0,
                    "shares": 50.0,
                    "p": 0.70,
                    "ev": 0.30,
                    "bucket_low": 20.0,
                    "bucket_high": 21.0,
                    "forecast_src": "ecmwf",
                },
                {
                    "city": "London",
                    "date": "2026-04-29",
                    "horizon": "D+0",
                    "entry_price": 0.50,
                    "cost": 10.0,
                    "shares": 20.0,
                    "p": 0.60,
                    "ev": 0.10,
                    "bucket_low": 10.0,
                    "bucket_high": 11.0,
                    "forecast_src": "gfs",
                },
            ]
        ),
        encoding="utf-8",
    )
    (markets_dir / "paris_2026-04-29.json").write_text(
        json.dumps({"city": "paris", "city_name": "Paris", "date": "2026-04-29", "actual_temp": 20.5}),
        encoding="utf-8",
    )
    (markets_dir / "london_2026-04-29.json").write_text(
        json.dumps({"city": "london", "city_name": "London", "date": "2026-04-29", "actual_temp": 20.0}),
        encoding="utf-8",
    )

    report = format_paper_report(
        trades_path=logs_dir / "paper_trades.json",
        markets_dir=markets_dir,
        data_dir=data_dir,
    )

    assert "Trades logged: `2`" in report
    assert "Settled / open: `2` / `0`" in report
    assert "Win rate: `50.0%`" in report
    assert "Net PnL on settled trades: `$+19.70`" in report
    assert "| Paris | 1 | 100.0% | $+29.80 | +149.0% | +0.3000 |" in report


def test_paper_report_flags_unproven_edge(tmp_path):
    logs_dir = tmp_path / "logs"
    data_dir = tmp_path / "data"
    markets_dir = data_dir / "markets"
    logs_dir.mkdir()
    markets_dir.mkdir(parents=True)
    (logs_dir / "paper_trades.json").write_text("[]", encoding="utf-8")

    report = format_paper_report(
        trades_path=logs_dir / "paper_trades.json",
        markets_dir=markets_dir,
        data_dir=data_dir,
    )

    assert "No settled paper trades yet" in report
    assert "Quality-filtered sample too small" in report
