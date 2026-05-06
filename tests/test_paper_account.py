import json

import pytest

from src.trading.paper_account import PaperAccount


def test_paper_account_persists_and_loads_filtered_fields(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    account_file = data_dir / "paper_account.json"
    account_file.write_text(
        json.dumps({"balance": 9000.0, "wins": 2, "unknown_field": "ignored"}),
        encoding="utf-8",
    )

    account = PaperAccount(str(data_dir))

    assert account.get_state().balance == 9000.0
    assert account.get_state().wins == 2


def test_paper_account_invalid_json_falls_back_to_defaults(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    (data_dir / "paper_account.json").write_text("{not-json", encoding="utf-8")

    account = PaperAccount(str(data_dir))

    assert account.get_state().balance == 10000.0


def test_record_trade_rejects_non_positive_cost(tmp_path):
    account = PaperAccount(str(tmp_path))

    with pytest.raises(ValueError):
        account.record_trade(0)


def test_record_result_rejects_non_positive_cost(tmp_path):
    account = PaperAccount(str(tmp_path))

    with pytest.raises(ValueError):
        account.record_result(True, pnl=1.0, cost=0)


def test_record_trade_and_results_update_stats_and_report(tmp_path):
    account = PaperAccount(str(tmp_path))

    # record_trade locks only the stake; resolver PnL handles fees/slippage.
    account.record_trade(100.0)
    state = account.get_state()
    assert state.total_trades == 1
    assert state.balance == 9900.0  # 10000 - 100 stake
    assert state.total_fees_paid == 0.0
    assert state.locked_in_positions == 100.0  # Stake locked

    # record_result unlocks stake and adds pnl
    # settlement_cashflow = cost + pnl = 100 + 20 = 120
    account.record_result(True, pnl=20.0, cost=100.0)
    state = account.get_state()
    assert state.wins == 1
    assert state.locked_in_positions == 0.0  # Unlocked
    assert state.total_pnl == 20.0  # Net PnL recorded

    # Second trade
    account.record_trade(50.0)
    state = account.get_state()
    assert state.locked_in_positions == 50.0

    # Loss: settlement_cashflow = 50 + (-50) = 0
    account.record_result(False, pnl=-50.0, cost=50.0)
    state = account.get_state()
    assert state.losses == 1
    assert state.total_pnl == -30.0  # 20 - 50
    assert state.locked_in_positions == 0.0
    assert state.drawdown > 0

    report = account.get_report()
    assert "Trades: `2`" in report
    assert "Win Rate: `50.0%`" in report

    # Test get_equity
    equity = account.get_equity()
    assert equity == account.get_state().balance  # No locked positions

    # Test check_coherence
    coherence = account.check_coherence()
    assert "is_coherent" in coherence
    assert "expected_equity" in coherence
    assert "actual_equity" in coherence


def test_legacy_mixed_results_do_not_fake_gains_losses(tmp_path):
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

    account = PaperAccount(str(tmp_path))

    assert account.get_state().total_gains == 0.0
    assert account.get_state().total_losses == 0.0


def test_detailed_history_rebuilds_gross_gains_losses_from_pnl_sign(tmp_path):
    history_file = tmp_path / "paper_trades.jsonl"
    history_file.write_text(
        "\n".join(
            [
                json.dumps({"won": False, "pnl": 50.0, "stake": 20.0}),
                json.dumps({"won": True, "pnl": -170.76, "stake": 20.0}),
                json.dumps({"won": True, "pnl": 0.0, "stake": 20.0}),
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    account = PaperAccount(str(tmp_path))

    assert account.get_state().total_gains == 50.0
    assert account.get_state().total_losses == 170.76


def test_legacy_history_backfills_detailed_pnl_from_closed_markets(tmp_path):
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
        + "\n",
        encoding="utf-8",
    )
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    (markets_dir / "win.json").write_text(
        json.dumps(
            {
                "city": "paris",
                "date": "2026-05-03",
                "paper_position": {
                    "status": "closed",
                    "market_id": "win-1",
                    "pnl": 50.0,
                    "cost": 20.0,
                    "entry_price": 0.4,
                },
            }
        ),
        encoding="utf-8",
    )
    (markets_dir / "loss.json").write_text(
        json.dumps(
            {
                "city": "london",
                "date": "2026-05-03",
                "paper_position": {
                    "status": "closed",
                    "market_id": "loss-1",
                    "pnl": -170.76,
                    "cost": 20.0,
                    "entry_price": 0.4,
                },
            }
        ),
        encoding="utf-8",
    )

    account = PaperAccount(str(tmp_path))

    assert account.get_state().total_gains == 50.0
    assert account.get_state().total_losses == 170.76
    assert account.get_state().total_pnl == -120.76
    assert "reconstructed_from_market" in history_file.read_text(encoding="utf-8")


def test_recalc_clears_locked_positions_when_no_open_markets(tmp_path):
    account_file = tmp_path / "paper_account.json"
    account_file.write_text(
        json.dumps({"locked_in_positions": 170.15, "balance": 9800.0}),
        encoding="utf-8",
    )
    markets_dir = tmp_path / "markets"
    markets_dir.mkdir()
    (markets_dir / "closed.json").write_text(
        json.dumps(
            {
                "city": "atlanta",
                "date": "2026-05-03",
                "paper_position": {
                    "status": "closed",
                    "market_id": "atl-closed",
                    "pnl": -58.5,
                    "cost": 58.5,
                    "entry_price": 0.5,
                },
            }
        ),
        encoding="utf-8",
    )

    account = PaperAccount(str(tmp_path))

    assert account.get_state().locked_in_positions == 0.0
