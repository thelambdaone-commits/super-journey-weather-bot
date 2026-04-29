import os
import shutil
import unittest
from dataclasses import dataclass
from unittest.mock import patch

from src.storage import Market
from src.trading.paper_account import PaperAccount
from src.trading.resolver import MarketResolver


class TestPaperLogic(unittest.TestCase):
    def setUp(self):
        self.data_dir = "data_test_paper"
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)
        os.makedirs(self.data_dir)
        self.paper = PaperAccount(self.data_dir)

    def tearDown(self):
        if os.path.exists(self.data_dir):
            shutil.rmtree(self.data_dir)

    def test_paper_execution_flow(self):
        # 1. Initial State
        initial_balance = self.paper.get_state().balance
        self.assertEqual(initial_balance, 10000.0)

        # 2. Record a trade
        cost = 50.0
        self.paper.record_trade(cost)

        # 3. Verify stake lock plus 2% simulated entry friction
        new_state = self.paper.get_state()
        self.assertEqual(new_state.balance, 9949.0)
        self.assertEqual(new_state.total_trades, 1)
        self.assertEqual(new_state.total_fees_paid, 1.0)
        self.assertEqual(new_state.total_pnl, -1.0)

    def test_persistence(self):
        self.paper.record_trade(100.0)
        # Create a new instance pointing to same file
        paper2 = PaperAccount(self.data_dir)
        self.assertEqual(paper2.get_state().balance, 9898.0)

    def test_settlement_win_returns_stake_and_profit(self):
        self.paper.record_trade(20.0)
        self.paper.record_result(won=True, pnl=35.0, cost=20.0)

        state = self.paper.get_state()
        self.assertEqual(state.balance, 10034.6)
        self.assertEqual(state.total_trades, 1)
        self.assertEqual(state.wins, 1)
        self.assertEqual(state.losses, 0)
        self.assertAlmostEqual(state.total_pnl, 34.6)

    def test_settlement_loss_keeps_entry_friction_in_net_pnl(self):
        self.paper.record_trade(20.0)
        self.paper.record_result(won=False, pnl=-20.0, cost=20.0)

        state = self.paper.get_state()
        self.assertEqual(state.balance, 9979.6)
        self.assertEqual(state.wins, 0)
        self.assertEqual(state.losses, 1)
        self.assertAlmostEqual(state.total_pnl, -20.4)

    def test_rejects_invalid_costs(self):
        with self.assertRaises(ValueError):
            self.paper.record_trade(0)
        with self.assertRaises(ValueError):
            self.paper.record_result(won=True, pnl=1.0, cost=0)

    def test_load_ignores_unknown_fields(self):
        self.paper.file_path.write_text(
            '{"balance": 42.0, "unknown_future_field": true}',
            encoding="utf-8",
        )

        state = PaperAccount(self.data_dir).get_state()
        self.assertEqual(state.balance, 42.0)
        self.assertEqual(state.starting_balance, 10000.0)

    def test_resolver_closes_paper_only_position_without_live_state_pnl(self):
        self.paper.record_trade(20.0)
        market = Market(
            city="paris",
            city_name="Paris",
            date="2026-04-28",
            paper_position={
                "market_id": "paper-1",
                "entry_price": 0.40,
                "cost": 20.0,
                "shares": 50.0,
                "bucket_low": 20.0,
                "bucket_high": 20.0,
                "status": "open",
            },
        )

        @dataclass
        class Engine:
            paper_account: PaperAccount

        resolver = MarketResolver(Engine(self.paper))

        with (
            patch("src.trading.resolver.check_market_resolved", return_value=True),
            patch("src.trading.resolver.get_actual_temp", return_value=20.0),
        ):
            new_balance, won, pnl = resolver.resolve_market(market, balance=500.0)

        self.assertEqual(new_balance, 500.0)
        self.assertTrue(won)
        self.assertIsNone(pnl)
        self.assertEqual(market.status, "resolved")
        self.assertEqual(market.resolved_outcome, "win")
        self.assertEqual(market.paper_position["status"], "closed")
        self.assertEqual(market.paper_position["pnl"], 29.8)
        self.assertEqual(self.paper.get_state().wins, 1)
        self.assertEqual(self.paper.get_state().balance, 10029.4)

    def test_resolver_uses_open_paper_market_when_live_position_is_already_closed(self):
        self.paper.record_trade(20.0)
        market = Market(
            city="paris",
            city_name="Paris",
            date="2026-04-28",
            position={
                "market_id": "closed-live",
                "entry_price": 0.50,
                "cost": 20.0,
                "shares": 40.0,
                "status": "closed",
            },
            paper_position={
                "market_id": "open-paper",
                "entry_price": 0.40,
                "cost": 20.0,
                "shares": 50.0,
                "bucket_low": 20.0,
                "bucket_high": 20.0,
                "status": "open",
            },
        )

        @dataclass
        class Engine:
            paper_account: PaperAccount

        with (
            patch("src.trading.resolver.check_market_resolved", return_value=False) as resolved,
            patch("src.trading.resolver.get_actual_temp", return_value=21.0),
        ):
            MarketResolver(Engine(self.paper)).resolve_market(market, balance=500.0)

        resolved.assert_called_once_with("open-paper")
        self.assertEqual(market.position["status"], "closed")
        self.assertEqual(market.paper_position["status"], "closed")
        self.assertEqual(market.paper_position["pnl"], -20.2)
        self.assertEqual(self.paper.get_state().losses, 1)

    def test_resolver_finalizes_closed_stop_for_learning(self):
        market = Market(
            city="paris",
            city_name="Paris",
            date="2026-04-28",
            actual_temp=21.0,
            position={
                "market_id": "stopped-live",
                "entry_price": 0.50,
                "cost": 20.0,
                "shares": 40.0,
                "bucket_low": 20.0,
                "bucket_high": 20.0,
                "status": "closed",
                "close_reason": "stop",
                "pnl": -6.5,
            },
        )

        class Recorder:
            def __init__(self):
                self.calls = []

            def record_resolution(self, **kwargs):
                self.calls.append(kwargs)

        @dataclass
        class Modes:
            live_trade: bool = False
            paper_mode: bool = False
            signal_mode: bool = True

        @dataclass
        class Engine:
            paper_account: PaperAccount
            feedback_recorder: Recorder
            modes: Modes

        recorder = Recorder()
        won, pnl = MarketResolver(Engine(self.paper, recorder, Modes())).finalize_closed_position(market)

        self.assertFalse(won)
        self.assertEqual(pnl, -6.5)
        self.assertEqual(market.status, "resolved")
        self.assertEqual(market.resolved_outcome, "loss")
        self.assertEqual(market.pnl, -6.5)
        self.assertEqual(len(recorder.calls), 1)
        self.assertEqual(recorder.calls[0]["outcome"], "loss")


if __name__ == "__main__":
    unittest.main()
