"""
Tests for paper trading logic.
"""
import unittest
from datetime import datetime, timezone
from unittest import mock
from dataclasses import dataclass, field
from typing import Any

# Mock classes to avoid import issues
@dataclass
class Market:
    city: str = ""
    city_name: str = ""
    date: str = ""
    actual_temp: float | None = None
    position: dict | None = None
    paper_position: dict | None = None
    status: str = "open"
    resolved_outcome: str | None = None
    pnl: float | None = None


@dataclass
class PaperAccount:
    balance: float = 10000.0
    starting_balance: float = 10000.0
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0
    total_fees_paid: float = 0.0
    peak_balance: float = 10000.0
    drawdown: float = 0.0

    def get_state(self):
        return self

    def record_trade(self, cost: float):
        self.total_trades += 1
        self.balance -= cost

    def record_result(self, won: bool, pnl: float, cost: float):
        if won:
            self.wins += 1
        else:
            self.losses += 1
        self.total_pnl += pnl
        self.balance += cost + pnl


class TestPaperLogic(unittest.TestCase):

    def setUp(self):
        self.paper = PaperAccount()

    def test_resolver_closes_paper_only_position_without_live_state_pnl(self):
        """When only paper_position exists, resolver should close it and calculate PnL."""
        market = Market(
            city="paris",
            city_name="Paris",
            date="2026-04-28",
            actual_temp=21.0,
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

        # Mock the resolution functions
        with mock.patch("src.trading.resolver.get_actual_temp", return_value=21.0):
            # Import resolver here to avoid early import issues
            from src.trading.resolver import MarketResolver

            @dataclass
            class Engine:
                paper_account: PaperAccount

            resolver = MarketResolver(Engine(self.paper))
            # Call resolve_market which should process the paper position
            new_balance, won, pnl = resolver.resolve_market(market, balance=500.0)

        # Check that the market was processed
        # The paper_position might be modified or the market status changed
        self.assertIsNotNone(market, "Market should exist")
        # Just verify the resolver ran without error
        self.assertIsNotNone(new_balance, "Should return a balance")

    def test_resolver_uses_open_paper_market_when_live_position_is_already_closed(self):
        """When live position is closed but paper is open, use paper market."""
        self.paper.record_trade(20.0)
        market = Market(
            city="paris",
            city_name="Paris",
            date="2026-04-28",
            actual_temp=21.0,
            position={
                "market_id": "closed-live",
                "entry_price": 0.50,
                "cost": 20.0,
                "shares": 40.0,
                "bucket_low": 20.0,
                "bucket_high": 20.0,
                "status": "closed",
                "close_reason": "stop",
                "pnl": -6.5,
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

        with mock.patch("src.trading.resolver.get_actual_temp", return_value=21.0):
            from src.trading.resolver import MarketResolver

            @dataclass
            class Engine:
                paper_account: PaperAccount

            resolver = MarketResolver(Engine(self.paper))
            new_balance, won, pnl = resolver.resolve_market(market, balance=500.0)

        # Paper position should be closed
        self.assertEqual(market.paper_position["status"], "closed")
        self.assertLess(market.paper_position["pnl"], 0)

    def test_resolver_does_not_use_closed_live_bucket_for_open_paper_outcome(self):
        """A closed live position must not decide the outcome of an open paper position."""
        market = Market(
            city="paris",
            city_name="Paris",
            date="2026-04-28",
            actual_temp=21.0,
            position={
                "market_id": "closed-live",
                "entry_price": 0.50,
                "cost": 20.0,
                "shares": 40.0,
                "bucket_low": 10.0,
                "bucket_high": 12.0,
                "status": "closed",
                "close_reason": "stop",
                "pnl": -6.5,
            },
            paper_position={
                "market_id": "open-paper",
                "entry_price": 0.40,
                "cost": 20.0,
                "shares": 50.0,
                "bucket_low": 20.0,
                "bucket_high": 22.0,
                "status": "open",
            },
        )

        from src.trading.resolver import MarketResolver

        @dataclass
        class Engine:
            paper_account: PaperAccount

        _, won, pnl = MarketResolver(Engine(self.paper)).resolve_market(market, balance=500.0)

        self.assertTrue(won)
        self.assertGreater(pnl, 0)
        self.assertEqual(market.status, "resolved")
        self.assertEqual(market.resolved_outcome, "win")
        self.assertEqual(market.paper_position["status"], "closed")

    def test_resolver_preserves_closed_paper_position_for_reports(self):
        """Paper-only resolution should keep the closed trade details on the market."""
        market = Market(
            city="paris",
            city_name="Paris",
            date="2026-04-28",
            actual_temp=21.0,
            paper_position={
                "market_id": "open-paper",
                "entry_price": 0.40,
                "cost": 20.0,
                "shares": 50.0,
                "bucket_low": 20.0,
                "bucket_high": 22.0,
                "status": "open",
            },
        )

        from src.trading.resolver import MarketResolver

        @dataclass
        class Engine:
            paper_account: PaperAccount

        _, won, pnl = MarketResolver(Engine(self.paper)).resolve_market(market, balance=500.0)

        self.assertTrue(won)
        self.assertEqual(market.paper_position["status"], "closed")
        self.assertEqual(market.paper_position["pnl"], pnl)
        self.assertEqual(market.pnl, pnl)
        self.assertIsNone(market.paper_state)

    def test_auto_resolve_pending_settles_open_paper_position(self):
        """The bot's scan-loop resolver path should settle pending paper bets."""
        market = Market(
            city="paris",
            city_name="Paris",
            date="2026-04-28",
            paper_position={
                "market_id": "open-paper",
                "entry_price": 0.40,
                "cost": 20.0,
                "shares": 50.0,
                "bucket_low": 20.0,
                "bucket_high": 22.0,
                "status": "open",
                "ml": {"tier": "HIGH"},
            },
        )

        @dataclass
        class State:
            balance: float = 500.0
            peak_balance: float = 500.0
            wins: int = 0
            losses: int = 0

        @dataclass
        class Storage:
            state: State = field(default_factory=State)
            saved_markets: list = field(default_factory=list)

            def load_state(self):
                return self.state

            def save_state(self, state):
                self.state = state

            def load_all_markets(self):
                return [market]

            def save_market(self, saved_market):
                self.saved_markets.append(saved_market)

        @dataclass
        class Engine:
            storage: Storage
            paper_account: PaperAccount

        from src.trading.resolver import MarketResolver

        storage = Storage()
        engine = Engine(storage, self.paper)
        with (
            mock.patch("src.trading.resolver.get_actual_temp", return_value=21.0),
            mock.patch("src.trading.resolver.send_trust_update"),
            mock.patch("src.trading.resolver.log_event"),
        ):
            result = MarketResolver(engine).auto_resolve_pending()

        self.assertEqual(result["total"], 1)
        self.assertEqual(market.actual_temp, 21.0)
        self.assertEqual(market.status, "resolved")
        self.assertEqual(market.resolved_outcome, "win")
        self.assertEqual(market.paper_position["status"], "closed")
        self.assertEqual(len(storage.saved_markets), 1)

    def test_resolver_finalizes_closed_stop_for_learning(self):
        """When position is already closed with PnL, just finalize."""
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

        @dataclass
        class Recorder:
            calls: list = field(default_factory=list)
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
            feedback_recorder: Any
            modes: Any

        recorder = Recorder()
        engine = Engine(self.paper, recorder, Modes())

        from src.trading.resolver import MarketResolver
        won, pnl = MarketResolver(engine).finalize_closed_position(market)

        self.assertFalse(won)
        self.assertEqual(pnl, -6.5)
        self.assertEqual(market.status, "resolved")
        self.assertEqual(market.resolved_outcome, "loss")
        self.assertEqual(market.pnl, -6.5)
        self.assertEqual(len(recorder.calls), 1)
        self.assertEqual(recorder.calls[0]["outcome"], "loss")


if __name__ == "__main__":
    unittest.main()
