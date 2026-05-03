import unittest
from dataclasses import dataclass, field
from typing import Any
from unittest import mock

from src.trading.decision import TradeDecision
from src.trading.scanner import MarketScanner


@dataclass
class Market:
    city: str = "paris"
    city_name: str = "Paris"
    date: str = "2026-04-29"
    actual_temp: float | None = None
    position: dict | None = None
    paper_position: dict | None = None
    paper_state: dict | None = None
    status: str = "open"
    resolved_outcome: str | None = None


@dataclass
class Modes:
    live_trade: bool = False
    paper_mode: bool = True
    signal_mode: bool = False


@dataclass
class PaperAccount:
    total_trades: int = 0
    wins: int = 0
    losses: int = 0
    total_pnl: float = 0.0

    def record_trade(self, cost: float):
        self.total_trades += 1

    def record_result(self, won: bool, pnl: float, cost: float):
        if won:
            self.wins += 1
        else:
            self.losses += 1
        self.total_pnl += pnl


@dataclass
class Feedback:
    calls: list = field(default_factory=list)

    def notify_trade_open(self, *args, **kwargs):
        self.calls.append((args, kwargs))


class Executor:
    def place_bracket_order(self, *args, **kwargs):
        raise AssertionError("paper mode must not call private CLOB execution")


@dataclass
class Engine:
    modes: Modes = field(default_factory=Modes)
    paper_account: PaperAccount = field(default_factory=PaperAccount)
    feedback: Feedback = field(default_factory=Feedback)
    executor: Executor = field(default_factory=Executor)
    emitted: list = field(default_factory=list)

    def emit(self, message: str):
        self.emitted.append(message)


class TestPaperTradingFlow(unittest.TestCase):
    def _scanner(self, engine: Engine | None = None) -> MarketScanner:
        scanner = MarketScanner.__new__(MarketScanner)
        scanner.engine = engine or Engine()
        return scanner

    def _signal(self):
        return {
            "market_id": "paper-market",
            "token_id": "paper-token",
            "question": "Temp 20-21C",
            "bucket_low": 20.0,
            "bucket_high": 21.0,
            "entry_price": 0.40,
            "bid_at_entry": 0.38,
            "spread": 0.02,
            "shares": 50.0,
            "cost": 20.0,
            "p": 0.70,
            "ev": 0.30,
            "kelly": 0.10,
            "forecast_temp": 20.5,
            "forecast_src": "ecmwf",
            "ml": {"confidence": 0.80},
            "status": "open",
        }

    def _execute(self, scanner, signal, market):
        state = type("State", (), {"total_trades": 0})()
        result = type("Result", (), {"new_trades": 0})()
        loc = type("Loc", (), {"name": "Paris"})()
        with mock.patch("src.trading.scanner.log_paper_trade"):
            return scanner._execute_trade(
                signal,
                market,
                loc,
                "2026-04-29",
                "D+0",
                "C",
                "",
                "TEST",
                500.0,
                state,
                result,
            )

    def test_good_signal_creates_paper_position(self):
        scanner = self._scanner()
        market = Market()
        balance, executed = self._execute(scanner, self._signal(), market)
        self.assertTrue(executed)
        self.assertEqual(balance, 500.0)
        self.assertEqual(market.paper_position["market_id"], "paper-market")
        self.assertEqual(scanner.engine.paper_account.total_trades, 1)

    def test_non_buy_actions_create_no_position(self):
        for action in ["SKIP", "WAIT", "REPRICE", "REDUCE_SIZE", "CANCEL"]:
            decision = TradeDecision(
                market_id="m",
                event_slug="e",
                location="Paris",
                date="2026-04-29",
                outcome="Temp 20-21C",
                model_probability=0.50,
                market_bid=0.40,
                market_ask=0.42,
                entry_price=0.42,
                spread=0.02,
                volume=1000.0,
                gross_edge=0.0,
                net_ev=0.0,
                suggested_size=0.0,
                action=action,
                passed_filters=False,
            )
            market = Market()
            if decision.should_trade():
                self._execute(self._scanner(), self._signal(), market)
            self.assertIsNone(market.paper_position)

    def test_live_false_does_not_call_private_clob(self):
        scanner = self._scanner()
        market = Market()
        _, executed = self._execute(scanner, self._signal(), market)
        self.assertTrue(executed)

    def test_same_signal_twice_creates_one_position(self):
        scanner = self._scanner()
        market = Market()
        self._execute(scanner, self._signal(), market)
        self._execute(scanner, self._signal(), market)
        self.assertEqual(scanner.engine.paper_account.total_trades, 1)

    def test_winning_resolution_positive_pnl(self):
        from src.trading.resolver import MarketResolver

        paper = PaperAccount()
        market = Market(actual_temp=20.5, paper_position=self._signal())
        engine = type("ResolverEngine", (), {"paper_account": paper})()
        with mock.patch("src.trading.resolver.get_actual_temp", return_value=20.5):
            _, won, _ = MarketResolver(engine).resolve_market(market, balance=500.0)
        self.assertTrue(won)
        self.assertGreater(paper.total_pnl, 0)

    def test_losing_resolution_negative_pnl(self):
        from src.trading.resolver import MarketResolver

        paper = PaperAccount()
        market = Market(actual_temp=10.0, paper_position=self._signal())
        engine = type("ResolverEngine", (), {"paper_account": paper})()
        with mock.patch("src.trading.resolver.get_actual_temp", return_value=10.0):
            _, won, _ = MarketResolver(engine).resolve_market(market, balance=500.0)
        self.assertFalse(won)
        self.assertLess(paper.total_pnl, 0)

    def test_ai_review_is_skipped_when_disabled(self):
        scanner = self._scanner()
        scanner._ai_reviews_used = 0
        scanner.engine.config = type("Config", (), {"ai_flow_enabled": False})()
        loc = type("Loc", (), {"name": "Paris"})()

        with mock.patch("src.trading.scanner.get_ai_trade_context") as ai_call:
            result = scanner._review_signal_with_ai(loc, {}, self._signal(), "C")

        self.assertTrue(result["allowed"])
        ai_call.assert_not_called()

    def test_ai_review_runs_when_enabled(self):
        scanner = self._scanner()
        scanner._ai_reviews_used = 0
        scanner.engine.config = type(
            "Config",
            (),
            {"ai_flow_enabled": True, "ai_max_reviews_per_scan": 5, "ai_force_blocking": False, "ai_min_confidence": 0.5, "ai_max_ev_threshold": 2.0},
        )()
        loc = type("Loc", (), {"name": "Paris"})()

        with mock.patch(
            "src.trading.scanner.get_ai_trade_context",
            return_value=({"confidence": "high", "analysis": "ok"}, False),
        ) as ai_call:
            result = scanner._review_signal_with_ai(loc, {}, self._signal(), "C")

        self.assertTrue(result["allowed"])
        self.assertEqual(scanner._ai_reviews_used, 1)
        ai_call.assert_called_once()

    def test_ai_anomaly_blocks_trade(self):
        scanner = self._scanner()
        scanner._ai_reviews_used = 0
        scanner.engine.config = type(
            "Config",
            (),
            {"ai_flow_enabled": True, "ai_max_reviews_per_scan": 5, "ai_force_blocking": False, "ai_min_confidence": 0.5, "ai_max_ev_threshold": 2.0},
        )()
        loc = type("Loc", (), {"name": "Paris"})()

        with mock.patch(
            "src.trading.scanner.get_ai_trade_context",
            return_value=({"confidence": "high", "anomaly": {"is_anomaly": True}}, True),
        ):
            result = scanner._review_signal_with_ai(loc, {}, self._signal(), "C")

        self.assertFalse(result["allowed"])
        self.assertEqual(result["reason"], "ai_anomaly")


if __name__ == "__main__":
    unittest.main()
