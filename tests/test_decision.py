"""
Tests for decision.py - TradeDecision and DecisionEngine.
Covers 6 actions: BUY / SKIP / WAIT / REPRICE / CANCEL / REDUCE_SIZE.
"""

import unittest
from src.trading.decision import TradeDecision, DecisionEngine, log_decision_jsonl
from src.weather.config import Config


class TestTradeDecision(unittest.TestCase):
    """Test TradeDecision dataclass."""

    def setUp(self):
        self.config = Config()
        self.decision = TradeDecision(
            market_id="mkt_1",
            event_slug="highest-temperature-in-london-on-jan-1-2026",
            location="london",
            date="2026-01-01",
            outcome="Temperature > 15°C",
            model_probability=0.80,
            market_bid=0.65,
            market_ask=0.70,
            entry_price=0.70,
            spread=0.05,
            volume=1000.0,
            gross_edge=0.10,
            net_ev=0.08,
            suggested_size=50.0,
            action="BUY",
            passed_filters=True,
        )

    def test_should_trade_buy(self):
        """BUY action should return True for should_trade()."""
        self.assertTrue(self.decision.should_trade())
        self.assertFalse(self.decision.is_terminal())  # BUY is not terminal

    def test_should_trade_skip(self):
        """SKIP action should return False for should_trade()."""
        self.decision.action = "SKIP"
        self.assertFalse(self.decision.should_trade())
        self.assertTrue(self.decision.is_terminal())

    def test_wait_action(self):
        """WAIT action: good edge but bad liquidity/price."""
        self.decision.action = "WAIT"
        self.assertFalse(self.decision.should_trade())
        self.assertFalse(self.decision.is_terminal())  # Not terminal, wait and retry

    def test_reprice_action(self):
        """REPRICE action: edge good but ask moved."""
        self.decision.action = "REPRICE"
        self.assertFalse(self.decision.should_trade())
        self.assertFalse(self.decision.is_terminal())

    def test_cancel_action(self):
        """CANCEL action: cancel remaining unfilled portion."""
        self.decision.action = "CANCEL"
        self.assertFalse(self.decision.should_trade())
        self.assertTrue(self.decision.is_terminal())

    def test_reduce_size_action(self):
        """REDUCE_SIZE action: insufficient depth, reduce size."""
        self.decision.action = "REDUCE_SIZE"
        self.assertFalse(self.decision.should_trade())
        self.assertFalse(self.decision.is_terminal())

    def test_to_dict(self):
        """Test serialization to dict for JSONL logging."""
        d = self.decision.to_dict()
        self.assertEqual(d["market_id"], "mkt_1")
        self.assertEqual(d["action"], "BUY")
        self.assertEqual(d["model_probability"], 0.80)
        self.assertIn("timestamp", d)


class TestDecisionEngine(unittest.TestCase):
    """Test DecisionEngine.evaluate() with mocked context."""

    def setUp(self):
        self.config = Config()
        self.engine = DecisionEngine(self.config)

    def test_evaluate_buy_decision(self):
        """Context with good edge and passing filters => BUY."""
        context = {
            "outcome": {
                "market_id": "mkt_1",
                "token_id": "token_1",
                "bid": 0.65,
                "ask": 0.70,
                "spread": 0.05,
                "volume": 1000.0,
            },
            "model_probability": 0.80,
            "bankroll": 10000.0,
        }
        decision = self.engine.evaluate(context)
        self.assertEqual(decision.action, "BUY")
        self.assertTrue(decision.passed_filters)
        self.assertGreater(decision.suggested_size, 0)

    def test_evaluate_skip_low_edge(self):
        """Context with low edge => SKIP."""
        context = {
            "outcome": {
                "market_id": "mkt_2",
                "token_id": "token_2",
                "bid": 0.65,
                "ask": 0.70,
                "spread": 0.05,
                "volume": 1000.0,
            },
            "model_probability": 0.714,  # Very low edge: 0.714 - 0.70 = 0.014 < min_edge (0.015)
            "bankroll": 10000.0,
        }
        decision = self.engine.evaluate(context)
        self.assertEqual(decision.action, "SKIP")
        self.assertFalse(decision.passed_filters)
        self.assertIn("edge", decision.rejected_reason.lower())

    def test_evaluate_skip_low_volume(self):
        """Context with low volume => REDUCE_SIZE or SKIP."""
        context = {
            "outcome": {
                "market_id": "mkt_3",
                "token_id": "token_3",
                "bid": 0.65,
                "ask": 0.70,
                "spread": 0.05,
                "volume": 10.0,  # Below min_volume=50
            },
            "model_probability": 0.80,
            "bankroll": 10000.0,
        }
        decision = self.engine.evaluate(context)
        # Low volume leads to REDUCE_SIZE (not enough depth) or SKIP
        self.assertIn(decision.action, ["REDUCE_SIZE", "SKIP"])
        self.assertIn("volume", decision.rejected_reason.lower())

    def test_evaluate_skip_crossed_book(self):
        """Context with crossed book (bid > ask) => SKIP."""
        context = {
            "outcome": {
                "market_id": "mkt_4",
                "token_id": "token_4",
                "bid": 0.75,
                "ask": 0.70,  # Crossed!
                "spread": -0.05,
                "volume": 1000.0,
            },
            "model_probability": 0.80,
            "bankroll": 10000.0,
        }
        decision = self.engine.evaluate(context)
        self.assertEqual(decision.action, "SKIP")
        self.assertIn("crossed", decision.rejected_reason.lower())


class TestLogDecisionJsonl(unittest.TestCase):
    """Test logging decisions to JSONL."""

    def test_log_decision(self):
        """Log a decision and verify it's written correctly."""
        import tempfile
        import os
        import json

        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.jsonl') as f:
            tmpfile = f.name

        try:
            decision = TradeDecision(
                market_id="mkt_test",
                event_slug="test",
                location="london",
                date="2026-01-01",
                outcome=">15°C",
                model_probability=0.80,
                market_bid=0.65,
                market_ask=0.70,
                entry_price=0.70,
                spread=0.05,
                volume=1000.0,
                gross_edge=0.10,
                net_ev=0.08,
                suggested_size=50.0,
                action="BUY",
                passed_filters=True,
            )
            log_decision_jsonl(decision, filepath=tmpfile)

            with open(tmpfile, 'r') as f:
                line = f.readline()
                data = json.loads(line)
                self.assertEqual(data["market_id"], "mkt_test")
                self.assertEqual(data["action"], "BUY")
                self.assertIn("timestamp", data)
        finally:
            os.unlink(tmpfile)


if __name__ == '__main__':
    unittest.main()
