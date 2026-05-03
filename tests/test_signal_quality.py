import unittest

from src.trading.decision import DecisionEngine
from src.weather.config import Config


class TestSignalQuality(unittest.TestCase):
    def setUp(self):
        self.config = Config()
        self.config.min_edge = 0.06
        self.config.max_spread = 0.05
        self.config.min_volume = 500
        self.config.min_orderbook_depth_usd = 100.0
        self.engine = DecisionEngine(self.config)

    def _context(self, **overrides):
        outcome = {
            "market_id": "mkt_signal",
            "token_id": "token_signal",
            "question": "Temp 20-21C",
            "bid": 0.64,
            "ask": 0.66,
            "spread": 0.02,
            "volume": 1000.0,
        }
        orderbook = {
            "bids": [{"price": "0.64", "size": "300"}],
            "asks": [{"price": "0.66", "size": "300"}],
        }
        outcome.update(overrides.pop("outcome", {}))
        return {
            "outcome": outcome,
            "features": {"confidence": 0.80},
            "orderbook": overrides.pop("orderbook", orderbook),
            "model_probability": overrides.pop("model_probability", 0.78),
            "bankroll": overrides.pop("bankroll", 10000.0),
            **overrides,
        }

    def test_low_ev_skips(self):
        decision = self.engine.evaluate(self._context(model_probability=0.68))
        self.assertEqual(decision.action, "SKIP")
        self.assertIn("edge", decision.rejected_reason.lower())

    def test_high_spread_skips(self):
        decision = self.engine.evaluate(
            self._context(outcome={"bid": 0.55, "ask": 0.66})
        )
        self.assertEqual(decision.action, "SKIP")
        self.assertIn("spread", decision.rejected_reason.lower())

    def test_low_volume_skips(self):
        decision = self.engine.evaluate(self._context(outcome={"volume": 100.0}))
        self.assertEqual(decision.action, "SKIP")
        self.assertIn("volume", decision.rejected_reason.lower())

    def test_missing_bid_ask_skips(self):
        decision = self.engine.evaluate(self._context(outcome={"bid": 0.0, "ask": 0.0}))
        self.assertEqual(decision.action, "SKIP")
        # Check for either old or new reason format
        reason_lower = decision.rejected_reason.lower()
        self.assertTrue("bid_ask" in reason_lower or "crossed_or_invalid" in reason_lower,
                        f"Expected 'bid_ask' or 'crossed_or_invalid' in '{reason_lower}'")

    def test_insufficient_depth_reduces_size(self):
        shallow_book = {
            "bids": [{"price": "0.64", "size": "300"}],
            "asks": [{"price": "0.66", "size": "10"}],
        }
        decision = self.engine.evaluate(self._context(orderbook=shallow_book))
        self.assertEqual(decision.action, "REDUCE_SIZE")
        self.assertFalse(decision.should_trade())
        self.assertIn("depth", decision.rejected_reason.lower())

    def test_drawdown_exceeded_does_not_buy(self):
        decision = self.engine.evaluate(self._context(daily_pnl=-600.0))
        self.assertNotEqual(decision.action, "BUY")
        self.assertIn("drawdown", decision.rejected_reason.lower())

    def test_size_zero_does_not_buy(self):
        self.config.max_position_pct = 0.0
        decision = self.engine.evaluate(self._context())
        self.assertEqual(decision.action, "SKIP")
        self.assertEqual(decision.suggested_size, 0.0)


if __name__ == "__main__":
    unittest.main()
