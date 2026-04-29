"""
Tests for edge.py functions.
"""

import unittest
from src.strategy.edge import (
    implied_probability_from_price,
    gross_edge,
    estimate_fee,
    estimate_slippage,
    net_ev,
    should_bet,
)


class TestEdgeFunctions(unittest.TestCase):

    def test_implied_probability_from_price(self):
        self.assertEqual(implied_probability_from_price(0.75), 0.75)
        self.assertEqual(implied_probability_from_price(0.0), 0.0)
        self.assertEqual(implied_probability_from_price(1.0), 1.0)

    def test_gross_edge(self):
        # model_prob > market_price => positive edge
        self.assertAlmostEqual(gross_edge(0.80, 0.70), 0.10, places=4)
        # model_prob < market_price => negative edge
        self.assertAlmostEqual(gross_edge(0.60, 0.70), -0.10, places=4)
        # equal
        self.assertAlmostEqual(gross_edge(0.70, 0.70), 0.0, places=4)

    def test_estimate_fee(self):
        # Mock config
        class MockConfig:
            estimated_fee_bps = 10.0  # 10 bps = 0.1%

        config = MockConfig()
        # fee as probability-equivalent
        fee = estimate_fee(0.70, 100.0, config)
        expected = (100.0 * 0.001) / 100.0  # fee_usd / size = 0.001
        self.assertAlmostEqual(fee, 0.001, places=4)

    def test_estimate_slippage(self):
        orderbook = {
            "asks": [
                [0.71, 500.0],  # price, size (shares)
                [0.72, 1000.0],
            ]
        }
        # Buying $100 worth at $0.71 -> ~140.85 shares
        slippage = estimate_slippage(orderbook, 100.0, side="buy")
        self.assertGreaterEqual(slippage, 0.0)

    def test_net_ev_positive(self):
        model_prob = 0.80
        entry_price = 0.70
        fee = 0.005  # 0.5%
        slippage = 0.002
        result = net_ev(model_prob, entry_price, fee, slippage)
        # 0.80 - 0.70 - 0.005 - 0.002 = 0.093
        self.assertAlmostEqual(result, 0.093, places=4)

    def test_net_ev_negative(self):
        model_prob = 0.70
        entry_price = 0.75
        fee = 0.005
        slippage = 0.002
        result = net_ev(model_prob, entry_price, fee, slippage)
        self.assertLess(result, 0)

    def test_should_bet(self):
        self.assertTrue(should_bet(0.06, 0.05))
        self.assertFalse(should_bet(0.04, 0.05))


if __name__ == "__main__":
    unittest.main()
