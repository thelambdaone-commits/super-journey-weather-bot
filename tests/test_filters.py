"""
Tests for filters.py classes.
"""

import unittest
from src.strategy.filters import (
    VolumeFilter,
    SpreadFilter,
    LiquidityFilter,
    EVFilter,
    AntiCrossedBookFilter,
    ConfidenceFilter,
    SourceContradictionFilter,
    run_all_filters,
)


class TestVolumeFilter(unittest.TestCase):

    def setUp(self):
        self.filter = VolumeFilter(min_volume=500)

    def test_volume_sufficient(self):
        outcome = {"volume": 1000}
        result = self.filter.check(outcome)
        self.assertTrue(result.passed)

    def test_volume_too_low(self):
        outcome = {"volume": 200}
        result = self.filter.check(outcome)
        self.assertFalse(result.passed)
        self.assertIn("volume_too_low", result.reason)


class TestSpreadFilter(unittest.TestCase):

    def setUp(self):
        self.filter = SpreadFilter(max_spread=0.05)

    def test_spread_ok(self):
        outcome = {"spread": 0.03}
        result = self.filter.check(outcome)
        self.assertTrue(result.passed)

    def test_spread_too_high(self):
        outcome = {"spread": 0.08}
        result = self.filter.check(outcome)
        self.assertFalse(result.passed)


class TestAntiCrossedBookFilter(unittest.TestCase):

    def setUp(self):
        self.filter = AntiCrossedBookFilter()

    def test_normal_book(self):
        outcome = {"bid": 0.65, "ask": 0.70}
        result = self.filter.check(outcome)
        self.assertTrue(result.passed)

    def test_crossed_book(self):
        outcome = {"bid": 0.70, "ask": 0.65}
        result = self.filter.check(outcome)
        self.assertFalse(result.passed)
        self.assertEqual(result.reason, "crossed_book")


class TestLiquidityFilter(unittest.TestCase):

    def setUp(self):
        self.filter = LiquidityFilter(min_depth_usd=100.0)

    def test_orderbook_depth_sufficient(self):
        outcome = {"bid": 0.65, "ask": 0.70}
        orderbook = {"asks": [[0.70, 200.0], [0.71, 300.0]]}
        result = self.filter.check(outcome, orderbook)
        self.assertTrue(result.passed)

    def test_orderbook_depth_insufficient(self):
        outcome = {"bid": 0.65, "ask": 0.70}
        orderbook = {"asks": [[0.70, 50.0]]}
        result = self.filter.check(outcome, orderbook)
        self.assertFalse(result.passed)


class TestEVFilter(unittest.TestCase):

    def setUp(self):
        self.filter = EVFilter(min_edge=0.06, require_positive_net_ev=True)

    def test_good_ev(self):
        result = self.filter.check(net_ev=0.08, gross_edge=0.10)
        self.assertTrue(result.passed)

    def test_insufficient_gross_edge(self):
        result = self.filter.check(net_ev=0.04, gross_edge=0.04)
        self.assertFalse(result.passed)

    def test_negative_net_ev(self):
        result = self.filter.check(net_ev=-0.02, gross_edge=0.10)
        self.assertFalse(result.passed)


class TestConfidenceFilter(unittest.TestCase):

    def setUp(self):
        self.filter = ConfidenceFilter(min_confidence=0.15)

    def test_high_confidence(self):
        features = {"confidence": 0.20}
        result = self.filter.check(features)
        self.assertTrue(result.passed)

    def test_low_confidence(self):
        features = {"confidence": 0.10}
        result = self.filter.check(features)
        self.assertFalse(result.passed)

    def test_no_confidence(self):
        features = {}
        result = self.filter.check(features)
        self.assertTrue(result.passed)  # None means no filter applied


class TestSourceContradictionFilter(unittest.TestCase):

    def setUp(self):
        self.filter = SourceContradictionFilter()

    def test_sources_agree(self):
        features = {"ecmwf_max": 25.0, "gfs_max": 26.0}
        result = self.filter.check(features)
        self.assertTrue(result.passed)

    def test_sources_disagree(self):
        features = {"ecmwf_max": 20.0, "gfs_max": 26.0}
        result = self.filter.check(features)
        self.assertFalse(result.passed)


class TestRunAllFilters(unittest.TestCase):

    def test_all_passed(self):
        outcome = {
            "bid": 0.65, "ask": 0.70, "spread": 0.05, "volume": 1000
        }
        features = {"confidence": 0.20, "ecmwf_max": 25.0, "gfs_max": 26.0}
        orderbook = {"asks": [[0.70, 200.0]]}

        # Mock config
        class MockConfig:
            min_volume = 500
            max_spread = 0.05
            min_edge = 0.06
            require_positive_net_ev = True
            min_confidence = 0.15
            min_orderbook_depth_usd = 100.0

        result = run_all_filters(outcome, features, orderbook, net_ev=0.08, gross_edge=0.10, config=MockConfig())
        self.assertTrue(result["passed"])
        self.assertEqual(result["rejected_reason"], "")

    def test_spread_too_high(self):
        outcome = {
            "bid": 0.65, "ask": 0.75, "spread": 0.10, "volume": 1000
        }
        features = {"confidence": 0.20}
        orderbook = {"asks": [[0.75, 200.0]]}

        class MockConfig:
            min_volume = 500
            max_spread = 0.05
            min_edge = 0.06
            require_positive_net_ev = True
            min_confidence = 0.15
            min_orderbook_depth_usd = 100.0

        result = run_all_filters(outcome, features, orderbook, net_ev=0.08, gross_edge=0.10, config=MockConfig())
        self.assertFalse(result["passed"])
        self.assertIn("spread", result["rejected_reason"])


if __name__ == "__main__":
    unittest.main()
