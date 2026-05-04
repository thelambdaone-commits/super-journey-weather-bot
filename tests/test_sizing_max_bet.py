"""
Test that find_all_opportunities never proposes a size greater than max_bet.
"""
import unittest
from unittest import mock
from src.trading.scanner import MarketScanner
from src.weather.config import Config
from src.strategy.signal_quality import SignalQualityLayer


class TestSizingMaxBet(unittest.TestCase):
    """Verify that opportunity sizes never exceed max_bet."""

    @mock.patch("src.trading.scanner.MarketScanner._refresh_outcome_orderbook", return_value=True)
    @mock.patch("src.trading.scanner.MarketScanner._filter_config_for_mode")
    @mock.patch("src.trading.scanner.get_ai_trade_context", return_value=({"confidence": 0.8}, False))
    @mock.patch("src.trading.scanner.run_all_filters")
    @mock.patch("src.strategy.signal_quality.SignalQualityLayer")
    @mock.patch("src.strategy.range_probability.calculate_all_bucket_probs")
    def test_opportunity_size_never_exceeds_max_bet(
        self,
        mock_calc_probs,
        mock_signal_quality_cls,
        mock_run_filters,
        mock_ai_context,
        mock_refresh_orderbook,
        mock_filter_config,
    ):
        """Ensure all opportunities have size <= max_bet."""
        config = Config()
        config.max_bet = 5.0
        config.kelly_fraction = 1.0
        config.min_edge = 0.0
        config.max_position_pct = 1.0
        config.max_market_exposure_pct = 1.0

        engine = mock.MagicMock()
        engine.config = config
        engine.modes.paper_mode = False
        engine.modes.live_trade = False
        engine.modes.signal_mode = False
        engine.feature_engine.build.return_value = {"confidence": 0.9, "mae": 1.0}

        mock_signal_quality = mock.MagicMock()
        mock_signal_quality.validate.return_value = {"accepted": True}
        engine.signal_quality = mock_signal_quality

        scanner = MarketScanner.__new__(MarketScanner)
        scanner.engine = engine
        scanner._orderbook_cache = {}
        scanner._clob_requests_used = 0
        scanner._ai_reviews_used = 0

        mock_calc_probs.return_value = [
            {
                "bucket": "20-21",
                "outcome": {
                    "token_id": "tok1",
                    "market_id": "m1",
                    "question": "Temp 20-21",
                    "price_market": 0.3,
                    "volume": 1000,
                    "bid": 0.28,
                    "ask": 0.32,
                    "spread": 0.04,
                    "orderbook": {"asks": [{"price": 0.32, "size": 1000}]},
                },
                "prob_model": 0.95,
                "price_market": 0.3,
                "edge_brut": 0.65,
            }
        ]

        mock_run_filters.return_value = {"passed": True, "rejected_reason": "", "filter_results": {}}
        mock_filter_config.return_value = config

        opportunities = scanner.find_all_opportunities(
            city_slug="paris",
            loc=mock.MagicMock(unit="C", slug="paris"),
            snap={"best": 20.5},
            outcomes=[
                {
                    "token_id": "tok1",
                    "market_id": "m1",
                    "question": "Temp 20-21",
                    "price": 0.3,
                    "volume": 1000,
                    "bid": 0.28,
                    "ask": 0.32,
                    "spread": 0.04,
                    "orderbook": {"asks": [{"price": 0.32, "size": 1000}]},
                }
            ],
            hours=24,
            base_features={},
            balance=100000.0,
        )

        for opp in opportunities:
            with self.subTest(bucket=opp.get("bucket")):
                self.assertLessEqual(
                    opp["size"],
                    config.max_bet,
                    msg=f"Size ${opp['size']:.2f} exceeds max_bet ${config.max_bet:.2f}",
                )

    @mock.patch("src.trading.scanner.MarketScanner._refresh_outcome_orderbook", return_value=True)
    @mock.patch("src.trading.scanner.MarketScanner._filter_config_for_mode")
    @mock.patch("src.trading.scanner.get_ai_trade_context", return_value=({"confidence": 0.8}, False))
    @mock.patch("src.trading.scanner.run_all_filters")
    @mock.patch("src.strategy.signal_quality.SignalQualityLayer")
    @mock.patch("src.strategy.range_probability.calculate_all_bucket_probs")
    def test_paper_training_mode_respects_paper_training_max_bet(
        self,
        mock_calc_probs,
        mock_signal_quality_cls,
        mock_run_filters,
        mock_ai_context,
        mock_refresh_orderbook,
        mock_filter_config,
    ):
        config = Config()
        config.paper_training_mode = True
        config.paper_training_max_bet_usd = 3.0
        config.max_bet = 20.0

        engine = mock.MagicMock()
        engine.config = config
        engine.modes.paper_mode = True
        engine.modes.live_trade = False
        engine.modes.signal_mode = False
        engine.feature_engine.build.return_value = {"confidence": 0.9, "mae": 1.0}

        mock_signal_quality = mock.MagicMock()
        mock_signal_quality.validate.return_value = {"accepted": True}
        engine.signal_quality = mock_signal_quality

        scanner = MarketScanner.__new__(MarketScanner)
        scanner.engine = engine
        scanner._orderbook_cache = {}
        scanner._clob_requests_used = 0
        scanner._ai_reviews_used = 0

        mock_calc_probs.return_value = [
            {
                "bucket": "20-21",
                "outcome": {
                    "token_id": "tok1",
                    "market_id": "m1",
                    "question": "Temp 20-21",
                    "price_market": 0.3,
                    "volume": 1000,
                    "bid": 0.28,
                    "ask": 0.32,
                    "spread": 0.04,
                    "orderbook": {"asks": [{"price": 0.32, "size": 1000}]},
                },
                "prob_model": 0.95,
                "price_market": 0.3,
                "edge_brut": 0.65,
            }
        ]
        mock_run_filters.return_value = {"passed": True, "rejected_reason": "", "filter_results": {}}
        mock_filter_config.return_value = config

        opportunities = scanner.find_all_opportunities(
            city_slug="paris",
            loc=mock.MagicMock(unit="C", slug="paris"),
            snap={"best": 20.5},
            outcomes=[
                {
                    "token_id": "tok1",
                    "market_id": "m1",
                    "question": "Temp 20-21",
                    "price": 0.3,
                    "volume": 1000,
                    "bid": 0.28,
                    "ask": 0.32,
                    "spread": 0.04,
                    "orderbook": {"asks": [{"price": 0.32, "size": 1000}]},
                }
            ],
            hours=24,
            base_features={},
            balance=100000.0,
        )

        for opp in opportunities:
            with self.subTest(bucket=opp.get("bucket")):
                self.assertLessEqual(
                    opp["size"],
                    config.paper_training_max_bet_usd,
                    msg=f"Size ${opp['size']:.2f} exceeds paper_training_max_bet_usd ${config.paper_training_max_bet_usd:.2f}",
                )


if __name__ == "__main__":
    unittest.main()
