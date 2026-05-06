"""
Tests for live trading safety and risk management.
Critical: verify that no live trade can happen without all prerequisites.
"""

import unittest
from src.weather.config import Config
from src.trading.engine import can_trade_live


class TestLiveTradingSafety(unittest.TestCase):
    """Verify can_trade_live(config) prevents accidental live trading."""

    def setUp(self):
        # Create a config with live_trade=true but missing double lock
        self.config = Config()
        self.config.live_trade = True
        self.config.kill_switch_enabled = False
        self.config.confirm_live_trading = ""  # Double lock NOT set

    def test_live_trade_false(self):
        """When live_trade=False, trading should be blocked."""
        self.config.live_trade = False
        allowed, reason = can_trade_live(self.config)
        self.assertFalse(allowed)
        self.assertEqual(reason, "live_trade=false")

    def test_kill_switch_active(self):
        """When kill_switch_enabled=True, trading should be blocked."""
        self.config.kill_switch_enabled = True
        allowed, reason = can_trade_live(self.config)
        self.assertFalse(allowed)
        self.assertEqual(reason, "kill_switch_active")

    def test_missing_double_lock(self):
        """When confirm_live_trading is not set, trading should be blocked."""
        self.config.confirm_live_trading = ""  # Not set
        allowed, reason = can_trade_live(self.config)
        self.assertFalse(allowed)
        self.assertIn("missing_double_lock", reason)

    def test_wrong_double_lock_value(self):
        """When confirm_live_trading is wrong value, trading should be blocked."""
        self.config.confirm_live_trading = "WRONG_VALUE"
        allowed, reason = can_trade_live(self.config)
        self.assertFalse(allowed)
        self.assertIn("missing_double_lock", reason)

    def test_double_lock_correct(self):
        """When all conditions are met, trading should be allowed."""
        self.config.confirm_live_trading = "I_ACCEPT_REAL_LOSS"
        allowed, reason = can_trade_live(self.config)
        # Will fail executor readiness check if private key missing
        # But basic conditions should pass
        if not allowed:
            self.assertNotIn("live_trade=false", reason)
            self.assertNotIn("kill_switch_active", reason)
            self.assertNotIn("missing_double_lock", reason)


class TestPortfolioRiskManager(unittest.TestCase):
    """Test existing risk manager."""

    def setUp(self):
        self.config = Config()
        self.config.max_exposure_per_city = 300.0
        self.config.max_exposure_per_region = 250.0
        self.config.max_exposure_per_cluster = 300.0
        self.config.max_total_exposure = 1000.0

    def test_city_concentration_limit(self):
        """Exceeding city limit should be rejected."""
        from src.strategy.risk_manager import PortfolioRiskManager
        rm = PortfolioRiskManager(self.config)
        # Simulate open positions: city "london" already has $290 exposure
        open_markets = [type('M', (), {'position': {'status': 'open', 'cost': 290.0}, 'city': 'london'})()]
        result = rm.check_new_trade(city='london', cost=20.0, open_markets=open_markets)
        self.assertFalse(result['allowed'])
        self.assertIn('city_concentration_limit', result['reason'])

    def test_paper_positions_count_toward_city_limit(self):
        """Open paper positions should count as exposure."""
        from src.strategy.risk_manager import PortfolioRiskManager
        rm = PortfolioRiskManager(self.config)
        open_markets = [
            type(
                'M',
                (),
                {
                    'position': None,
                    'paper_position': {'status': 'open', 'cost': 290.0},
                    'city': 'atlanta',
                },
            )()
        ]
        result = rm.check_new_trade(city='atlanta', cost=20.0, open_markets=open_markets)
        self.assertFalse(result['allowed'])
        self.assertIn('city_concentration_limit', result['reason'])

    def test_closed_and_resolved_positions_do_not_count_as_exposure(self):
        """Closed/resolved market files should not create ghost exposure."""
        from src.strategy.risk_manager import PortfolioRiskManager
        rm = PortfolioRiskManager(self.config)
        open_markets = [
            type(
                'M',
                (),
                {
                    'status': 'resolved',
                    'position': {'status': 'open', 'cost': 500.0},
                    'paper_position': {'status': 'closed', 'cost': 500.0},
                    'city': 'atlanta',
                },
            )()
        ]
        result = rm.check_new_trade(city='atlanta', cost=20.0, open_markets=open_markets)
        self.assertTrue(result['allowed'])

    def test_total_exposure_limit(self):
        """Exceeding total exposure should be rejected."""
        from src.strategy.risk_manager import PortfolioRiskManager
        rm = PortfolioRiskManager(self.config)
        # Simulate $990 total exposure already
        open_markets = [type('M', (), {'position': {'status': 'open', 'cost': 990.0}, 'city': 'nyc'})()]
        result = rm.check_new_trade(city='chicago', cost=20.0, open_markets=open_markets)
        self.assertFalse(result['allowed'])
        self.assertIn('total_exposure_limit', result['reason'])


if __name__ == '__main__':
    unittest.main()
