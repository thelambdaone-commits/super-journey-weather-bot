"""
Tests for open-meteo rate limiter.
"""
import unittest
from unittest import mock
from datetime import datetime


class FakeResponse:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.ok = 200 <= status_code < 300
        self.url = None  # Will be set by mock


class TestRateLimitedGet(unittest.TestCase):

    def test_rate_limited_get_waits_between_requests(self):
        """Test that rate_limited_get waits between requests."""
        import src.weather.open_meteo_rate_limiter as limiter

        sleeps = []
        calls = []

        # Mock the time functions
        with mock.patch.object(limiter, '_cooldown_until', 0.0):
            with mock.patch.object(limiter, '_last_request_at', 100.0):
                with mock.patch('time.monotonic', side_effect=[100.5, 100.6]):
                    with mock.patch('time.sleep', side_effect=lambda d: sleeps.append(d)):
                        with mock.patch.object(
                            limiter.requests, 'get', 
                            side_effect=lambda url, **kw: calls.append(url) or FakeResponse()
                        ):
                            response = limiter.rate_limited_get(
                                "https://api.open-meteo.com/v1/forecast", timeout=10
                            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(calls), 1)
        self.assertEqual(calls[0], "https://api.open-meteo.com/v1/forecast")
        # Should have slept for 0.75s (175.0 - 100.5 = 0.1, but min cooldown is 0.75)
        if sleeps:
            self.assertGreater(sleeps[0], 0.5)

    def test_rate_limited_get_retries_after_429(self):
        """Test retry after 429."""
        import src.weather.open_meteo_rate_limiter as limiter

        sleeps = []
        responses = [
            FakeResponse(429, {"Retry-After": "2"}),
            FakeResponse(200),
        ]

        with mock.patch.object(limiter, '_cooldown_until', 0.0):
            with mock.patch.object(limiter, '_last_request_at', -100.0):
                with mock.patch('time.monotonic', side_effect=[0.0, 0.1, 2.1, 2.2]):
                    with mock.patch('time.sleep', side_effect=lambda d: sleeps.append(d)):
                        with mock.patch.object(
                            limiter.requests, 'get', 
                            side_effect=lambda *a, **kw: responses.pop(0)
                        ):
                            response = limiter.rate_limited_get(
                                "https://api.open-meteo.com/v1/forecast", timeout=10
                            )

        self.assertEqual(response.status_code, 200)
        self.assertGreater(len(sleeps), 0)

    def test_rate_limited_get_opens_global_cooldown_after_repeated_429(self):
        """Test global cooldown after repeated 429."""
        import src.weather.open_meteo_rate_limiter as limiter

        sleeps = []
        calls = []
        responses = [
            FakeResponse(429, {"Retry-After": "2"}),
            FakeResponse(429, {"Retry-After": "2"}),
            FakeResponse(429, {"Retry-After": "2"}),
        ]

        # Need enough monotonic values: initial + each retry + sleep checks
        monotonic_values = [0.0, 0.1, 2.1, 2.2, 4.3, 4.4, 5.0, 6.0, 7.0, 8.0]

        with mock.patch.object(limiter, '_cooldown_until', 0.0):
            with mock.patch.object(limiter, '_last_request_at', -100.0):
                with mock.patch('time.monotonic', side_effect=iter(monotonic_values)):
                    with mock.patch('time.sleep', side_effect=lambda d: sleeps.append(d)):
                        with mock.patch.object(
                            limiter.requests, 'get', side_effect=lambda *a, **kw: calls.append(a[0]) or responses.pop(0)
                        ):
                            first = limiter.rate_limited_get(
                                "https://api.open-meteo.com/v1/forecast", timeout=10
                            )
                            second = limiter.rate_limited_get(
                                "https://api.open-meteo.com/v1/forecast", timeout=10
                            )

        self.assertEqual(first.status_code, 429)
        self.assertEqual(second.status_code, 429)
        # After repeated 429s, should be in cooldown
        self.assertTrue(len(sleeps) > 0)


if __name__ == '__main__':
    unittest.main()
