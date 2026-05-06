from unittest import mock

from src.strategy.signal_quality import Signal, SignalQualityLayer
from src.weather.config import Config


class FakeFairValueEngine:
    def __init__(self):
        self.calls = []

    def calculate_fair_value(self, city, threshold, target_time, side="ABOVE"):
        self.calls.append((city, threshold, side))
        return 0.60

    def get_vwap_edge(self, fair_value, vwap_ask, spread):
        return 0.0


def test_fair_value_alpha_uses_bucket_threshold_not_market_price():
    fake = FakeFairValueEngine()
    with mock.patch("src.strategy.signal_quality.get_fair_value_engine", return_value=fake):
        layer = SignalQualityLayer(Config())
    signal = Signal.from_dict(
        "chicago",
        {
            "market_id": "m1",
            "entry_price": 0.68,
            "ev": 0.08,
            "bucket_low": 64.0,
            "bucket_high": 999.0,
            "unit": "F",
            "ml": {"confidence": 0.8, "mae": 1.0},
        },
    )

    with mock.patch("src.strategy.signal_quality.is_enabled", side_effect=lambda name: name == "V3_FAIR_VALUE"):
        layer.compute_quality(signal)

    assert fake.calls
    _, threshold, side = fake.calls[0]
    assert side == "ABOVE"
    assert round(threshold, 4) == 17.7778
    assert threshold != signal.price


def test_fair_value_alpha_is_skipped_without_bucket_boundaries():
    fake = FakeFairValueEngine()
    with mock.patch("src.strategy.signal_quality.get_fair_value_engine", return_value=fake):
        layer = SignalQualityLayer(Config())
    signal = Signal.from_dict(
        "chicago",
        {
            "market_id": "m1",
            "entry_price": 0.68,
            "ev": 0.08,
            "ml": {"confidence": 0.8, "mae": 1.0},
        },
    )

    with mock.patch("src.strategy.signal_quality.is_enabled", side_effect=lambda name: name == "V3_FAIR_VALUE"):
        layer.compute_quality(signal)

    assert fake.calls == []
