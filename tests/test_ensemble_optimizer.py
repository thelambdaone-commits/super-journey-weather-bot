from src.weather.ensemble_optimizer import EnsembleOptimizer


def test_optimizer_combines_sources_with_bias_and_weights(tmp_path):
    model_path = tmp_path / "ml_model.json"
    model_path.write_text(
        """
{
  "by_city_source": {
    "london:ecmwf": {"n": 30, "mae": 0.8, "bias": 0.5},
    "london:gfs": {"n": 30, "mae": 1.6, "bias": -0.5}
  },
  "by_source": {}
}
""",
        encoding="utf-8",
    )

    optimizer = EnsembleOptimizer(str(tmp_path))
    result = optimizer.optimize("london", "C", {"ecmwf": 20.5, "gfs": 19.5})

    assert result is not None
    assert result.primary_source == "ecmwf"
    assert result.weights["ecmwf"] > result.weights["gfs"]
    assert 19.8 <= result.temp <= 20.2
    assert result.sigma >= 0.75
    assert 0.1 <= result.confidence <= 0.95


def test_optimizer_returns_none_without_sources(tmp_path):
    assert EnsembleOptimizer(str(tmp_path)).optimize("london", "C", {}) is None
