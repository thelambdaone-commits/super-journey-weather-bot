from src.weather import apis


class FakeResponse:
    status_code = 200

    def raise_for_status(self):
        return None

    def json(self):
        return {
            "properties": {
                "timeseries": [
                    {
                        "time": "2026-04-29T00:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 10.0}}},
                    },
                    {
                        "time": "2026-04-29T13:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 16.4}}},
                    },
                    {
                        "time": "2026-04-30T13:00:00Z",
                        "data": {"instant": {"details": {"air_temperature": 17.2}}},
                    },
                ]
            }
        }


def test_metno_forecast_returns_daily_max(monkeypatch):
    calls = []

    def fake_get(url, **kwargs):
        calls.append((url, kwargs))
        return FakeResponse()

    monkeypatch.setattr(apis.requests, "get", fake_get)

    result = apis.get_metno("london", ["2026-04-29", "2026-04-30"])

    assert result == {"2026-04-29": 16.4, "2026-04-30": 17.2}
    assert calls[0][0] == "https://api.met.no/weatherapi/locationforecast/2.0/compact"
    assert "User-Agent" in calls[0][1]["headers"]


def test_get_forecasts_uses_metno_when_open_meteo_sources_fail(monkeypatch):
    monkeypatch.setattr(apis, "get_ecmwf", lambda *args, **kwargs: {})
    monkeypatch.setattr(apis, "get_hrrr", lambda *args, **kwargs: {})
    monkeypatch.setattr(apis, "get_gfs", lambda *args, **kwargs: {})
    monkeypatch.setattr(apis, "get_dwd", lambda *args, **kwargs: {})
    monkeypatch.setattr(apis, "get_nws", lambda *args, **kwargs: {})
    monkeypatch.setattr(apis, "get_metar", lambda *args, **kwargs: None)
    monkeypatch.setattr(apis, "get_metno", lambda *args, **kwargs: {"2026-04-29": 16.4})

    snapshots = apis.get_forecasts("london", ["2026-04-29"])

    assert snapshots["2026-04-29"]["metno"] == 16.4
    assert snapshots["2026-04-29"]["best"] == 16.4
    assert snapshots["2026-04-29"]["best_source"] == "metno"


def test_get_actual_temp_skips_unavailable_meteostat_daily(monkeypatch, caplog):
    def failing_archive(*args, **kwargs):
        raise RuntimeError("archive unavailable")

    caplog.set_level("INFO", logger=apis.__name__)
    monkeypatch.setattr(apis, "_METEOSTAT_DAILY_AVAILABLE", None)
    monkeypatch.setattr(apis, "rate_limited_get", failing_archive)

    assert apis.get_actual_temp("london", "2026-04-29") is None
    assert "Meteostat Daily fallback unavailable" in caplog.text

    caplog.clear()
    assert apis.get_actual_temp("london", "2026-04-29") is None
    assert "Meteostat Daily fallback unavailable" not in caplog.text
