from src.weather import open_meteo_rate_limiter as limiter


class FakeResponse:
    def __init__(self, status_code=200, headers=None):
        self.status_code = status_code
        self.headers = headers or {}
        self.ok = 200 <= status_code < 300


def test_rate_limited_get_waits_between_requests(monkeypatch):
    sleeps = []
    calls = []
    monotonic_values = iter([100.5, 100.6])

    monkeypatch.setattr(limiter, "_cooldown_until", 0.0)
    monkeypatch.setattr(limiter, "_last_request_at", 100.0)
    monkeypatch.setattr(limiter.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(limiter.time, "sleep", lambda delay: sleeps.append(delay))
    monkeypatch.setattr(
        limiter.requests,
        "get",
        lambda url, **kwargs: calls.append((url, kwargs)) or FakeResponse(),
    )

    response = limiter.rate_limited_get("https://api.open-meteo.com/v1/forecast", timeout=10)

    assert response.status_code == 200
    assert calls == [("https://api.open-meteo.com/v1/forecast", {"timeout": 10})]
    assert sleeps == [0.75]


def test_rate_limited_get_retries_after_429(monkeypatch):
    sleeps = []
    responses = [
        FakeResponse(429, {"Retry-After": "2"}),
        FakeResponse(200),
    ]
    monotonic_values = iter([0.0, 0.1, 2.1, 2.2])

    monkeypatch.setattr(limiter, "_cooldown_until", 0.0)
    monkeypatch.setattr(limiter, "_last_request_at", -100.0)
    monkeypatch.setattr(limiter.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(limiter.time, "sleep", lambda delay: sleeps.append(delay))
    monkeypatch.setattr(limiter.requests, "get", lambda *args, **kwargs: responses.pop(0))

    response = limiter.rate_limited_get("https://api.open-meteo.com/v1/forecast", timeout=10)

    assert response.status_code == 200
    assert sleeps == [2.0]


def test_rate_limited_get_opens_global_cooldown_after_repeated_429(monkeypatch):
    sleeps = []
    calls = []
    responses = [
        FakeResponse(429, {"Retry-After": "2"}),
        FakeResponse(429, {"Retry-After": "2"}),
        FakeResponse(429, {"Retry-After": "2"}),
    ]
    monotonic_values = iter([0.0, 0.1, 2.1, 2.2, 4.3, 4.4, 4.5, 5.0])

    monkeypatch.setattr(limiter, "_cooldown_until", 0.0)
    monkeypatch.setattr(limiter, "_last_request_at", -100.0)
    monkeypatch.setattr(limiter.time, "monotonic", lambda: next(monotonic_values))
    monkeypatch.setattr(limiter.time, "sleep", lambda delay: sleeps.append(delay))
    monkeypatch.setattr(limiter.requests, "get", lambda *args, **kwargs: calls.append(args[0]) or responses.pop(0))

    first = limiter.rate_limited_get("https://api.open-meteo.com/v1/forecast", timeout=10)
    second = limiter.rate_limited_get("https://api.open-meteo.com/v1/forecast", timeout=10)

    assert first.status_code == 429
    assert second.status_code == 429
    assert second.url == "open-meteo://cooldown"
    assert calls == [
        "https://api.open-meteo.com/v1/forecast",
        "https://api.open-meteo.com/v1/forecast",
        "https://api.open-meteo.com/v1/forecast",
    ]
    assert sleeps == [2.0, 2.0]
