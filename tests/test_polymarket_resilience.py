import json
import time

import requests

from src.trading import polymarket


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.ok = 200 <= status_code < 300

    def json(self):
        return self._payload


def reset_stale_registry(tmp_path, monkeypatch):
    stale_file = tmp_path / "stale_clob_tokens.json"
    monkeypatch.setattr(polymarket, "STALE_TOKEN_FILE", stale_file)
    monkeypatch.setattr(polymarket, "_stale_tokens", None)
    return stale_file


def test_clob_404_marks_token_stale_and_suppresses_retries(tmp_path, monkeypatch):
    stale_file = reset_stale_registry(tmp_path, monkeypatch)
    calls = []

    def fake_get(*args, **kwargs):
        calls.append((args, kwargs))
        return FakeResponse(404)

    monkeypatch.setattr(polymarket.requests, "get", fake_get)

    assert polymarket.get_orderbook("bad-token") is None
    assert polymarket.is_stale_token("bad-token")
    assert json.loads(stale_file.read_text(encoding="utf-8"))["bad-token"]

    assert polymarket.get_orderbook("bad-token") is None
    assert len(calls) == 1


def test_clob_transient_error_retries_with_backoff(tmp_path, monkeypatch):
    reset_stale_registry(tmp_path, monkeypatch)
    responses = [
        FakeResponse(503),
        FakeResponse(200, {"bids": [], "asks": []}),
    ]
    sleeps = []

    def fake_get(*args, **kwargs):
        return responses.pop(0)

    monkeypatch.setattr(polymarket.requests, "get", fake_get)
    monkeypatch.setattr(polymarket.time, "sleep", lambda delay: sleeps.append(delay))

    assert polymarket.get_orderbook("transient-token", max_attempts=3, backoff_s=0.25) == {"bids": [], "asks": []}
    assert sleeps == [0.25]


def test_clob_request_exception_retries_then_fails(tmp_path, monkeypatch):
    reset_stale_registry(tmp_path, monkeypatch)
    calls = 0

    def fake_get(*args, **kwargs):
        nonlocal calls
        calls += 1
        raise requests.Timeout("timeout")

    monkeypatch.setattr(polymarket.requests, "get", fake_get)
    monkeypatch.setattr(polymarket.time, "sleep", lambda delay: None)

    assert polymarket.get_orderbook("timeout-token", max_attempts=3, backoff_s=0.01) is None
    assert calls == 3


def test_stale_token_pruning_removes_expired_entries(tmp_path, monkeypatch):
    reset_stale_registry(tmp_path, monkeypatch)
    now = time.time()
    polymarket.mark_stale_token("old-token", now=now - polymarket.STALE_TOKEN_TTL_SECONDS - 1)
    polymarket.mark_stale_token("fresh-token", now=now)

    assert polymarket.prune_stale_tokens(now=now) == 1
    assert not polymarket.is_stale_token("old-token", now=now)
    assert polymarket.is_stale_token("fresh-token", now=now)
