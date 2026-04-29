from src.notifications import TelegramNotifier


class FakeResponse:
    def __init__(self, ok, status_code, text, payload=None):
        self.ok = ok
        self.status_code = status_code
        self.text = text
        self._payload = payload or {}

    def json(self):
        return self._payload


def test_send_retries_without_markdown_when_telegram_parse_fails(monkeypatch):
    calls = []
    responses = [
        FakeResponse(False, 400, "Bad Request: can't parse entities"),
        FakeResponse(True, 200, "OK", {"ok": True, "result": {"message_id": 42}}),
    ]

    def fake_post(url, json, timeout):
        calls.append(json)
        return responses.pop(0)

    class Idempotence:
        def is_duplicate(self, *args, **kwargs):
            return False

    monkeypatch.setattr("src.notifications.requests.post", fake_post)
    monkeypatch.setattr("src.trading.idempotence.get_idempotence_manager", lambda: Idempotence())

    notifier = TelegramNotifier(token="token", chat_id="chat")
    message_id = notifier.send("dynamic _ markdown", parse_mode="Markdown")

    assert message_id == 42
    assert calls[0]["parse_mode"] == "Markdown"
    assert "parse_mode" not in calls[1]
