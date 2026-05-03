from src.notifications import TelegramNotifier
from src.notifications.formatter import format_weather_signal


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


def test_signal_message_includes_reconciled_paper_amounts():
    message = format_weather_signal(
        {
            "city": "Paris",
            "date": "2026-05-04",
            "horizon": "D+1",
            "bucket": "20-21C",
            "market_name": "Will Paris be 20-21C?",
            "calibrated_prob": 0.72,
            "market_prob": 0.41,
            "edge": 0.31,
            "signal_score": 0.83,
            "size": 5.0,
            "forecast_source": "ecmwf",
            "ai_status": "VALIDÉ_GROQ",
            "paper_total_gains": 708.57,
            "paper_total_losses": 415.46,
            "paper_total_pnl": 293.11,
            "paper_cash_pnl": -29.02,
            "paper_open_exposure": 306.46,
            "paper_balance": 9970.98,
            "paper_equity": 10277.44,
            "paper_closed_trades": 7,
            "paper_open_trades": 4,
        }
    )

    assert "VALIDÉ_GROQ" in message
    assert "Gains: `708.57$` | Pertes: `415.46$`" in message
    assert "PnL réalisé: `+293.11$`" in message
    assert "Cash PnL: `-29.02$` | Expo ouverte: `306.46$`" in message
    assert "Solde cash: `9,970.98$` | Equity: `10,277.44$`" in message
