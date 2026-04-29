from dataclasses import dataclass

from src.storage import Market
from src.trading.execution import ClobExecutor
from src.trading.paper_account import PaperAccount
from src.trading.resolver import MarketResolver


class Config:
    pass


def test_clob_surebet_rolls_back_filled_legs(monkeypatch):
    executor = ClobExecutor(Config())
    executor.private_key = "test-key"
    executor._import_error = None
    buys = []
    closes = []

    def fake_buy(token_id, stake):
        buys.append((token_id, stake))
        if token_id == "bad":
            raise RuntimeError("second leg failed")
        return {"orderID": f"buy-{token_id}"}

    def fake_close(token_id, size):
        closes.append((token_id, size))
        return {"success": True, "order": f"sell-{token_id}"}

    monkeypatch.setattr(executor, "_place_fok_buy_market", fake_buy)
    monkeypatch.setattr(executor, "close_position_market", fake_close)

    result = executor.place_surebet_atomic([
        {"token_id": "good", "stake": 20.0, "ask": 0.2},
        {"token_id": "bad", "stake": 30.0, "ask": 0.3},
    ])

    assert not result["success"]
    assert buys == [("good", 20.0), ("bad", 30.0)]
    assert closes == [("good", 100.0)]
    assert result["rollback"][0]["result"]["success"]


def test_resolver_settles_paper_surebet_position(tmp_path):
    paper = PaperAccount(str(tmp_path))
    paper.record_trade(90.0)
    market = Market(
        city="paris",
        city_name="Paris",
        date="2026-04-29",
        actual_temp=15.0,
        paper_position={
            "type": "surebet",
            "market_id": "surebet:paris:2026-04-29",
            "cost": 90.0,
            "status": "open",
            "legs": [
                {"market_id": "cold", "bucket_low": -999.0, "bucket_high": 10.0, "shares": 100.0},
                {"market_id": "mild", "bucket_low": 11.0, "bucket_high": 20.0, "shares": 100.0},
                {"market_id": "hot", "bucket_low": 21.0, "bucket_high": 999.0, "shares": 100.0},
            ],
        },
    )

    @dataclass
    class Modes:
        live_trade: bool = False
        paper_mode: bool = True
        signal_mode: bool = False

    @dataclass
    class Engine:
        paper_account: PaperAccount
        modes: Modes

    balance, won, pnl = MarketResolver(Engine(paper, Modes())).resolve_market(market, balance=500.0)

    assert balance == 500.0
    assert won
    assert pnl == 9.1
    assert market.status == "resolved"
    assert market.resolved_outcome == "win"
    assert market.paper_position["status"] == "closed"
    assert market.paper_position["winning_market_id"] == "mild"
    assert paper.get_state().wins == 1
