from src.strategy.surebet import detect_surebet, has_exhaustive_temperature_coverage


def outcome(market_id, low, high, ask, size=1000):
    return {
        "market_id": market_id,
        "token_id": f"token-{market_id}",
        "range": (low, high),
        "bid": max(0.01, ask - 0.01),
        "ask": ask,
        "best_ask_size": size,
    }


def test_detects_exhaustive_profitable_surebet():
    outcomes = [
        outcome("cold", -999, 10, 0.20),
        outcome("mild", 11, 20, 0.30),
        outcome("hot", 21, 999, 0.40),
    ]

    surebet = detect_surebet(outcomes, max_total_stake=90, min_profit_pct=0.03)

    assert surebet is not None
    assert surebet.implied_sum == 0.9
    assert surebet.total_cost == 90.0
    assert surebet.guaranteed_payout == 100.0
    assert surebet.guaranteed_profit == 10.0
    assert len(surebet.legs) == 3


def test_rejects_non_exhaustive_buckets():
    outcomes = [
        outcome("cold", -999, 10, 0.20),
        outcome("hot", 21, 999, 0.40),
    ]

    assert not has_exhaustive_temperature_coverage(outcomes)
    assert detect_surebet(outcomes, max_total_stake=90) is None


def test_rejects_unprofitable_or_illiquid_surebet():
    unprofitable = [
        outcome("cold", -999, 10, 0.35),
        outcome("mild", 11, 20, 0.35),
        outcome("hot", 21, 999, 0.35),
    ]
    illiquid = [
        outcome("cold", -999, 10, 0.20, size=1),
        outcome("mild", 11, 20, 0.30, size=1),
        outcome("hot", 21, 999, 0.40, size=1),
    ]

    assert detect_surebet(unprofitable, max_total_stake=90) is None
    assert detect_surebet(illiquid, max_total_stake=90, min_liquidity_usd=10) is None
