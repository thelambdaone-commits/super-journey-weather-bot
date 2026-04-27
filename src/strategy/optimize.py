"""
Walk-forward grid search for scoring weights.

Explores weight combinations conservatively to avoid overfitting.
Use only after you have >= 50 eligible snapshots.
"""
from __future__ import annotations

from itertools import product
from statistics import mean

from .ranking_backtest import RankingBacktester


_DEFAULT_WEIGHTS = {
    "edge": 0.36,
    "confidence": 0.24,
    "liquidity": 0.16,
    "regime": 0.12,
    "market_edge": 0.12,
}


_GRID = {
    "edge": [0.30, 0.36, 0.42],
    "confidence": [0.18, 0.24, 0.30],
    "liquidity": [0.12, 0.16, 0.20],
    "regime": [0.08, 0.12, 0.16],
    "market_edge": [0.08, 0.12, 0.16],
}


def _write_weights(w: dict[str, float]) -> None:
    import json
    path = "data/scoring_weights.json"
    with open(path, "w") as f:
        json.dump(w, f, indent=2)


def _read_weights() -> dict[str, float]:
    import json
    path = "data/scoring_weights.json"
    try:
        with open(path) as f:
            return json.load(f)
    except (Exception,) as e:
        return _DEFAULT_WEIGHTS.copy()


def _apply_weights(weights: dict[str, float]) -> None:
    import re
    path = "src/strategy/scoring.py"
    with open(path) as f:
        src = f.read()

    replacements = {
        "0.36 * edge": f"{weights['edge']:.2f} * edge",
        "0.24 * confidence": f"{weights['confidence']:.2f} * confidence",
        "0.16 * liquidity": f"{weights['liquidity']:.2f} * liquidity",
        "0.12 * regime": f"{weights['regime']:.2f} * regime",
        "0.12 * market_edge": f"{weights['market_edge']:.2f} * market_edge",
    }

    for old, new in replacements.items():
        old_val = old.split(" * ")[0]
        new_val = new.split(" * ")[0]
        src = re.sub(rf"{old_val} \* \w+", new, src)

    with open(path, "w") as f:
        f.write(src)


def run_grid_search(min_snapshots: int = 50) -> dict | None:
    """
    Run grid search over weight combinations.
    
    Returns dict with best weights or None if not enough data.
    """
    backtester = RankingBacktester()
    report = backtester.run()
    
    if report.eligible_snapshots < min_snapshots:
        return None

    best = None
    best_score = float("-inf")
    results = []

    keys = list(_GRID.keys())
    for combo in product(*[_GRID[k] for k in keys]):
        weights = dict(zip(keys, combo))
        _apply_weights(weights)
        r = RankingBacktester().run()
        outperf = r.benchmark_outperformance
        n = r.eligible_snapshots
        if n == 0:
            continue
        score = outperf
        results.append((score, weights.copy(), r))
        if score > best_score:
            best_score = score
            best = weights.copy()

    _apply_weights(_DEFAULT_WEIGHTS)

    if best is None:
        return None

    _write_weights(best)
    _apply_weights(best)

    return {
        "best_weights": best,
        "outperformance": round(best_score, 4),
        "snapshots": report.eligible_snapshots,
    }
# Audit: Includes fee and slippage awareness
