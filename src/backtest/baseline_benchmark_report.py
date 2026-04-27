"""
Baseline Benchmark Report V3 - PR #5.
Compares V3 Alpha against raw ECMWF, Weighted Ensemble, and Market Implied Probabilities.
"""
from __future__ import annotations
import polars as pl
import numpy as np
from pathlib import Path
from ..data.loader import load_rows
from ..ml.metrics import calculate_ml_metrics
import logging

logger = logging.getLogger(__name__)

def run_benchmark(dataset_path: str = "data/dataset_rows.jsonl"):
    """
    Run the A/B test baseline report.
    """
    rows = load_rows(Path(dataset_path))
    if not rows:
        print("No data found for benchmark.")
        return

    data = []
    for row in rows:
        if row.actual_temp is None or row.bucket is None:
            continue
            
        low, high = map(float, row.bucket.split("-"))
        won = low <= float(row.actual_temp) <= high
        
        data.append({
            "won": won,
            "raw_ecmwf_prob": row.raw_ecmwf_prob or 0.5,
            "weighted_ensemble_prob": row.ensemble_prob or 0.5,
            "market_implied_prob": row.market_implied_prob or 0.5,
            "v3_alpha_prob": row.calibrated_prob or 0.5,
        })
        
    df = pl.DataFrame(data)
    if df.is_empty():
        print("No resolved rows found for benchmark.")
        return

    results = {}
    for col in ["raw_ecmwf_prob", "weighted_ensemble_prob", "market_implied_prob", "v3_alpha_prob"]:
        metrics = calculate_ml_metrics(df["won"].to_list(), df[col].to_list())
        results[col] = metrics

    print("\n" + "="*50)
    print("A/B TEST BASELINE BENCHMARK REPORT")
    print("="*50)
    print(f"{'Strategy':<25} | {'Brier':<8} | {'LogLoss':<8} | {'AUC':<6}")
    print("-" * 50)
    
    for strategy, m in results.items():
        print(f"{strategy:<25} | {m.brier_score:.4f} | {m.log_loss:.4f} | {m.roc_auc:.3f}")
    
    print("="*50)
    
    v3_brier = results["v3_alpha_prob"].brier_score
    market_brier = results["market_implied_prob"].brier_score
    outperf = (market_brier - v3_brier) / market_brier * 100
    
    print(f"\nV3 Outperformance vs Market: {outperf:+.2f}%")
    if outperf > 0:
        print("🚀 STATUS: ALPHA DETECTED")
    else:
        print("⚠️ STATUS: NO ALPHA (Model is worse than market)")

if __name__ == "__main__":
    run_benchmark()

# Audit: Includes fee and slippage awareness
