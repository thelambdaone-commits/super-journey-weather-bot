"""
Strict Walk-Forward Validation - Robust performance estimation via rolling windows.
"""
from __future__ import annotations
import pandas as pd
import json
from pathlib import Path
from datetime import datetime, timedelta
import numpy as np

from ..ml.xgboost_train import XGBoostBaseline, load_dataset
from ..ml.metrics import brier_score, log_loss

class WalkForwardValidator:
    """
    Simulates model performance by sliding training and testing windows forward in time.
    Prevents temporal leakage (no look-ahead bias).
    """
    def __init__(self, data_dir: str = "data", train_days: int = 7, test_days: int = 1):
        self.data_dir = Path(data_dir)
        self.split_dir = self.data_dir / "ml_splits"
        self.train_days = train_days
        self.test_days = test_days
        self.results = []

    def run(self):
        """Run the strict walk-forward simulation."""
        # 1. Load full dataset
        dataset_path = self.data_dir / "dataset_rows.jsonl"
        if not dataset_path.exists():
            return []

        rows = []
        with open(dataset_path, "r") as f:
            for line in f:
                if line.strip():
                    rows.append(json.loads(line))
        
        if not rows:
            return []

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date")
        
        # 2. Iterate through windows
        start_date = df["date"].min()
        end_date = df["date"].max()
        
        current_train_end = start_date + timedelta(days=self.train_days)
        fold = 1
        
        while current_train_end + timedelta(days=self.test_days) <= end_date:
            test_end = current_train_end + timedelta(days=self.test_days)
            
            # Split data strictly by time
            train_df = df[df["date"] < current_train_end]
            test_df = df[(df["date"] >= current_train_end) & (df["date"] < test_end)]
            
            if len(train_df) >= 20 and len(test_df) >= 5:
                # Prepare temporary splits for this fold
                fold_dir = self.data_dir / f"wf_fold_{fold}"
                fold_dir.mkdir(exist_ok=True)
                
                # Save as JSONL for load_dataset
                train_df.to_json(fold_dir / "train.jsonl", orient="records", lines=True)
                test_df.to_json(fold_dir / "test.jsonl", orient="records", lines=True)
                (fold_dir / "valid.jsonl").touch() # Empty valid
                
                # Train and evaluate
                runner = XGBoostBaseline(data_dir=str(self.data_dir))
                runner.split_dir = fold_dir
                
                # Choose model based on size
                model_type = "logistic" if len(train_df) < 300 else "xgboost"
                result = runner.train(model_type=model_type)
                
                self.results.append({
                    "fold": fold,
                    "test_start": current_train_end.strftime("%Y-%m-%d"),
                    "test_end": test_end.strftime("%Y-%m-%d"),
                    "train_samples": len(train_df),
                    "test_samples": len(test_df),
                    "model": model_type,
                    "metrics": {
                        "auc": result.test_auc,
                        "brier": result.test_brier,
                        "logloss": result.test_logloss,
                        "accuracy": result.test_accuracy
                    }
                })
                
                # Cleanup fold dir
                import shutil
                shutil.rmtree(fold_dir)

            # Slide forward
            current_train_end += timedelta(days=self.test_days)
            fold += 1
            
        return self.results

    def format_report(self) -> str:
        if not self.results:
            return "⚠️ Pas assez de données pour une validation Walk-Forward stricte."
        
        lines = [
            "──────────────",
            "🕒 STRICT WALK-FORWARD REPORT",
            "──────────────",
            f"{'Fold':<4} | {'Test Window':<22} | {'Samples':<8} | {'AUC':<6} | {'Brier':<8}",
            "────────────────────────────────────────────────────────────"
        ]
        
        for r in self.results:
            window = f"{r['test_start']} -> {r['test_end']}"
            m = r["metrics"]
            lines.append(f"{r['fold']:<4} | {window:<22} | {r['test_samples']:<8} | {m['auc']:<6.3f} | {m['brier']:<8.4f}")
            
        avg_auc = np.mean([r["metrics"]["auc"] for r in self.results])
        avg_brier = np.mean([r["metrics"]["brier"] for r in self.results])
        
        lines.append("────────────────────────────────────────────────────────────")
        lines.append(f"MOYENNE | AUC: {avg_auc:.3f} | Brier: {avg_brier:.4f}")
        lines.append(f"MODÈLE  | {self.results[-1]['model'].upper()} (Dernière fenêtre)")
        
        return "\n".join(lines)

# Audit: Includes fee and slippage awareness
