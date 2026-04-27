"""
XGBoost Hyperparameter Tuning - Open Source Only.
Optimisation niveau "top 1%" avec safety gates.
"""
from __future__ import annotations

import json
import time
import random
import hashlib
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import numpy as np
from sklearn.model_selection import StratifiedKFold
from sklearn.metrics import roc_auc_score, brier_score_loss, log_loss


PARAM_GRID = {
    "max_depth": [3, 4, 5, 6],
    "learning_rate": [0.01, 0.05, 0.1, 0.15],
    "n_estimators": [50, 100, 150],
    "min_child_weight": [1, 3, 5],
    "subsample": [0.7, 0.8, 0.9],
    "colsample_bytree": [0.7, 0.8, 0.9],
}

RANDOM_SEARCH_SPACE = {
    "max_depth": [3, 4, 5, 6],
    "learning_rate": [0.01, 0.03, 0.05, 0.08, 0.1, 0.12, 0.15],
    "n_estimators": [50, 75, 100, 125, 150, 175, 200],
    "min_child_weight": [1, 2, 3, 4, 5],
    "subsample": [0.65, 0.7, 0.75, 0.8, 0.85, 0.9],
    "colsample_bytree": [0.65, 0.7, 0.75, 0.8, 0.85, 0.9],
    "gamma": [0, 0.1, 0.2, 0.3],
    "reg_alpha": [0, 0.01, 0.1],
    "reg_lambda": [1, 1.5, 2],
}

DEFAULT_PARAMS = {
    "objective": "binary:logistic",
    "max_depth": 4,
    "learning_rate": 0.1,
    "n_estimators": 100,
    "min_child_weight": 3,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "eval_metric": "auc",
    "early_stopping_rounds": 20,
    "verbosity": 0,
}


@dataclass
class TuningResult:
    """Result of hyperparameter tuning."""
    best_params: dict
    cv_auc: float
    cv_brier: float
    cv_logloss: float
    test_auc: float
    test_brier: float
    baseline_auc: float
    baseline_brier: float
    improvement_auc: float
    improvement_brier: float
    search_type: str
    n_trials: int
    duration_seconds: float
    accepted: bool
    accept_reason: str
    timestamp: str


@dataclass
class TuningTrial:
    """Single tuning trial."""
    params: dict
    train_auc: float
    valid_auc: float
    test_auc: float
    train_brier: float
    valid_brier: float
    test_brier: float
    timestamp: str


def _load_dataset(split_dir: Path) -> tuple:
    """Load train/valid/test splits."""
    from src.ml.xgboost_train import load_dataset
    
    train_rows, X_train, y_train = load_dataset(split_dir, "train")
    valid_rows, X_valid, y_valid = load_dataset(split_dir, "valid")
    test_rows, X_test, y_test = load_dataset(split_dir, "test")
    
    if len(X_train) == 0:
        return None, None, None, None, None, None
    
    return X_train, y_train, X_valid, y_valid, X_test, y_test


def _train_xgboost(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
    params: dict,
) -> tuple[dict, dict]:
    """Train XGBoost and return metrics."""
    try:
        import xgboost as xgb
        has_xgb = True
    except ImportError:
        from sklearn.ensemble import GradientBoostingClassifier
        has_xgb = False
    
    if has_xgb:
        model_params = {**DEFAULT_PARAMS, **params}
        model_params.pop("eval_metric", None)
        model_params.pop("early_stopping_rounds", None)
        
        model = xgb.XGBClassifier(**model_params)
        eval_set = [(X_valid, y_valid)] if len(X_valid) > 0 else None
        
        model.fit(
            X_train, y_train,
            eval_set=eval_set,
            verbose=False,
        )
    else:
        from sklearn.ensemble import GradientBoostingClassifier
        model = GradientBoostingClassifier(
            max_depth=params.get("max_depth", 4),
            learning_rate=params.get("learning_rate", 0.1),
            n_estimators=params.get("n_estimators", 100),
        )
        model.fit(X_train, y_train)
    
    train_pred = model.predict_proba(X_train)[:, 1]
    valid_pred = model.predict_proba(X_valid)[:, 1] if len(X_valid) > 0 else np.array([])
    test_pred = model.predict_proba(X_test)[:, 1] if len(X_test) > 0 else np.array([])
    
    metrics = {
        "train_auc": roc_auc_score(y_train, train_pred) if len(np.unique(y_train)) > 1 else 0.5,
        "valid_auc": roc_auc_score(y_valid, valid_pred) if len(np.unique(y_valid)) > 1 else 0.5,
        "test_auc": roc_auc_score(y_test, test_pred) if len(np.unique(y_test)) > 1 else 0.5,
        "train_brier": brier_score_loss(y_train, train_pred),
        "valid_brier": brier_score_loss(y_valid, valid_pred) if len(valid_pred) > 0 else 0.25,
        "test_brier": brier_score_loss(y_test, test_pred) if len(test_pred) > 0 else 0.25,
    }
    
    return model, metrics


def _get_baseline_metrics(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_valid: np.ndarray,
    y_valid: np.ndarray,
    X_test: np.ndarray,
    y_test: np.ndarray,
) -> dict:
    """Get baseline metrics with default params."""
    _, metrics = _train_xgboost(
        X_train, y_train,
        X_valid, y_valid,
        X_test, y_test,
        DEFAULT_PARAMS,
    )
    return metrics


def _grid_search_configs(grid: dict, max_trials: int = 32) -> list[dict]:
    """Generate random configs from grid (not full cartesian)."""
    keys = list(grid.keys())
    configs = []
    
    for _ in range(max_trials):
        config = {}
        for key in keys:
            config[key] = random.choice(grid[key])
        configs.append(config)
    
    return configs


def _random_search_configs(space: dict, n_trials: int = 32) -> list[dict]:
    """Generate random configs from search space."""
    keys = list(space.keys())
    configs = []
    
    for _ in range(n_trials):
        config = {}
        for key in keys:
            values = space[key]
            config[key] = values[random.randint(0, len(values) - 1)]
        configs.append(config)
    
    return configs


def _compute_cv_score(
    X: np.ndarray,
    y: np.ndarray,
    params: dict,
    n_folds: int = 5,
) -> float:
    """Compute cross-validation AUC."""
    if len(np.unique(y)) < 2 or len(X) < n_folds * 2:
        return 0.5
    
    kf = StratifiedKFold(n_splits=n_folds, shuffle=True, random_state=42)
    aucs = []
    
    for train_idx, val_idx in kf.split(X, y):
        X_tr, X_val = X[train_idx], X[val_idx]
        y_tr, y_val = y[train_idx], y[val_idx]
        
        try:
            import xgboost as xgb
            model = xgb.XGBClassifier(**{**DEFAULT_PARAMS, **params}, verbosity=0)
            model.fit(X_tr, y_tr, verbose=False)
            
            pred = model.predict_proba(X_val)[:, 1]
            if len(np.unique(y_val)) > 1:
                aucs.append(roc_auc_score(y_val, pred))
        except (Exception,) as e:
            pass
    
    return np.mean(aucs) if aucs else 0.5


def _save_tuning_history(result: TuningResult) -> None:
    """Save tuning result to history."""
    history_path = Path("data/tuning_history.jsonl")
    
    record = {
        "timestamp": result.timestamp,
        "search_type": result.search_type,
        "n_trials": result.n_trials,
        "cv_auc": result.cv_auc,
        "cv_brier": result.cv_brier,
        "test_auc": result.test_auc,
        "test_brier": result.test_brier,
        "baseline_auc": result.baseline_auc,
        "baseline_brier": result.baseline_brier,
        "improvement_auc": result.improvement_auc,
        "improvement_brier": result.improvement_brier,
        "accepted": result.accepted,
        "accept_reason": result.accept_reason,
        "best_params": result.best_params,
    }
    
    with open(history_path, "a") as f:
        f.write(json.dumps(record) + "\n")


def _load_tuning_history(limit: int = 10) -> list[dict]:
    """Load recent tuning history."""
    history_path = Path("data/tuning_history.jsonl")
    
    if not history_path.exists():
        return []
    
    records = []
    with open(history_path) as f:
        for line in f:
            try:
                records.append(json.loads(line))
            except (Exception,) as e:
                pass
    
    return records[-limit:]


def run_tuning(
    data_dir: str = "data",
    search_type: str = "random",
    max_trials: int = 32,
    timeout: int = 300,
    min_improvement: float = 0.01,
) -> TuningResult:
    """
    Run hyperparameter tuning.
    
    Args:
        data_dir: Path to data directory
        search_type: "grid" or "random"
        max_trials: Maximum trials (default 32)
        timeout: Timeout in seconds
        min_improvement: Minimum improvement to accept
    
    Returns:
        TuningResult with best params and metrics
    """
    split_dir = Path(data_dir) / "ml_splits"
    
    X_train, y_train, X_valid, y_valid, X_test, y_test = _load_dataset(split_dir)
    
    if X_train is None:
        return TuningResult(
            best_params=DEFAULT_PARAMS,
            cv_auc=0.0, cv_brier=0.25, cv_logloss=0.0,
            test_auc=0.0, test_brier=0.25,
            baseline_auc=0.0, baseline_brier=0.25,
            improvement_auc=0.0, improvement_brier=0.0,
            search_type=search_type, n_trials=0,
            duration_seconds=0.0,
            accepted=False,
            accept_reason="Insufficient data",
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    
    rejection_reasons = []
    
    if len(X_train) < 100:
        rejection_reasons.append(f"train={len(X_train)}<100")
    if len(X_valid) < 30:
        rejection_reasons.append(f"valid={len(X_valid)}<30")
    if len(X_test) < 30:
        rejection_reasons.append(f"test={len(X_test)}<30")
    
    train_classes = set(y_train)
    valid_classes = set(y_valid)
    test_classes = set(y_test)
    if len(train_classes) < 2:
        rejection_reasons.append(f"train_missing_class")
    if len(valid_classes) < 2:
        rejection_reasons.append(f"valid_missing_class")
    if len(test_classes) < 2:
        rejection_reasons.append(f"test_missing_class")
    
    baseline = _get_baseline_metrics(X_train, y_train, X_valid, y_valid, X_test, y_test)
    
    if baseline["test_auc"] >= 0.999 or baseline["test_brier"] <= 0.001:
        if len(X_test) < 50:
            rejection_reasons.append(f"suspicious_perfect_score_AUC={baseline['test_auc']}_Brier={baseline['test_brier']}")
    
    if rejection_reasons:
        return TuningResult(
            best_params=DEFAULT_PARAMS,
            cv_auc=0.0, cv_brier=0.25, cv_logloss=0.0,
            test_auc=baseline["test_auc"], test_brier=baseline["test_brier"],
            baseline_auc=baseline["test_auc"], baseline_brier=baseline["test_brier"],
            improvement_auc=0.0, improvement_brier=0.0,
            search_type=search_type, n_trials=0,
            duration_seconds=0.0,
            accepted=False,
            accept_reason=f"REJECTED: {'; '.join(rejection_reasons)}",
            timestamp=datetime.now(timezone.utc).isoformat(),
        )
    
    start_time = time.time()
    ts = datetime.now(timezone.utc).isoformat()
    
    baseline = _get_baseline_metrics(X_train, y_train, X_valid, y_valid, X_test, y_test)
    
    if search_type == "grid":
        configs = _grid_search_configs(PARAM_GRID, max_trials)
    else:
        configs = _random_search_configs(RANDOM_SEARCH_SPACE, max_trials)
    
    best_params = DEFAULT_PARAMS.copy()
    best_metrics = baseline.copy()
    best_cv = 0.5
    n_evaluated = 0
    
    for config in configs:
        if time.time() - start_time > timeout:
            break
        
        try:
            _, metrics = _train_xgboost(
                X_train, y_train,
                X_valid, y_valid,
                X_test, y_test,
                config,
            )
            
            cv_score = _compute_cv_score(
                np.vstack([X_train, X_valid]),
                np.concatenate([y_train, y_valid]),
                config,
            )
            
            if cv_score > best_cv:
                best_cv = cv_score
                best_params = config.copy()
                best_metrics = metrics.copy()
            
            n_evaluated += 1
        except (Exception,) as e:
            continue
    
    final_model, final_metrics = _train_xgboost(
        X_train, y_train,
        X_valid, y_valid,
        X_test, y_test,
        best_params,
    )
    
    improvement_auc = final_metrics["test_auc"] - baseline["test_auc"]
    improvement_brier = baseline["test_brier"] - final_metrics["test_brier"]
    
    accepted = (
        improvement_auc >= min_improvement or
        improvement_brier >= min_improvement
    )
    
    if accepted:
        accept_reason = f"improvement: AUC {improvement_auc:+.4f}, Brier {improvement_brier:+.4f}"
    else:
        accept_reason = f"no improvement (min: {min_improvement})"
    
    result = TuningResult(
        best_params=best_params,
        cv_auc=best_cv,
        cv_brier=final_metrics.get("valid_brier", 0.25),
        cv_logloss=0.0,
        test_auc=final_metrics["test_auc"],
        test_brier=final_metrics["test_brier"],
        baseline_auc=baseline["test_auc"],
        baseline_brier=baseline["test_brier"],
        improvement_auc=improvement_auc,
        improvement_brier=improvement_brier,
        search_type=search_type,
        n_trials=n_evaluated,
        duration_seconds=time.time() - start_time,
        accepted=accepted,
        accept_reason=accept_reason,
        timestamp=ts,
    )
    
    _save_tuning_history(result)
    
    return result


def get_best_params() -> dict:
    """Get last best params or defaults."""
    history = _load_tuning_history(limit=1)
    
    if history and history[0].get("accepted"):
        return history[0].get("best_params", DEFAULT_PARAMS)
    
    return DEFAULT_PARAMS.copy()


def format_tuning_report(result: TuningResult) -> list[str]:
    """Format tuning result as CLI report."""
    lines = [
        "",
        "=" * 50,
        "HYPERPARAMETER TUNING REPORT",
        "=" * 50,
        f"Search type: {result.search_type}",
        f"Trials: {result.n_trials}",
        f"Duration: {result.duration_seconds:.1f}s",
        "",
        f"CV AUC: {result.cv_auc:.4f}",
        f"Test AUC: {result.test_auc:.4f} (baseline: {result.baseline_auc:.4f})",
        f"Test Brier: {result.test_brier:.4f} (baseline: {result.baseline_brier:.4f})",
        "",
        f"Improvement AUC: {result.improvement_auc:+.4f}",
        f"Improvement Brier: {result.improvement_brier:+.4f}",
        "",
        f"Status: {'✅ ACCEPTED' if result.accepted else '❌ REJECTED'}",
        f"Reason: {result.accept_reason}",
        "",
        "Best params:",
    ]
    
    for k, v in result.best_params.items():
        if k in PARAM_GRID or k in RANDOM_SEARCH_SPACE:
            lines.append(f"  {k}: {v}")
    
    lines.append("=" * 50)
    
    return lines