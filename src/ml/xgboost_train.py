"""
XGBoost Baseline: First ML experiment on weather market data.
Purpose: Discover if there's exploitable signal, not optimize trading.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import numpy as np


FEATURE_COLS = [
    "forecast_temp",
    "ecmwf_max",
    "hrrr_max",
    "ensemble_mean",
    "ensemble_std",
    "forecast_spread",
    "model_confidence_score",
    "city_source_mae",
    "city_source_bias",
    "market_price",
    "market_implied_prob",
    "spread",
    "volume",
    "hours_to_resolution",
    "day_of_year",
    "forecast_market_gap",
    "latitude",
    "longitude",
    "regime_confidence",
    "source_disagreement",
]

TARGET_COL = "target_win"


@dataclass
class FeatureImportance:
    feature: str
    importance: float
    gain: float


@dataclass
class TrainingResult:
    train_accuracy: float
    train_auc: float
    valid_accuracy: float
    valid_auc: float
    test_accuracy: float
    test_auc: float
    feature_importances: list[FeatureImportance]
    signal_detected: bool
    signal_strength: str
    top_features: list[str]
    notes: list[str]


def extract_features(row: dict) -> dict[str, float]:
    """Extract numerical features from a dataset row."""
    features = {}

    features["forecast_temp"] = row.get("forecast_temp") or 0
    features["ecmwf_max"] = row.get("ecmwf_max") or 0
    features["hrrr_max"] = row.get("hrrr_max") or 0
    features["ensemble_mean"] = row.get("ensemble_mean") or 0
    features["ensemble_std"] = row.get("ensemble_std") or 0
    features["forecast_spread"] = row.get("forecast_spread") or 0

    features["model_confidence_score"] = row.get("model_confidence_score") or 0.5
    features["city_source_mae"] = row.get("city_source_mae") or 2.0
    features["city_source_bias"] = row.get("city_source_bias") or 0

    features["market_price"] = row.get("market_price") or 0.5
    features["market_implied_prob"] = row.get("market_implied_prob") or 0.5
    features["spread"] = row.get("spread") or 0
    features["volume"] = row.get("volume") or 0
    features["hours_to_resolution"] = row.get("hours_to_resolution") or 24
    features["day_of_year"] = row.get("day_of_year") or 0

    gap = row.get("forecast_market_gap")
    features["forecast_market_gap"] = gap if gap is not None else 0

    features["latitude"] = row.get("lat") or row.get("latitude") or 0
    features["longitude"] = row.get("lon") or row.get("longitude") or 0

    features["regime_confidence"] = row.get("regime_confidence") or 0.5
    features["source_disagreement"] = row.get("source_disagreement") or 0

    return features


def compute_target(row: dict) -> float:
    """Compute target variable: 1 if won, 0 if lost."""
    if row.get("resolution_outcome") == "win":
        return 1.0
    if row.get("resolution_outcome") == "loss":
        return 0.0

    actual_temp = row.get("actual_temp")
    bucket_low = row.get("bucket_low")
    bucket_high = row.get("bucket_high")

    if actual_temp is not None:
        if bucket_low is not None and bucket_high is not None:
            return 1.0 if bucket_low <= actual_temp <= bucket_high else 0.0
        
        forecast_temp = row.get("forecast_temp")
        if forecast_temp is not None:
            unit = row.get("unit", "C")
            bucket_width = 2 if unit == "F" else 1
            expected_bucket_low = round(forecast_temp / bucket_width) * bucket_width
            expected_bucket_high = expected_bucket_low + bucket_width
            
            distance = abs(actual_temp - forecast_temp)
            if distance <= bucket_width:
                return 1.0
            else:
                return 0.0

    return 0.5


def load_dataset(split_dir: Path, split_name: str = "train") -> tuple[list[dict], np.ndarray, np.ndarray]:
    """Load dataset split and extract features/targets."""
    path = split_dir / f"{split_name}.jsonl"
    if not path.exists():
        return [], np.array([]), np.array([])

    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))

    if not rows:
        return rows, np.array([]), np.array([])

    X = []
    y = []
    weights = []

    for row in rows:
        actual_temp = row.get("actual_temp")
        if actual_temp is None and row.get("resolution_outcome") is None:
            continue

        features = extract_features(row)
        target = compute_target(row)

        if target == 0.5:
            continue

        X.append([features.get(f, 0) for f in FEATURE_COLS])
        y.append(target)
        weights.append(row.get("_sample_weight", 1.0))

    return rows, np.array(X), np.array(y)


def compute_accuracy(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Compute classification accuracy."""
    if len(y_true) == 0:
        return 0.0
    return float(np.mean((y_pred > 0.5) == y_true))


def compute_auc(y_true: np.ndarray, y_pred: np.ndarray) -> float:
    """Compute AUC-ROC."""
    if len(y_true) < 2 or len(np.unique(y_true)) < 2:
        return 0.5

    from sklearn.metrics import roc_auc_score
    try:
        return roc_auc_score(y_true, y_pred)
    except Exception:
        return 0.5


def compute_calibration(y_true: np.ndarray, y_pred: np.ndarray, n_bins: int = 10) -> dict[str, float]:
    """Compute calibration metrics."""
    if len(y_true) < n_bins:
        return {"ece": 0.0, "mce": 0.0}

    bins = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    mce = 0.0

    for i in range(n_bins):
        mask = (y_pred >= bins[i]) & (y_pred < bins[i + 1])
        if i == n_bins - 1:
            mask = (y_pred >= bins[i]) & (y_pred <= bins[i + 1])

        if np.sum(mask) == 0:
            continue

        bin_mean_pred = np.mean(y_pred[mask])
        bin_mean_true = np.mean(y_true[mask])
        bin_weight = np.sum(mask) / len(y_true)

        ece += bin_weight * abs(bin_mean_pred - bin_mean_true)
        mce = max(mce, abs(bin_mean_pred - bin_mean_true))

    return {"ece": float(ece), "mce": float(mce)}


class XGBoostBaseline:
    """XGBoost baseline experiment."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.split_dir = self.data_dir / "ml_splits"

    def train(self, params: Optional[dict] = None) -> TrainingResult:
        """Train XGBoost baseline model."""

        try:
            import xgboost as xgb
        except ImportError:
            try:
                from sklearn.ensemble import GradientBoostingClassifier
                has_xgb = False
            except ImportError:
                return self._sklearn_fallback(params)
        else:
            has_xgb = True

        _, X_train, y_train = load_dataset(self.split_dir, "train")
        _, X_valid, y_valid = load_dataset(self.split_dir, "valid")
        _, X_test, y_test = load_dataset(self.split_dir, "test")

        if len(X_train) < 10:
            return self._insufficient_data_result()

        if has_xgb:
            return self._train_xgboost(X_train, y_train, X_valid, y_valid, X_test, y_test, params)
        else:
            return self._train_sklearn(X_train, y_train, X_valid, y_valid, X_test, y_test, params)

    def _train_xgboost(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_valid: np.ndarray,
        y_valid: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        params: Optional[dict] = None,
    ) -> TrainingResult:
        """Train using XGBoost."""
        import xgboost as xgb

        default_params = {
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
        if params:
            default_params.update(params)

        model = xgb.XGBClassifier(**default_params)

        eval_set = [(X_valid, y_valid)] if len(X_valid) > 0 else None

        model.fit(
            X_train, y_train,
            eval_set=eval_set,
            verbose=False,
        )

        train_pred = model.predict_proba(X_train)[:, 1]
        valid_pred = model.predict_proba(X_valid)[:, 1] if len(X_valid) > 0 else np.array([])
        test_pred = model.predict_proba(X_test)[:, 1] if len(X_test) > 0 else np.array([])

        feature_importances = []
        if hasattr(model, "feature_importances_"):
            for f, imp in zip(FEATURE_COLS, model.feature_importances_):
                feature_importances.append(FeatureImportance(feature=f, importance=float(imp), gain=0))

        feature_importances.sort(key=lambda x: x.importance, reverse=True)

        return self._build_result(
            y_train, train_pred,
            y_valid, valid_pred,
            y_test, test_pred,
            feature_importances,
        )

    def _train_sklearn(
        self,
        X_train: np.ndarray,
        y_train: np.ndarray,
        X_valid: np.ndarray,
        y_valid: np.ndarray,
        X_test: np.ndarray,
        y_test: np.ndarray,
        params: Optional[dict] = None,
    ) -> TrainingResult:
        """Train using sklearn fallback."""
        from sklearn.ensemble import GradientBoostingClassifier

        default_params = {
            "max_depth": 4,
            "learning_rate": 0.1,
            "n_estimators": 100,
            "min_samples_split": 5,
            "min_samples_leaf": 3,
            "subsample": 0.8,
        }
        if params:
            default_params.update(params)

        model = GradientBoostingClassifier(**default_params)
        model.fit(X_train, y_train)

        train_pred = model.predict_proba(X_train)[:, 1]
        valid_pred = model.predict_proba(X_valid)[:, 1] if len(X_valid) > 0 else np.array([])
        test_pred = model.predict_proba(X_test)[:, 1] if len(X_test) > 0 else np.array([])

        feature_importances = []
        if hasattr(model, "feature_importances_"):
            for f, imp in zip(FEATURE_COLS, model.feature_importances_):
                feature_importances.append(FeatureImportance(feature=f, importance=float(imp), gain=0))

        feature_importances.sort(key=lambda x: x.importance, reverse=True)

        return self._build_result(
            y_train, train_pred,
            y_valid, valid_pred,
            y_test, test_pred,
            feature_importances,
        )

    def _sklearn_fallback(self, params: Optional[dict]) -> TrainingResult:
        """Fallback when no ML library available."""
        return TrainingResult(
            train_accuracy=0.0, train_auc=0.0,
            valid_accuracy=0.0, valid_auc=0.0,
            test_accuracy=0.0, test_auc=0.0,
            feature_importances=[],
            signal_detected=False,
            signal_strength="unknown",
            top_features=[],
            notes=["No ML library available - install xgboost or scikit-learn"],
        )

    def _insufficient_data_result(self) -> TrainingResult:
        """Result when not enough data."""
        return TrainingResult(
            train_accuracy=0.0, train_auc=0.0,
            valid_accuracy=0.0, valid_auc=0.0,
            test_accuracy=0.0, test_auc=0.0,
            feature_importances=[],
            signal_detected=False,
            signal_strength="insufficient_data",
            top_features=[],
            notes=["Insufficient training data"],
        )

    def _build_result(
        self,
        y_train: np.ndarray, train_pred: np.ndarray,
        y_valid: np.ndarray, valid_pred: np.ndarray,
        y_test: np.ndarray, test_pred: np.ndarray,
        feature_importances: list[FeatureImportance],
    ) -> TrainingResult:
        """Build training result."""
        train_acc = compute_accuracy(y_train, train_pred)
        train_auc = compute_auc(y_train, train_pred)

        valid_acc = compute_accuracy(y_valid, valid_pred) if len(y_valid) > 0 else 0.0
        valid_auc = compute_auc(y_valid, valid_pred) if len(y_valid) > 0 else 0.5

        test_acc = compute_accuracy(y_test, test_pred) if len(y_test) > 0 else 0.0
        test_auc = compute_auc(y_test, test_pred) if len(y_test) > 0 else 0.5

        auc_drop = train_auc - test_auc if len(y_test) > 0 else 0
        signal_detected = test_auc > 0.55

        if test_auc > 0.65:
            signal_strength = "strong"
        elif test_auc > 0.55:
            signal_strength = "moderate"
        elif test_auc > 0.52:
            signal_strength = "weak"
        else:
            signal_strength = "none"

        top_features = [f.feature for f in feature_importances[:5]]

        notes = []
        if auc_drop > 0.15:
            notes.append("High train/test gap - possible overfitting")
        if test_auc < 0.52:
            notes.append("No signal detected - model learns noise")
        if len(y_test) < 20:
            notes.append("Very small test set - results not reliable")

        valid_cal = compute_calibration(y_valid, valid_pred) if len(y_valid) > 0 else {}
        if valid_cal.get("ece", 0) > 0.1:
            notes.append("Poor calibration - consider isotonic regression")

        return TrainingResult(
            train_accuracy=train_acc,
            train_auc=train_auc,
            valid_accuracy=valid_acc,
            valid_auc=valid_auc,
            test_accuracy=test_acc,
            test_auc=test_auc,
            feature_importances=feature_importances,
            signal_detected=signal_detected,
            signal_strength=signal_strength,
            top_features=top_features,
            notes=notes,
        )


def run_baseline(data_dir: str = "data") -> TrainingResult:
    """Convenience function to run baseline experiment."""
    runner = XGBoostBaseline(data_dir=data_dir)
    return runner.train()


def format_baseline_report(result: TrainingResult) -> list[str]:
    """Format baseline report for CLI."""
    lines = [
        f"\n{'='*50}",
        "XGBOOST BASELINE REPORT",
        f"{'='*50}",
        "",
        "PERFORMANCE METRICS:",
        f"  Train  - Accuracy: {result.train_accuracy:.1%}, AUC: {result.train_auc:.3f}",
        f"  Valid  - Accuracy: {result.valid_accuracy:.1%}, AUC: {result.valid_auc:.3f}",
        f"  Test   - Accuracy: {result.test_accuracy:.1%}, AUC: {result.test_auc:.3f}",
        "",
        f"SIGNAL DETECTION:",
    ]

    signal_emoji = "NO" if not result.signal_detected else ("WEAK" if result.signal_strength == "weak" else "YES")
    lines.append(f"  Status: {result.signal_strength.upper()}")
    lines.append(f"  Signal detected: {signal_emoji}")

    lines.extend([
        "",
        "TOP FEATURES:",
    ])

    for i, feat in enumerate(result.feature_importances[:10]):
        bar = "█" * int(feat.importance * 20)
        lines.append(f"  {i+1}. {feat.feature:25s} {bar} {feat.importance:.3f}")

    if result.top_features:
        lines.append("")
        lines.append(f"Top 5: {', '.join(result.top_features[:5])}")

    if result.notes:
        lines.append("")
        lines.append("NOTES:")
        for note in result.notes:
            lines.append(f"  ⚠️ {note}")

    lines.append(f"{'='*50}\n")
    return lines