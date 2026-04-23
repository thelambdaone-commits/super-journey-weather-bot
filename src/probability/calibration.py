"""
Probability calibration utilities.
"""
from __future__ import annotations

from pathlib import Path
from dataclasses import dataclass

import numpy as np

try:
    import joblib
except Exception:  # pragma: no cover - optional dependency
    joblib = None

try:
    from sklearn.isotonic import IsotonicRegression
    from sklearn.linear_model import LogisticRegression
    from sklearn.metrics import brier_score_loss
except Exception:  # pragma: no cover - optional dependency
    IsotonicRegression = None
    LogisticRegression = None
    brier_score_loss = None


class CalibrationEngine:
    """
    Handles probability calibration for weather prediction markets.

    Supports:
    - Isotonic Regression (primary)
    - Platt Scaling fallback
    - persistence
    """

    def __init__(self, method: str = "isotonic"):
        self.method = method
        self.model = None
        self.fitted = False

    def fit(self, y_prob, y_true) -> None:
        """Fit the calibration model from raw probabilities and outcomes."""
        y_prob = np.asarray(y_prob, dtype=float)
        y_true = np.asarray(y_true, dtype=float)
        if y_prob.size == 0 or y_true.size == 0 or y_prob.shape[0] != y_true.shape[0]:
            self.fitted = False
            return

        try:
            if self.method == "isotonic" and IsotonicRegression is not None:
                self.model = IsotonicRegression(out_of_bounds="clip")
                self.model.fit(y_prob, y_true)
            elif LogisticRegression is not None:
                self.model = LogisticRegression()
                self.model.fit(y_prob.reshape(-1, 1), y_true)
            else:
                self.model = None
                self.fitted = False
                return
            self.fitted = True
        except Exception:
            self.model = None
            self.fitted = False

    def _safe_identity(self, y_prob, confidence: float | np.ndarray | None = None):
        """Fallback shrinkage toward neutral when no calibrator is ready."""
        probs = np.asarray(y_prob, dtype=float)
        if confidence is None:
            return np.clip(probs, 0.0, 1.0)
        conf = np.asarray(confidence, dtype=float)
        neutral = 0.5
        shrunk = neutral + (probs - neutral) * np.clip(conf, 0.1, 1.0)
        return np.clip(shrunk, 0.0, 1.0)

    def transform(self, y_prob, confidence: float | np.ndarray | None = None):
        """Apply calibration to raw probabilities."""
        if not self.fitted or self.model is None:
            return self._safe_identity(y_prob, confidence)

        probs = np.asarray(y_prob, dtype=float)
        try:
            if self.method == "isotonic":
                calibrated = self.model.transform(probs)
            else:
                calibrated = self.model.predict_proba(probs.reshape(-1, 1))[:, 1]
            return np.clip(calibrated, 0.0, 1.0)
        except Exception:
            return self._safe_identity(y_prob, confidence)

    def evaluate(self, y_prob, y_true) -> dict:
        """Return simple calibration metrics."""
        probs = np.asarray(y_prob, dtype=float)
        truth = np.asarray(y_true, dtype=float)
        calibrated = np.asarray(self.transform(probs), dtype=float)
        mean_error = float(np.mean(calibrated - truth)) if truth.size else 0.0
        if brier_score_loss is not None and truth.size:
            brier = float(brier_score_loss(truth, calibrated))
        else:
            brier = float(np.mean((calibrated - truth) ** 2)) if truth.size else 0.0
        return {
            "brier_score": brier,
            "mean_error": mean_error,
            "fitted": self.fitted,
            "method": self.method,
        }

    def save(self, path: str = "calibration.pkl") -> bool:
        """Persist a fitted calibrator if dependencies are available."""
        if joblib is None:
            return False
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        joblib.dump(
            {
                "model": self.model,
                "method": self.method,
                "fitted": self.fitted,
            },
            target,
        )
        return True

    def load(self, path: str = "calibration.pkl") -> bool:
        """Load a persisted calibrator if present."""
        if joblib is None:
            return False
        target = Path(path)
        if not target.exists():
            return False
        obj = joblib.load(target)
        self.model = obj.get("model")
        self.method = obj.get("method", self.method)
        self.fitted = bool(obj.get("fitted"))
        return True


@dataclass
class CalibrationValidationReport:
    """Holdout validation report for one fitted calibrator."""

    accepted: bool
    method: str
    train_samples: int
    test_samples: int
    before_brier: float
    after_brier: float
    brier_improved: bool
    has_perfect_predictions: bool
    variance_preserved: bool
    before_std: float
    after_std: float
    mean_error_before: float
    mean_error_after: float
    reason: str


class CalibrationValidator:
    """Guardrail layer that accepts calibration only if holdout behavior is sane."""

    def __init__(
        self,
        holdout_ratio: float = 0.33,
        random_seed: int = 42,
        min_train_samples: int = 12,
        min_test_samples: int = 6,
        min_brier_gain: float = 1e-4,
        min_variance_ratio: float = 0.25,
        max_variance_ratio: float = 1.75,
    ):
        self.holdout_ratio = holdout_ratio
        self.random_seed = random_seed
        self.min_train_samples = min_train_samples
        self.min_test_samples = min_test_samples
        self.min_brier_gain = min_brier_gain
        self.min_variance_ratio = min_variance_ratio
        self.max_variance_ratio = max_variance_ratio

    def split(self, y_prob, y_true) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray] | None:
        """Create a deterministic holdout split."""
        probs = np.asarray(y_prob, dtype=float)
        truth = np.asarray(y_true, dtype=float)
        n = probs.shape[0]
        if n != truth.shape[0]:
            return None
        if n < self.min_train_samples + self.min_test_samples:
            return None

        rng = np.random.default_rng(self.random_seed)
        indices = np.arange(n)
        rng.shuffle(indices)
        test_size = max(self.min_test_samples, int(round(n * self.holdout_ratio)))
        test_size = min(test_size, n - self.min_train_samples)
        if test_size < self.min_test_samples:
            return None
        train_idx = indices[test_size:]
        test_idx = indices[:test_size]
        return probs[train_idx], truth[train_idx], probs[test_idx], truth[test_idx]

    def validate(self, calibrator: CalibrationEngine, y_prob, y_true) -> CalibrationValidationReport:
        """Validate one calibrator on holdout data."""
        split = self.split(y_prob, y_true)
        if split is None:
            total = min(len(np.asarray(y_prob)), len(np.asarray(y_true)))
            return CalibrationValidationReport(
                accepted=False,
                method=calibrator.method,
                train_samples=0,
                test_samples=0,
                before_brier=0.0,
                after_brier=0.0,
                brier_improved=False,
                has_perfect_predictions=False,
                variance_preserved=False,
                before_std=0.0,
                after_std=0.0,
                mean_error_before=0.0,
                mean_error_after=0.0,
                reason=f"insufficient_samples:{total}",
            )

        train_prob, train_true, test_prob, test_true = split
        candidate = CalibrationEngine(method=calibrator.method)
        candidate.fit(train_prob, train_true)
        if not candidate.fitted:
            return CalibrationValidationReport(
                accepted=False,
                method=calibrator.method,
                train_samples=len(train_true),
                test_samples=len(test_true),
                before_brier=0.0,
                after_brier=0.0,
                brier_improved=False,
                has_perfect_predictions=False,
                variance_preserved=False,
                before_std=0.0,
                after_std=0.0,
                mean_error_before=0.0,
                mean_error_after=0.0,
                reason="fit_failed",
            )

        calibrated = np.asarray(candidate.transform(test_prob), dtype=float)
        before_brier = float(np.mean((test_prob - test_true) ** 2))
        after_brier = float(np.mean((calibrated - test_true) ** 2))
        before_std = float(np.std(test_prob))
        after_std = float(np.std(calibrated))
        mean_error_before = float(np.mean(test_prob - test_true))
        mean_error_after = float(np.mean(calibrated - test_true))

        has_perfect_predictions = bool(
            np.any(np.isclose(calibrated, 0.0, atol=1e-6)) or np.any(np.isclose(calibrated, 1.0, atol=1e-6))
        )
        variance_ratio = after_std / before_std if before_std > 1e-9 else 1.0
        variance_preserved = self.min_variance_ratio <= variance_ratio <= self.max_variance_ratio
        brier_improved = (before_brier - after_brier) > self.min_brier_gain

        accepted = brier_improved and not has_perfect_predictions and variance_preserved
        if not brier_improved:
            reason = "holdout_brier_not_improved"
        elif has_perfect_predictions:
            reason = "perfect_predictions_detected"
        elif not variance_preserved:
            reason = "variance_not_preserved"
        else:
            reason = "accepted"

        return CalibrationValidationReport(
            accepted=accepted,
            method=calibrator.method,
            train_samples=len(train_true),
            test_samples=len(test_true),
            before_brier=before_brier,
            after_brier=after_brier,
            brier_improved=brier_improved,
            has_perfect_predictions=has_perfect_predictions,
            variance_preserved=variance_preserved,
            before_std=before_std,
            after_std=after_std,
            mean_error_before=mean_error_before,
            mean_error_after=mean_error_after,
            reason=reason,
        )
