import os
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
import joblib
import numpy as np

try:
    import xgboost as xgb
except ImportError:
    xgb = None

from .metrics import calculate_ml_metrics
from .shrunk_model import LogisticShrinkageModel
from .bayesian_model import BayesianProbabilityModel

logger = logging.getLogger(__name__)

class TrainingResult:
    def __init__(self, metrics: Dict[str, Any], model_type: str, path: str):
        self.metrics = metrics
        self.model_type = model_type
        self.path = path
        self.timestamp = datetime.now().isoformat()

class XGBoostBaseline:
    """Baseline model trainer for WeatherBot."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = data_dir
        self.model_path = os.path.join(data_dir, "ml_model.pkl")
        self.model = None

    def prepare_data(self, test_size: float = 0.2):
        """Load and prepare data for training."""
        from .dataset import load_training_data
        X, y = load_training_data(self.data_dir)
        
        if len(X) < 10:
            return None, None, None, None

        # Simple split
        split_idx = int(len(X) * (1 - test_size))
        X_train, X_valid = X[:split_idx], X[split_idx:]
        y_train, y_valid = y[:split_idx], y[split_idx:]
        
        return X_train, X_valid, y_train, y_valid

    def train(self, model_type: str = "xgboost", params: Optional[dict] = None) -> Optional[TrainingResult]:
        """Train a model and save it."""
        X_train, X_valid, y_train, y_valid = self.prepare_data()
        
        if X_train is None:
            logger.warning("Not enough data to train model.")
            return None

        if model_type == "xgboost":
            if xgb is None:
                logger.error("XGBoost not installed.")
                return None
            
            # Default params
            xgb_params = {
                "n_estimators": 100,
                "max_depth": 3,
                "learning_rate": 0.05,
                "objective": "binary:logistic",
                "random_state": 42
            }
            if params:
                xgb_params.update(params)
            
            model = xgb.XGBClassifier(**xgb_params)
            model.fit(X_train, y_train)
            self.model = model
            
        elif model_type == "logistic":
            c_val = params.get("C", 0.1) if params else 0.1
            model = LogisticShrinkageModel(C=c_val)
            model.fit(X_train, y_train)
            self.model = model
            
        elif model_type == "bayesian":
            model = BayesianProbabilityModel(self.data_dir)
            model.train(X_train, y_train)
            self.model = model
            
        else:
            logger.error(f"Unknown model type: {model_type}")
            return None

        # Save model
        if params and params.get("save", False):
            joblib.dump(self.model, self.model_path)
            # Also save metadata
            with open(os.path.join(self.data_dir, "ml_metadata.json"), "w") as f:
                json.dump({
                    "model_type": model_type,
                    "samples": len(X_train) + len(X_valid),
                    "timestamp": datetime.now().isoformat(),
                    "features": ["ecmwf_err", "hrrr_err", "metar_err", "horizon", "is_night"] # Sample features
                }, f)

        # Evaluate
        if model_type == "bayesian":
            # Bayesian model returns (mean, std)
            y_pred_valid, _ = self.model.predict(X_valid) if len(X_valid) > 0 else (np.array([]), None)
        else:
            y_pred_valid = self.model.predict_proba(X_valid)[:, 1] if len(X_valid) > 0 else np.array([])
            
        metrics = calculate_ml_metrics(y_valid, y_pred_valid)
        
        return TrainingResult(metrics, model_type, self.model_path)

def format_baseline_report(result: TrainingResult) -> List[str]:
    """Format training result for CLI/Telegram."""
    m = result.metrics
    lines = [
        f"=== ML TRAINING REPORT ({result.model_type.upper()}) ===",
        f"Timestamp: {result.timestamp}",
        f"Log Loss: {m.get('log_loss', 0):.4f}",
        f"Brier Score: {m.get('brier', 0):.4f}",
        f"ROC AUC: {m.get('auc', 0):.4f}",
        f"ECE (Calibration): {m.get('ece', 0):.4f}",
        f"Path: {result.path}"
    ]
    return lines