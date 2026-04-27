"""
Regularized Logistic Regression (Logistic Shrinkage) for small datasets.
"""
from __future__ import annotations
import json
import pickle
from pathlib import Path
from typing import Optional

import numpy as np
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler

from .dataset import FEATURE_COLS

class LogisticShrinkageModel:
    """
    Logistic Regression with L2 regularization (shrinkage).
    Better suited for small datasets than XGBoost.
    """
    def __init__(self, C: float = 0.1):
        self.C = C
        self.model = LogisticRegression(
            C=C, 
            penalty='l2', 
            solver='lbfgs', 
            max_iter=1000,
            class_weight='balanced'
        )
        self.scaler = StandardScaler()
        self.is_fitted = False

    def train(self, X: np.ndarray, y: np.ndarray):
        """Train the model with scaling."""
        if len(np.unique(y)) < 2:
            raise ValueError("Dataset must contain at least two classes (win/loss)")
            
        X_scaled = self.scaler.fit_transform(X)
        self.model.fit(X_scaled, y)
        self.is_fitted = True

    def predict_proba(self, X: np.ndarray) -> np.ndarray:
        """Predict probabilities with scaling."""
        if not self.is_fitted:
            raise RuntimeError("Model not fitted")
        X_scaled = self.scaler.transform(X)
        return self.model.predict_proba(X_scaled)[:, 1]

    def save(self, path: str):
        """Save model and scaler to a file."""
        data = {
            "model": self.model,
            "scaler": self.scaler,
            "C": self.C,
            "features": FEATURE_COLS
        }
        with open(path, "wb") as f:
            pickle.dump(data, f)

    @classmethod
    def load(cls, path: str) -> 'LogisticShrinkageModel':
        """Load model from a file."""
        with open(path, "rb") as f:
            data = pickle.load(f)
        
        instance = cls(C=data.get("C", 0.1))
        instance.model = data["model"]
        instance.scaler = data["scaler"]
        instance.is_fitted = True
        return instance

    def get_coefficients(self) -> dict[str, float]:
        """Return model coefficients for interpretation."""
        if not self.is_fitted:
            return {}
        
        coefs = self.model.coef_[0]
        return {feat: float(c) for feat, c in zip(FEATURE_COLS, coefs)}

def train_shrunk_model(X_train: np.ndarray, y_train: np.ndarray, C: float = 0.1) -> LogisticShrinkageModel:
    """Convenience helper to train a shrunk model."""
    model = LogisticShrinkageModel(C=C)
    model.train(X_train, y_train)
    return model
