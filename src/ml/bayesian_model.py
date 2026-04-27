"""
Bayesian model for epistemic uncertainty estimation.
Uses Gaussian Process Regression to provide both mean and standard deviation.
"""
from __future__ import annotations
import numpy as np
from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.gaussian_process.kernels import RBF, ConstantKernel as C, WhiteKernel
import joblib
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class BayesianProbabilityModel:
    """
    Gaussian Process Regressor for weather probabilities.
    Provides p(win) and std(p) for epistemic uncertainty.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.model_path = self.data_dir / "bayesian_model.pkl"
        self.model = None
        self.fitted = False

    def train(self, X, y):
        """Train the GP model."""
        if len(X) < 10:
            logger.warning("Not enough data for Bayesian GP training")
            return False

        # Kernel: Constant * RBF + Noise
        kernel = C(1.0, (1e-3, 1e3)) * RBF(10, (1e-2, 1e2)) + WhiteKernel(noise_level=0.1, noise_level_bounds=(1e-5, 1e1))
        
        self.model = GaussianProcessRegressor(
            kernel=kernel,
            n_restarts_optimizer=10,
            alpha=0.1,
            normalize_y=True
        )
        
        try:
            self.model.fit(X, y)
            self.fitted = True
            joblib.dump(self.model, self.model_path)
            return True
        except (Exception,) as e:
            logger.error(f"Error training Bayesian model: {e}")
            return False

    def load(self):
        """Load the model from disk."""
        if self.model_path.exists():
            try:
                self.model = joblib.load(self.model_path)
                self.fitted = True
                return True
            except (Exception,) as e:
                pass
        return False

    def predict(self, X_new) -> tuple[np.ndarray, np.ndarray]:
        """
        Predict mean and standard deviation.
        Returns: (mean_prob, epistemic_uncertainty)
        """
        if not self.fitted or self.model is None:
            # Fallback to neutral values if not fitted
            return np.array([0.5] * len(X_new)), np.array([0.5] * len(X_new))

        y_mean, y_std = self.model.predict(X_new, return_std=True)
        # Clip probability to [0, 1]
        y_mean = np.clip(y_mean, 0.01, 0.99)
        return y_mean, y_std

def get_bayesian_model(data_dir: str = "data") -> BayesianProbabilityModel:
    """Get or load the Bayesian model."""
    bm = BayesianProbabilityModel(data_dir)
    bm.load()
    return bm
