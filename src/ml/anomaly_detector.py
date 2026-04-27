"""
Anomaly Detection Autoencoder (#5) for WeatherBot.
Uses reconstruction error to identify suspicious or manipulated market data.
"""
from __future__ import annotations
import numpy as np
import joblib
from pathlib import Path
from sklearn.neural_network import MLPRegressor
from sklearn.preprocessing import StandardScaler
import logging

logger = logging.getLogger(__name__)

class AnomalyAutoencoder:
    """
    Deep Autoencoder for outlier detection.
    Compresses and reconstructs feature vectors. High error = Anomaly.
    """

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.model_path = self.data_dir / "anomaly_autoencoder.pkl"
        self.model = None
        self.scaler = StandardScaler()
        self.fitted = False
        self.threshold = 0.5 # Default reconstruction error threshold

    def train(self, X):
        """Train the autoencoder on normal data."""
        if len(X) < 50:
            return False
            
        X_scaled = self.scaler.fit_transform(X)
        
        # Bottleneck architecture: 10 -> 4 -> 10
        self.model = MLPRegressor(
            hidden_layer_sizes=(6, 4, 6),
            activation='relu',
            solver='adam',
            max_iter=500,
            random_state=42
        )
        
        try:
            self.model.fit(X_scaled, X_scaled)
            self.fitted = True
            
            # Calculate threshold based on 95th percentile of reconstruction error
            reconstructed = self.model.predict(X_scaled)
            errors = np.mean(np.square(X_scaled - reconstructed), axis=1)
            self.threshold = float(np.percentile(errors, 95))
            
            joblib.dump({"model": self.model, "scaler": self.scaler, "threshold": self.threshold}, self.model_path)
            return True
        except Exception as e:
            logger.error(f"Error training Autoencoder: {e}")
            return False

    def load(self):
        """Load from disk."""
        if self.model_path.exists():
            try:
                data = joblib.load(self.model_path)
                self.model = data["model"]
                self.scaler = data["scaler"]
                self.threshold = data["threshold"]
                self.fitted = True
                return True
            except Exception:
                pass
        return False

    def is_anomalous(self, X_new) -> tuple[bool, float]:
        """Check if a sample is anomalous."""
        if not self.fitted:
            return False, 0.0
            
        try:
            X_scaled = self.scaler.transform(X_new.reshape(1, -1))
            reconstructed = self.model.predict(X_scaled)
            error = float(np.mean(np.square(X_scaled - reconstructed)))
            return error > self.threshold, error
        except Exception:
            return False, 0.0

def get_anomaly_detector(data_dir: str = "data") -> AnomalyAutoencoder:
    ad = AnomalyAutoencoder(data_dir)
    ad.load()
    return ad
