"""
ML Performance Metrics for audit and validation.
"""
from __future__ import annotations
import numpy as np

def brier_score(y_true: np.ndarray, y_prob: np.ndarray) -> float:
    """Mean Squared Error of probability estimates."""
    if len(y_true) == 0: return 0.0
    return float(np.mean((y_prob - y_true)**2))

def log_loss(y_true: np.ndarray, y_prob: np.ndarray, eps: float = 1e-15) -> float:
    """Cross-entropy loss."""
    if len(y_true) == 0: return 0.0
    y_prob = np.clip(y_prob, eps, 1 - eps)
    return float(-np.mean(y_true * np.log(y_prob) + (1 - y_true) * np.log(1 - y_prob)))

def expected_calibration_error(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10) -> float:
    """ECE: weighted average of the absolute difference between bin accuracy and bin confidence."""
    if len(y_true) == 0: return 0.0
    
    bins = np.linspace(0, 1, n_bins + 1)
    ece = 0.0
    
    for i in range(n_bins):
        mask = (y_prob >= bins[i]) & (y_prob < bins[i + 1])
        if i == n_bins - 1:
            mask = (y_prob >= bins[i]) & (y_prob <= bins[i + 1])
            
        if np.sum(mask) == 0:
            continue
            
        bin_mean_prob = np.mean(y_prob[mask])
        bin_mean_true = np.mean(y_true[mask])
        bin_weight = np.sum(mask) / len(y_true)
        
        ece += bin_weight * abs(bin_mean_prob - bin_mean_true)
        
    return float(ece)

def calibration_curve(y_true: np.ndarray, y_prob: np.ndarray, n_bins: int = 10):
    """Calculate reliability curve data points."""
    bins = np.linspace(0, 1, n_bins + 1)
    prob_true = []
    prob_pred = []
    
    for i in range(n_bins):
        mask = (y_prob >= bins[i]) & (y_prob < bins[i + 1])
        if i == n_bins - 1:
            mask = (y_prob >= bins[i]) & (y_prob <= bins[i + 1])
            
        if np.sum(mask) > 0:
            prob_true.append(np.mean(y_true[mask]))
            prob_pred.append(np.mean(y_prob[mask]))
            
    return np.array(prob_true), np.array(prob_pred)

def calculate_ml_metrics(y_true: np.ndarray, y_prob: np.ndarray) -> dict:
    """Calculate a complete set of ML metrics."""
    if len(y_true) == 0:
        return {"brier": 0, "log_loss": 0, "auc": 0, "ece": 0}
        
    from sklearn.metrics import roc_auc_score
    
    try:
        auc = roc_auc_score(y_true, y_prob) if len(np.unique(y_true)) > 1 else 0.5
    except (Exception,) as e:
        auc = 0.5
        
    return {
        "brier": brier_score(y_true, y_prob),
        "log_loss": log_loss(y_true, y_prob),
        "auc": auc,
        "ece": expected_calibration_error(y_true, y_prob)
    }
