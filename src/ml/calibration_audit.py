"""
Probabilistic calibration audit tools (Brier Score, Log Loss).
"""
import math
from typing import List, Dict, Any

def brier_score(probabilities: List[float], outcomes: List[int]) -> float:
    """Calculate the Brier Score (lower is better, 0 is perfect)."""
    if not probabilities or len(probabilities) != len(outcomes):
        return 1.0
    return sum((p - o)**2 for p, o in zip(probabilities, outcomes)) / len(probabilities)

def log_loss(probabilities: List[float], outcomes: List[int]) -> float:
    """Calculate the Log Loss (lower is better)."""
    if not probabilities or len(probabilities) != len(outcomes):
        return float('inf')
    eps = 1e-15
    loss = 0.0
    for p, o in zip(probabilities, outcomes):
        p = max(eps, min(1.0 - eps, p))
        loss += -(o * math.log(p) + (1 - o) * math.log(1 - p))
    return loss / len(probabilities)

def reliability_audit(probabilities: List[float], outcomes: List[int], bins: int = 5) -> Dict[str, Any]:
    """Audit the calibration quality across probability bins."""
    if not probabilities:
        return {}
    
    bin_data = []
    for i in range(bins):
        low = i / bins
        high = (i + 1) / bins
        
        # Filter samples in this bin
        bin_samples = [(p, o) for p, o in zip(probabilities, outcomes) if low <= p < high]
        if not bin_samples:
            continue
            
        avg_pred = sum(p for p, o in bin_samples) / len(bin_samples)
        actual_freq = sum(o for p, o in bin_samples) / len(bin_samples)
        
        bin_data.append({
            "range": (low, high),
            "avg_pred": round(avg_pred, 4),
            "actual_freq": round(actual_freq, 4),
            "error": round(actual_freq - avg_pred, 4),
            "count": len(bin_samples)
        })
        
    return {
        "brier_score": round(brier_score(probabilities, outcomes), 4),
        "log_loss": round(log_loss(probabilities, outcomes), 4),
        "bin_details": bin_data
    }
