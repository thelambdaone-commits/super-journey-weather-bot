"""
Fair Value Engine V3.1 - Quant Fund Grade.
Includes Calibration Gate and VWAP-based execution modeling.
"""
import numpy as np
from scipy.stats import norm
import logging
from datetime import datetime, timezone
from ..data.moat_manager import get_moat

logger = logging.getLogger(__name__)

class FairValueEngine:
    def __init__(self, moat_manager=None):
        self.moat = moat_manager or get_moat()

    def calculate_fair_value(self, city: str, threshold: float, target_time: datetime, side: str = "ABOVE") -> float:
        """Derive probability using anti-leakage ensemble."""
        try:
            ensemble_df = self.moat.get_latest_valid_forecasts(city, target_time)
            if ensemble_df.is_empty():
                return 0.5
            
            temps = ensemble_df["temp_c"].to_list()
            mean_f = np.mean(temps)
            sigma = max(1.1, np.std(temps))
            
            z_score = (threshold - mean_f) / sigma
            prob_above = 1.0 - norm.cdf(z_score)
            
            return round(float(prob_above if side == "ABOVE" else (1.0 - prob_above)), 4)
        except Exception as e:
            logger.error(f"FV failed for {city}: {e}")
            return 0.5

    def check_calibration_gate(self, city: str, model: str, edge: float) -> bool:
        """
        PR #4: Calibration Gate.
        A signal is only tradable if recent calibration error is below threshold.
        """
        recent_mae = self.moat.get_recent_calibration_error(city, model)
        # Gate: If MAE > 2.5 degrees, we require a much larger edge to compensate
        if recent_mae > 2.5 and edge < 0.15:
            logger.warning(f"[GATE] {city} blocked: High MAE {recent_mae:.2f} needs >15% edge")
            return False
        return True

    def get_vwap_edge(self, fair_value: float, vwap_ask: float, spread: float, slippage_buffer: float = 0.01) -> float:
        """
        Calculate edge against VWAP (Volume-Weighted Average Price).
        Blocks alpha if edge doesn't survive spread + slippage buffer.
        """
        if vwap_ask <= 0 or vwap_ask >= 1.0:
            return 0.0
            
        net_edge = fair_value - (vwap_ask + slippage_buffer)
        
        # Block if spread is too wide
        if spread > net_edge:
            return 0.0
            
        return round(net_edge, 4)

_engine_instance = None

def get_fair_value_engine() -> FairValueEngine:
    global _engine_instance
    if _engine_instance is None:
        _engine_instance = FairValueEngine()
    return _engine_instance
