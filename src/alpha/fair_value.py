"""
Fair Value Engine V3.2 - Professional Grade.
Robust error handling and execution validation.
"""
import numpy as np
from scipy.stats import norm
import logging
from datetime import datetime, timezone
from ..data.moat_manager import get_moat

logger = logging.getLogger(__name__)

class FairValueError(Exception):
    """Exception raised by the FairValueEngine."""
    pass

class FairValueEngine:
    def __init__(self, moat_manager=None):
        self.moat = moat_manager or get_moat()

    def calculate_fair_value(self, city: str, threshold: float, target_time: datetime, side: str = "ABOVE") -> float:
        """Derive probability using anti-leakage ensemble."""
        if side.upper() not in ("ABOVE", "BELOW"):
            raise FairValueError(f"Invalid side: {side}. Must be ABOVE or BELOW.")

        try:
            ensemble_df = self.moat.get_latest_valid_forecasts(city, target_time)
            if ensemble_df.is_empty():
                logger.warning("[FV] No valid forecasts found in Moat for %s at %s", city, target_time)
                raise FairValueError(f"Insufficient data for {city}")
            
            temps = ensemble_df["temp_c"].to_list()
            mean_f = np.mean(temps)
            # Dispersion-based uncertainty (min 1.1 degrees)
            sigma = max(1.1, np.std(temps))
            
            z_score = (threshold - mean_f) / sigma
            prob_above = 1.0 - norm.cdf(z_score)
            
            fair_value = prob_above if side.upper() == "ABOVE" else (1.0 - prob_above)
            return round(float(fair_value), 4)
            
        except Exception as e:
            if isinstance(e, FairValueError):
                raise
            logger.exception("Fair Value calculation crashed for %s", city)
            raise FairValueError(f"Internal computation error for {city}") from e

    def check_calibration_gate(self, city: str, model: str, edge: float) -> bool:
        """PR #4: Calibration Gate."""
        try:
            recent_mae = self.moat.get_recent_calibration_error(city, model)
            if recent_mae > 2.5 and edge < 0.15:
                logger.warning("[GATE] %s blocked: High MAE %.2f needs >15%% edge", city, recent_mae)
                return False
            return True
        except Exception:
            logger.exception("Calibration gate failed for %s", city)
            return False # Conservative: block if check fails

    def get_vwap_edge(self, fair_value: float, vwap_ask: float, spread: float, slippage_buffer: float = 0.01) -> float:
        """Calculate edge against VWAP."""
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
