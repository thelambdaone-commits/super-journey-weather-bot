"""
Portfolio Optimization for WeatherBot.
Manages cross-city correlations and liquidity-weighted allocation.
"""
from __future__ import annotations
import numpy as np
import logging
from typing import Dict, List, Any
from ..utils.feature_flags import is_enabled

logger = logging.getLogger(__name__)

# Climate Region Mapping (Used for Correlation Mitigation)
REGIONS = {
    "EUROPE": ["LONDON", "PARIS", "MUNICH", "MADRID", "ROME", "BERLIN"],
    "NORTH_AMERICA": ["NEW YORK CITY", "CHICAGO", "DALLAS", "MIAMI", "TORONTO", "SEATTLE", "ATLANTA"],
    "ASIA": ["TOKYO", "SEOUL", "SHANGHAI", "SINGAPORE", "HONG KONG", "DELHI", "LUCKNOW", "ANKARA"],
    "OCEANIA": ["SYDNEY", "WELLINGTON"],
    "LATAM": ["MEXICO CITY", "SAO PAULO", "BUENOS AIRES"],
}

def get_region(city_name: str) -> str:
    """Find the region for a given city."""
    city_upper = city_name.upper()
    for region, cities in REGIONS.items():
        if city_upper in cities:
            return region
    return "UNKNOWN"

class PortfolioOptimizer:
    """
    Optimizes sizing across multiple markets.
    Implements regional caps and liquidity-aware scaling.
    """

    def __init__(self, config):
        self.config = config
        self.max_region_exposure = 0.35 # Max 35% exposure to one climate region
        self.max_single_exposure = 0.15 # Max 15% per city
        self.base_kelly_multiplier = getattr(config, "kelly_fraction", 0.10)

    def get_adaptive_kelly(self, drawdown_pct: float) -> float:
        """Adaptive Kelly scaling based on drawdown (#8)."""
        if not is_enabled("ADAPTIVE_RISK_ENGINE"):
            return self.base_kelly_multiplier
            
        if drawdown_pct > 30:
            return 0.0 # Stop Loss
        if drawdown_pct > 20:
            return self.base_kelly_multiplier * 0.2
        if drawdown_pct > 10:
            return self.base_kelly_multiplier * 0.5
        return self.base_kelly_multiplier

    def optimize_sizing(self, pending_signals: List[Dict], current_markets: List[Any], balance: float, drawdown_pct: float = 0.0) -> List[Dict]:
        """
        Adjust sizing for a set of signals based on current portfolio exposure.
        """
        region_exposure = {}
        for m in current_markets:
            if m.status == "open":
                region = get_region(m.city_name)
                cost = m.position.get("cost", 0) if m.position else 0
                region_exposure[region] = region_exposure.get(region, 0) + cost

        optimized = []
        for sig in pending_signals:
            city = sig["city"]
            region = get_region(city)
            proposed_cost = sig["signal"]["cost"]
            
            # 1. Regional Cap
            current_reg_exp = region_exposure.get(region, 0)
            available_reg = (balance * self.max_region_exposure) - current_reg_exp
            
            if available_reg <= 0:
                logger.info(f"[PORTFOLIO] Regional cap hit for {region} ({city}). Skipping.")
                continue
                
            if proposed_cost > available_reg:
                logger.info(f"[PORTFOLIO] Scaling {city} down: ${proposed_cost:.2f} -> ${available_reg:.2f} (Region Cap)")
                proposed_cost = available_reg

            # 3. Liquidity-Adjusted Sizing (#9)
            if is_enabled("LIQUIDITY_ADJUSTED_SIZING"):
                try:
                    ask_size_usd = sig["signal"].get("best_ask_size_usd", proposed_cost * 10)
                    # We shouldn't take more than 50% of the top level to avoid high slippage
                    liquidity_cap = ask_size_usd * 0.5
                    if proposed_cost > liquidity_cap:
                        logger.info(f"[PORTFOLIO] Scaling {city} down: ${proposed_cost:.2f} -> ${liquidity_cap:.2f} (Liquidity Cap)")
                        proposed_cost = liquidity_cap
                except Exception:
                    pass

            # 3.5 Adaptive Sizing (#8)
            adaptive_multiplier = self.get_adaptive_kelly(drawdown_pct)
            if adaptive_multiplier < self.base_kelly_multiplier:
                # Re-calculate size with adaptive Kelly
                # Note: This is an approximation since we don't have the full raw Kelly here
                # but we can scale the proposed cost proportionally.
                scale = adaptive_multiplier / self.base_kelly_multiplier
                proposed_cost *= scale
                logger.info(f"[PORTFOLIO] Adaptive scaling for {city}: multiplier {adaptive_multiplier:.2f} (Drawdown {drawdown_pct:.1f}%)")

            # 4. Update state for next signal in loop
            sig["signal"]["cost"] = round(proposed_cost, 2)
            region_exposure[region] = region_exposure.get(region, 0) + proposed_cost
            optimized.append(sig)

        return optimized
