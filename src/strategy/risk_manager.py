"""
Portfolio Risk Management - Correlation and exposure limits.
"""
from __future__ import annotations
from typing import List, Dict, Any

REGIONAL_CLUSTERS = {
    "EUROPE": {"london", "paris", "munich", "ankara"},
    "US_EAST": {"nyc", "atlanta", "miami"},
    "US_CENTRAL": {"chicago", "dallas"},
    "PACIFIC_ASIA": {"seattle", "tokyo", "seoul", "shanghai", "singapore"},
    "LATAM": {"sao_paulo", "buenos_aires"}
}

class PortfolioRiskManager:
    """
    Manages global risk across all cities and horizons.
    Prevents concentration risk and regional clustering.
    """
    def __init__(self, config):
        self.config = config
        self.max_exposure_per_city = getattr(config, "max_exposure_per_city", 100.0)
        self.max_exposure_per_region = getattr(config, "max_exposure_per_region", 250.0)
        self.max_total_exposure = getattr(config, "max_total_exposure", 1000.0)

    def get_region(self, city_slug: str) -> str:
        for region, cities in REGIONAL_CLUSTERS.items():
            if city_slug.lower() in cities:
                return region
        return "OTHER"

    def check_new_trade(self, city: str, cost: float, open_markets: List[Any]) -> Dict[str, Any]:
        """
        Check if a new trade complies with regional and city limits.
        """
        # 1. Total Exposure
        total_open = sum(m.position.get("cost", 0) for m in open_markets if m.position and m.position.get("status") == "open")
        if total_open + cost > self.max_total_exposure:
            return {"allowed": False, "reason": f"total_exposure_limit ({total_open + cost:.2f} > {self.max_total_exposure})"}

        # 2. City Concentration
        city_open = sum(m.position.get("cost", 0) for m in open_markets if m.city == city and m.position and m.position.get("status") == "open")
        if city_open + cost > self.max_exposure_per_city:
            return {"allowed": False, "reason": f"city_concentration_limit ({city_open + cost:.2f} > {self.max_exposure_per_city})"}

        # 3. Regional Concentration (Hidden Correlation Defense)
        region = self.get_region(city)
        region_open = sum(m.position.get("cost", 0) for m in open_markets if self.get_region(m.city) == region and m.position and m.position.get("status") == "open")
        if region_open + cost > self.max_exposure_per_region:
            return {"allowed": False, "reason": f"regional_concentration_limit ({region}: {region_open + cost:.2f} > {self.max_exposure_per_region})"}

        return {"allowed": True, "reason": "ok"}

    def get_risk_summary(self, open_markets: List[Any]) -> Dict[str, Any]:
        """Return a snapshot of current portfolio risk."""
        active_pos = [m for m in open_markets if m.position and m.position.get("status") == "open"]
        total_open = sum(m.position.get("cost", 0) for m in active_pos)
        
        city_exposures = {}
        region_exposures = {}
        for m in active_pos:
            city_exposures[m.city] = city_exposures.get(m.city, 0) + m.position.get("cost", 0)
            region = self.get_region(m.city)
            region_exposures[region] = region_exposures.get(region, 0) + m.position.get("cost", 0)
        
        # Diversification Index: Inverse HHI (Effective Number of Bets)
        # HHI = Sum(weights^2). Inverse HHI = 1 / HHI.
        # Range: 1.0 (Concentrated) to N (Perfectly Diversified)
        if total_open > 0:
            hhi = sum((exp / total_open)**2 for exp in city_exposures.values())
            effective_bets = 1 / hhi if hhi > 0 else 0
        else:
            effective_bets = 0
        
        return {
            "total_exposure": total_open,
            "utilization_pct": round(total_open / self.max_total_exposure * 100, 2),
            "region_exposures": region_exposures,
            "diversification_index": round(effective_bets, 2),
            "active_cities": len(city_exposures)
        }
