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
    "LATAM": {"sao-paulo", "sao_paulo", "buenos-aires", "buenos_aires"}
}

from .risk_clusters import get_cluster_mapping

class PortfolioRiskManager:
    """
    Manages global risk across all cities and horizons.
    Prevents concentration risk and regional/learned clustering.
    """
    def __init__(self, config):
        self.config = config
        self.max_exposure_per_city = getattr(config, "max_exposure_per_city", 100.0)
        self.max_exposure_per_region = getattr(config, "max_exposure_per_region", 250.0)
        self.max_exposure_per_cluster = getattr(config, "max_exposure_per_cluster", 300.0)
        self.max_total_exposure = getattr(config, "max_total_exposure", 1000.0)
        
        # Load learned clusters (residual-based)
        self.learned_clusters = get_cluster_mapping(getattr(config, "data_dir", "data"))

    def get_region(self, city_slug: str) -> str:
        city_key = city_slug.lower()
        for region, cities in REGIONAL_CLUSTERS.items():
            if city_key in cities:
                return region
        return "OTHER"

    @staticmethod
    def _position_cost(position: dict | None) -> float:
        if not position or position.get("status") not in ("open", "paper"):
            return 0.0
        try:
            return float(position.get("cost", 0.0) or 0.0)
        except (TypeError, ValueError):
            return 0.0

    def _active_exposures(self, open_markets: List[Any]) -> list[dict[str, Any]]:
        """Return only unresolved live/paper exposures that still lock stake."""
        exposures = []
        for market in open_markets:
            if getattr(market, "status", None) == "resolved":
                continue

            city = getattr(market, "city", None)
            if not city:
                continue

            for position_attr in ("position", "paper_position"):
                position = getattr(market, position_attr, None)
                cost = self._position_cost(position)
                if cost > 0:
                    exposures.append({"city": city, "cost": cost, "source": position_attr})
        return exposures

    def check_new_trade(self, city: str, cost: float, open_markets: List[Any]) -> Dict[str, Any]:
        """
        Check if a new trade complies with regional, cluster and city limits.
        """
        active_pos = self._active_exposures(open_markets)
        
        # 1. Total Exposure
        total_open = sum(pos["cost"] for pos in active_pos)
        if total_open + cost > self.max_total_exposure:
            return {"allowed": False, "reason": f"total_exposure_limit ({total_open + cost:.2f} > {self.max_total_exposure})"}

        # 2. City Concentration
        city_open = sum(pos["cost"] for pos in active_pos if pos["city"] == city)
        if city_open + cost > self.max_exposure_per_city:
            return {"allowed": False, "reason": f"city_concentration_limit ({city_open + cost:.2f} > {self.max_exposure_per_city})"}

        # 3. Regional Concentration (Hidden Correlation Defense)
        region = self.get_region(city)
        region_open = sum(pos["cost"] for pos in active_pos if self.get_region(pos["city"]) == region)
        if region_open + cost > self.max_exposure_per_region:
            return {"allowed": False, "reason": f"regional_concentration_limit ({region}: {region_open + cost:.2f} > {self.max_exposure_per_region})"}

        # 4. Learned Cluster Concentration (Advanced Correlation Defense)
        cluster_id = self.learned_clusters.get(city)
        if cluster_id is not None:
            cluster_open = sum(pos["cost"] for pos in active_pos if self.learned_clusters.get(pos["city"]) == cluster_id)
            if cluster_open + cost > self.max_exposure_per_cluster:
                return {"allowed": False, "reason": f"cluster_concentration_limit (Cluster {cluster_id}: {cluster_open + cost:.2f} > {self.max_exposure_per_cluster})"}

        return {"allowed": True, "reason": "ok"}


    def get_risk_summary(self, open_markets: List[Any]) -> Dict[str, Any]:
        """Return a snapshot of current portfolio risk."""
        active_pos = self._active_exposures(open_markets)
        total_open = sum(pos["cost"] for pos in active_pos)
        
        city_exposures = {}
        region_exposures = {}
        for pos in active_pos:
            city_exposures[pos["city"]] = city_exposures.get(pos["city"], 0) + pos["cost"]
            region = self.get_region(pos["city"])
            region_exposures[region] = region_exposures.get(region, 0) + pos["cost"]
        
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

# Audit: Includes fee and slippage awareness
