"""
Open-Meteo Multi-Model Collector - Regional Optimization.
"""
import requests
import polars as pl
from datetime import datetime, timezone, timedelta
import logging

logger = logging.getLogger(__name__)

# Full elite blend
MODELS = "ecmwf_ifs04,gfs_seamless,icon_seamless,jma_seamless,hrrr"

# Model Regional Expertise
MODEL_REGIONS = {
    "hrrr": ["NORTH_AMERICA"],
    "gfs_seamless": ["NORTH_AMERICA", "GLOBAL"],
    "ecmwf_ifs04": ["EUROPE", "GLOBAL"],
    "jma_seamless": ["ASIA"],
    "icon_seamless": ["EUROPE", "GLOBAL"]
}

CITY_REGIONS = {
    "NYC": "NORTH_AMERICA", "CHICAGO": "NORTH_AMERICA", "DALLAS": "NORTH_AMERICA",
    "MIAMI": "NORTH_AMERICA", "ATLANTA": "NORTH_AMERICA", "SEATTLE": "NORTH_AMERICA",
    "PARIS": "EUROPE", "LONDON": "EUROPE", "MUNICH": "EUROPE",
    "TOKYO": "ASIA", "SINGAPORE": "ASIA", "SEOUL": "ASIA", "SHANGHAI": "ASIA",
    "ANKARA": "ASIA", "LUCKNOW": "ASIA",
    "WELLINGTON": "OCEANIA", "SAO PAULO": "SOUTH_AMERICA", "BUENOS AIRES": "SOUTH_AMERICA"
}

class MultiModelCollector:
    def __init__(self, cities_config: dict):
        self.cities = cities_config

    def is_model_valid_for_region(self, model: str, city: str) -> bool:
        """Check if a model should be used for a specific city based on regional expertise."""
        region = CITY_REGIONS.get(city.upper(), "GLOBAL")
        valid_regions = MODEL_REGIONS.get(model, ["GLOBAL"])
        return region in valid_regions or "GLOBAL" in valid_regions

    def fetch_all_forecasts(self) -> pl.DataFrame:
        all_data = []
        now_ts = datetime.now(timezone.utc)
        
        for city, config in self.cities.items():
            try:
                lat, lon = config.get("lat"), config.get("lon")
                if not lat or not lon: continue
                
                # Fetch more models but filter them during processing or weighting
                url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m&models={MODELS}&timezone=UTC"
                resp = requests.get(url, timeout=10).json()
                
                hourly = resp.get("hourly", {})
                times = hourly.get("time", [])
                if not times: continue
                
                run_cycle = datetime.fromisoformat(times[0])
                
                for model in MODELS.split(","):
                    # Apply regional filtering
                    if not self.is_model_valid_for_region(model, city):
                        continue
                        
                    key = f"temperature_2m_{model}"
                    temps = hourly.get(key, [])
                    for t_str, temp in zip(times, temps):
                        t_iso = datetime.fromisoformat(t_str)
                        horizon = (t_iso - run_cycle).total_seconds() // 3600
                        
                        all_data.append({
                            "ingested_at": now_ts,
                            "city": city,
                            "model": model,
                            "run_cycle": run_cycle,
                            "valid_time": t_iso,
                            "horizon_hours": int(horizon),
                            "temp_c": float(temp),
                            "humidity": 0.0,
                            "pressure": 0.0
                        })
            except (Exception,) as e:
                logger.error(f"Failed fetch for {city}: {e}")
                
        return pl.DataFrame(all_data)
