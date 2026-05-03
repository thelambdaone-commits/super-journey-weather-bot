"""
Open-Meteo Multi-Model Collector - Regional Optimization.
"""
import polars as pl
from datetime import datetime, timezone, timedelta
import logging
import requests
from ..open_meteo_rate_limiter import rate_limited_get

logger = logging.getLogger(__name__)

# Full elite blend (HRRR removed - not available in multi-model API)
MODELS = "ecmwf_ifs04,gfs_seamless,icon_seamless,jma_seamless"

# Model Regional Expertise
MODEL_REGIONS = {
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
                lat = getattr(config, "lat", None)
                lon = getattr(config, "lon", None)
                if not lat or not lon: continue
                
                # Fetch more models but filter them during processing or weighting
                url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m&models={MODELS}&timezone=UTC"
                response = rate_limited_get(url, timeout=10, max_429_retries=2)
                if response.status_code == 429:
                    logger.warning(f"Open-Meteo 429 for {city} after retries, using MET Norway fallback")
                    all_data.extend(self._fetch_metno_fallback(city, config, now_ts))
                    continue
                resp = response.json()
                
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
                        if temp is None:
                            continue
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

    def _fetch_metno_fallback(self, city: str, config: object, now_ts: datetime) -> list[dict]:
        lat = getattr(config, "lat", None)
        lon = getattr(config, "lon", None)
        if not lat or not lon:
            return []

        try:
            response = requests.get(
                "https://api.met.no/weatherapi/locationforecast/2.0/compact",
                params={"lat": lat, "lon": lon},
                headers={"User-Agent": "weatherbot/1.0 github.com/weatherbot"},
                timeout=(5, 10),
            )
            response.raise_for_status()
            data = response.json()
        except (Exception,) as e:
            logger.error(f"MET Norway fallback failed for {city}: {e}")
            return []

        rows = []
        run_cycle = now_ts.replace(minute=0, second=0, microsecond=0)
        for item in data.get("properties", {}).get("timeseries", []):
            time_str = item.get("time")
            if not time_str:
                continue
            temp = item.get("data", {}).get("instant", {}).get("details", {}).get("air_temperature")
            if temp is None:
                continue
            valid_time = datetime.fromisoformat(time_str.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)
            horizon = (valid_time - run_cycle.replace(tzinfo=None)).total_seconds() // 3600
            rows.append({
                "ingested_at": now_ts,
                "city": city,
                "model": "metno_locationforecast",
                "run_cycle": run_cycle.replace(tzinfo=None),
                "valid_time": valid_time,
                "horizon_hours": int(horizon),
                "temp_c": float(temp),
                "humidity": 0.0,
                "pressure": 0.0
            })
        return rows
