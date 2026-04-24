"""
Weather API clients - multiple sources with fallback.
"""
import logging
import math
import time
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from .locations import Location, get_by_slug, get_timezone, get_forecast_priority

logger = logging.getLogger(__name__)


class WeatherAPIError(Exception):
    """Weather API error."""
    pass


def get_ecmwf(city_slug: str, dates: list[str]) -> Dict[str, float]:
    """ECMWF via Open-Meteo."""
    loc = get_by_slug(city_slug)
    temp_unit = "fahrenheit" if loc.unit == "F" else "celsius"
    result = {}
    
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc.lat}&longitude={loc.lon}"
        f"&daily=temperature_2m_max&temperature_unit={temp_unit}"
        f"&forecast_days=7&timezone={get_timezone(city_slug)}"
        f"&models=ecmwf_ifs025&bias_correction=true"
    )
    
    for attempt in range(3):
        try:
            data = requests.get(url, timeout=(5, 10)).json()
            if "error" not in data:
                for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
                    if date in dates and temp is not None:
                        result[date] = round(temp, 1) if loc.unit == "C" else round(temp)
            return result
        except Exception:
            if attempt < 2:
                time.sleep(1)
    raise WeatherAPIError(f"ECMWF failed after 3 attempts")


def get_hrrr(city_slug: str, dates: list[str]) -> Dict[str, float]:
    """HRRR/GFS via Open-Meteo (US only)."""
    loc = get_by_slug(city_slug)
    if loc.region != "us":
        return {}
    
    result = {}
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc.lat}&longitude={loc.lon}"
        f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
        f"&forecast_days=3&timezone={get_timezone(city_slug)}"
        f"&models=gfs_seamless"
    )
    
    for attempt in range(3):
        try:
            data = requests.get(url, timeout=(5, 10)).json()
            if "error" not in data:
                for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
                    if date in dates and temp is not None:
                        result[date] = round(temp)
            return result
        except Exception:
            if attempt < 2:
                time.sleep(1)
    raise WeatherAPIError(f"HRRR failed after 3 attempts")


def get_dwd(city_slug: str, dates: list[str]) -> Dict[str, float]:
    """DWD API - German Weather Service (EU only)."""
    loc = get_by_slug(city_slug)
    if loc.region != "eu":
        return {}
    
    result = {}
    try:
        url = f"https://dwd.api.bund.de/weather/forecast/wide/{loc.station}"
        data = requests.get(url, timeout=(5, 10)).json()
        if "error" not in str(data):
            for date in dates:
                for day in data.get("forecasts", []):
                    if day.get("date") == date:
                        temp = day.get("temperature")
                        if temp is not None:
                            result[date] = round(temp)
    except Exception:
        pass
    return result


def get_nws(city_slug: str, dates: list[str]) -> Dict[str, float]:
    """NWS API - US National Weather Service (US only)."""
    loc = get_by_slug(city_slug)
    if loc.region != "us":
        return {}
    
    result = {}
    try:
        url = f"https://api.weather.gov/points/{loc.lat},{loc.lon}"
        points = requests.get(url, timeout=(5, 10)).json()
        forecast_url = points.get("properties", {}).get("forecast")
        if forecast_url:
            forecast = requests.get(forecast_url, timeout=(5, 10)).json()
            for period in forecast.get("properties", {}).get("periods", []):
                date_str = period.get("dateTime", "")
                if date_str[:10] in dates:
                    temp = period.get("temperature")
                    if isinstance(temp, dict):
                        temp = temp.get("value")
                    if temp is not None:
                        result[date_str[:10]] = round(temp)
    except Exception:
        pass
    return result


def get_metar(city_slug: str) -> Optional[float]:
    """Current METAR observation."""
    loc = get_by_slug(city_slug)
    try:
        url = f"https://aviationweather.gov/api/data/metar?ids={loc.station}&format=json"
        data = requests.get(url, timeout=(5, 8)).json()
        if data and isinstance(data, list):
            temp_c = data[0].get("temp")
            if temp_c is not None:
                if loc.unit == "F":
                    return round(float(temp_c) * 9/5 + 32)
                return round(float(temp_c), 1)
    except Exception:
        pass
    return None


def get_actual_temp(city_slug: str, date_str: str) -> Optional[float]:
    """Get actual temperature after market resolution via Open-Meteo + Meteostat fallback."""
    from datetime import datetime

    loc = get_by_slug(city_slug)
    temp_unit = "fahrenheit" if loc.unit == "F" else "celsius"

    # 1) Open-Meteo Archive — gratuit, open-source, sans clé
    try:
        url = (
            "https://archive-api.open-meteo.com/v1/archive"
            f"?latitude={loc.lat}&longitude={loc.lon}"
            f"&start_date={date_str}&end_date={date_str}"
            f"&daily=temperature_2m_max"
            f"&temperature_unit={temp_unit}"
            f"&timezone=auto"
        )
        data = requests.get(url, timeout=(5, 8)).json()
        values = data.get("daily", {}).get("temperature_2m_max", [])

        if values and values[0] is not None:
            return round(float(values[0]), 1 if loc.unit == "C" else 0)

    except Exception as e:
        logger.warning(f"Open-Meteo Archive failed for {city_slug}: {e}")
    
    # 2) Meteostat fallback — gratuit/open-source
    try:
        from meteostat import Point, Daily

        day = datetime.strptime(date_str, "%Y-%m-%d")
        point = Point(loc.lat, loc.lon)
        df = Daily(point, day, day).fetch()

        if not df.empty and "tmax" in df.columns:
            value = df.iloc[0]["tmax"]

            if value is not None:
                if loc.unit == "F":
                    return round((float(value) * 9 / 5) + 32, 0)
                return round(float(value), 1)

    except Exception as e:
        logger.warning(f"Meteostat fallback failed for {city_slug}: {e}")

    return None


def get_forecasts(city_slug: str, dates: list[str]) -> Dict[str, Dict]:
    """Get multi-source forecasts with fallback."""
    now = datetime.now(timezone.utc).isoformat()
    
    ecmwf = get_ecmwf(city_slug, dates)
    hrrr = get_hrrr(city_slug, dates)
    dwd = get_dwd(city_slug, dates)
    nws = get_nws(city_slug, dates)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    
    snapshots = {}
    for date in dates:
        snap = {
            "ts": now,
            "ecmwf": ecmwf.get(date),
            "hrrr": hrrr.get(date),
            "dwd": dwd.get(date),
            "nws": nws.get(date),
            "metar": get_metar(city_slug) if date == today else None,
        }
        
        # Find best forecast
        loc = get_by_slug(city_slug)
        priority = get_forecast_priority(loc.region)
        
        best = None
        best_source = None
        for src in priority:
            if snap.get(src) is not None:
                best = snap[src]
                best_source = src
                break
        
        snap["best"] = best
        snap["best_source"] = best_source
        snapshots[date] = snap
    
    return snapshots