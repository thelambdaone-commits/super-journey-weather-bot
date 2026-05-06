"""
Weather API clients - multiple sources with fallback.
"""
import logging
import math
import requests
from datetime import datetime, timedelta, timezone
from typing import Dict, Optional
from .locations import Location, get_by_slug, get_timezone, get_forecast_priority
from .open_meteo_rate_limiter import rate_limited_get
from .ensemble_optimizer import EnsembleOptimizer
from ..notifications.telegram_control_center import send_incident
from ..notifications.desk_metrics import log_event

logger = logging.getLogger(__name__)
METNO_USER_AGENT = "weatherbot/1.0 github.com/weatherbot"
_METEOSTAT_DAILY_AVAILABLE: bool | None = None


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
            data = rate_limited_get(url, timeout=(5, 10), max_429_retries=0).json()
            if "error" not in data:
                log_event("api_call", provider="ECMWF", ok=True, latency_s=1.0) # Placeholder for now
                for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
                    if date in dates and temp is not None:
                        result[date] = round(temp, 1) if loc.unit == "C" else round(temp)
            return result
        except (Exception,) as e:
            log_event("api_call", provider="ECMWF", ok=False, error=str(e), latency_s=0.0)
            log_event("error", error_type="network", module="weather.apis", message=f"ECMWF attempt {attempt} failed: {e}")
    raise WeatherAPIError(f"ECMWF failed after 3 attempts")


def get_gfs(city_slug: str, dates: list[str]) -> Dict[str, float]:
    """GFS seamless via Open-Meteo."""
    loc = get_by_slug(city_slug)
    temp_unit = "fahrenheit" if loc.unit == "F" else "celsius"

    result = {}
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc.lat}&longitude={loc.lon}"
        f"&daily=temperature_2m_max&temperature_unit={temp_unit}"
        f"&forecast_days=7&timezone={get_timezone(city_slug)}"
        f"&models=gfs_seamless"
    )

    for attempt in range(3):
        try:
            data = rate_limited_get(url, timeout=(5, 10), max_429_retries=0).json()
            if "error" not in data:
                for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
                    if date in dates and temp is not None:
                        result[date] = round(temp, 1) if loc.unit == "C" else round(temp)
            return result
        except (Exception,) as e:
            logger.warning("GFS attempt %s failed for %s: %s", attempt + 1, city_slug, e)
    raise WeatherAPIError(f"GFS failed after 3 attempts")


def get_hrrr(city_slug: str, dates: list[str]) -> Dict[str, float]:
    """HRRR via Open-Meteo (US only)."""
    loc = get_by_slug(city_slug)
    if loc.region != "us":
        return {}

    result = {}
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={loc.lat}&longitude={loc.lon}"
        f"&daily=temperature_2m_max&temperature_unit=fahrenheit"
        f"&forecast_days=3&timezone={get_timezone(city_slug)}"
        f"&models=hrrr"
    )

    for attempt in range(3):
        try:
            data = rate_limited_get(url, timeout=(5, 10), max_429_retries=0).json()
            if "error" not in data:
                for date, temp in zip(data["daily"]["time"], data["daily"]["temperature_2m_max"]):
                    if date in dates and temp is not None:
                        result[date] = round(temp)
            return result
        except (Exception,) as e:
            logger.warning("HRRR attempt %s failed for %s: %s", attempt + 1, city_slug, e)
    raise WeatherAPIError(f"HRRR failed after 3 attempts")


def get_dwd(city_slug: str, dates: list[str]) -> Dict[str, float]:
    """DWD API - German Weather Service (EU only). Disabled by default."""
    # Check if DWD is enabled in config
    from ..weather.config import get_config
    config = get_config()
    if not getattr(config, "dwd_enabled", False):
        return {}

    loc = get_by_slug(city_slug)
    if loc.region != "eu":
        return {}
    result = {}

    url = f"https://dwd.api.bund.de/weather/forecast/wide/{loc.station}"
    for attempt in range(3):
        try:
            data = rate_limited_get(url, timeout=(5, 10), max_429_retries=0).json()
            if "error" not in data:
                for item in data.get("forecasts", []):
                    date = item.get("date", "")
                    if date in dates:
                        result[date] = item.get("temp_max", 0)
                return result
        except (Exception,) as e:
            logger.warning("DWD failed for %s: %s", city_slug, e)
            if attempt == 2:
                raise WeatherAPIError(f"DWD failed: {e}") from e
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
    except (Exception,) as e:
        logger.warning("NWS failed for %s: %s", city_slug, e)
    return result


def get_metno(city_slug: str, dates: list[str]) -> Dict[str, float]:
    """MET Norway Locationforecast fallback, global and keyless."""
    loc = get_by_slug(city_slug)
    result: Dict[str, float] = {}

    try:
        url = "https://api.met.no/weatherapi/locationforecast/2.0/compact"
        response = requests.get(
            url,
            params={"lat": loc.lat, "lon": loc.lon},
            headers={"User-Agent": METNO_USER_AGENT},
            timeout=(5, 10),
        )
        if response.status_code == 429:
            logger.warning("MET Norway rate limited for %s", city_slug)
            return result
        response.raise_for_status()
        data = response.json()

        daily_values: dict[str, list[float]] = {date: [] for date in dates}
        for item in data.get("properties", {}).get("timeseries", []):
            time_str = item.get("time", "")
            date_str = time_str[:10]
            if date_str not in daily_values:
                continue

            details = item.get("data", {}).get("instant", {}).get("details", {})
            temp = details.get("air_temperature")
            if temp is not None:
                daily_values[date_str].append(float(temp))

        for date, values in daily_values.items():
            if not values:
                continue
            temp_c = max(values)
            if loc.unit == "F":
                result[date] = round((temp_c * 9 / 5) + 32)
            else:
                result[date] = round(temp_c, 1)
    except (Exception,) as e:
        logger.warning(f"MET Norway fallback failed for {city_slug}: {e}")

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
    except (Exception,) as e:
        logger.warning("METAR failed for %s: %s", city_slug, e)
    return None


def get_actual_temp(city_slug: str, date_str: str, station: str = "GENERIC") -> float | None:
    """
    Get the official historical temperature for a city and date.
    Supports station-specific overrides for V3 precision.
    """
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
        data = rate_limited_get(url, timeout=(5, 8)).json()
        values = data.get("daily", {}).get("temperature_2m_max", [])

        if values and values[0] is not None:
            return round(float(values[0]), 1 if loc.unit == "C" else 0)

    except (Exception,) as e:
        logger.warning(f"Open-Meteo Archive failed for {city_slug}: {e}")

    # 2) Meteostat fallback — gratuit/open-source.
    # Meteostat 2.x no longer exposes the legacy Daily class used here, so
    # disable this fallback quietly when the installed package lacks it.
    try:
        global _METEOSTAT_DAILY_AVAILABLE
        if _METEOSTAT_DAILY_AVAILABLE is False:
            return None

        try:
            from meteostat import Daily, Point
        except ImportError:
            _METEOSTAT_DAILY_AVAILABLE = False
            logger.info("Meteostat Daily fallback unavailable; skipping fallback")
            return None

        _METEOSTAT_DAILY_AVAILABLE = True

        day = datetime.strptime(date_str, "%Y-%m-%d")
        point = Point(loc.lat, loc.lon)
        df = Daily(point, day, day).fetch()

        if not df.empty and "tmax" in df.columns:
            value = df.iloc[0]["tmax"]

            if value is not None:
                if loc.unit == "F":
                    return round((float(value) * 9 / 5) + 32, 0)
                return round(float(value), 1)

    except (Exception,) as e:
        logger.warning(f"Meteostat fallback failed for {city_slug}: {e}")

    return None


def get_forecasts(city_slug: str, dates: list[str]) -> Dict[str, Dict]:
    """Get multi-source forecasts with fallback (parallelized)."""
    now = datetime.now(timezone.utc).isoformat()
    optimizer = EnsembleOptimizer()

    # Parallel fetch using ThreadPoolExecutor
    from concurrent.futures import ThreadPoolExecutor, as_completed

    def safe_fetch(func, name):
        try:
            return func(city_slug, dates)
        except (Exception,) as e:
            logger.warning(f"{name} failed for {city_slug}: {e}")
            if name == "ECMWF":
                send_incident(f"ECMWF degraded for {city_slug}. Fallback active.")
            return {}

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(safe_fetch, get_ecmwf, "ECMWF"): "ecmwf",
            executor.submit(safe_fetch, get_hrrr, "HRRR"): "hrrr",
            executor.submit(safe_fetch, get_gfs, "GFS"): "gfs",
            executor.submit(safe_fetch, get_metno, "MET Norway"): "metno",
            executor.submit(safe_fetch, get_dwd, "DWD"): "dwd",
            executor.submit(safe_fetch, get_nws, "NWS"): "nws",
        }
        results = {}
        for future in as_completed(futures):
            name = futures[future]
            try:
                results[name] = future.result()
            except (Exception,) as e:
                results[name] = {}
                logger.warning(f"{name} future failed for {city_slug}: {e}")

        ecmwf = results.get("ecmwf", {})
        hrrr = results.get("hrrr", {})
        gfs = results.get("gfs", {})
        metno = results.get("metno", {})
        dwd = results.get("dwd", {})
        nws = results.get("nws", {})
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    snapshots = {}
    for date in dates:
        snap = {
            "ts": now,
            "ecmwf": ecmwf.get(date),
            "hrrr": hrrr.get(date),
            "gfs": gfs.get(date),
            "dwd": dwd.get(date),
            "nws": nws.get(date),
            "metno": metno.get(date),
            "metar": get_metar(city_slug) if date == today else None,
        }
        loc = get_by_slug(city_slug)
        optimal = optimizer.optimize(city_slug, loc.unit, snap)
        if optimal:
            snap["optimal"] = optimal.temp
            snap["optimal_sigma"] = optimal.sigma
            snap["optimal_confidence"] = optimal.confidence
            snap["optimal_weights"] = optimal.weights
            snap["optimal_primary_source"] = optimal.primary_source

        # Find best forecast
        priority = get_forecast_priority(loc.region)

        best = None
        best_source = None
        if optimal and len(optimal.weights) >= 2:
            best = optimal.temp
            best_source = optimal.primary_source
        else:
            for src in priority:
                if snap.get(src) is not None:
                    best = snap[src]
                    best_source = src
                    break

        snap["best"] = best
        snap["best_source"] = best_source
        snapshots[date] = snap

    return snapshots
