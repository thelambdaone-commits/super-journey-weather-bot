#!/usr/bin/env python3
"""Polling real-time actual temperatures via Open-Meteo Archive."""
import argparse
import json
from datetime import date, timedelta

from src.weather.open_meteo_rate_limiter import rate_limited_get
from src.weather.locations import LOCATIONS


def get_actual_temp(lat: float, lon: float, date_str: str, unit: str = "celsius"):
    """Fetch actual temperature from Open-Meteo Archive."""
    temp_unit = "fahrenheit" if unit == "F" else "celsius"
    
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": date_str,
        "end_date": date_str,
        "daily": "temperature_2m_max",
        "temperature_unit": temp_unit,
        "timezone": "auto",
    }
    
    try:
        r = rate_limited_get(url, params=params, timeout=15)
        data = r.json()
        temps = data.get("daily", {}).get("temperature_2m_max", [])
        if temps and temps[0] is not None:
            return round(float(temps[0]), 1)
    except (Exception,) as e:
        print(f"Error: {e}")
    
    return None


def poll_date(date_str: str):
    """Poll actual temperatures for all cities."""
    results = {}
    
    for slug, loc in LOCATIONS.items():
        temp = get_actual_temp(loc.lat, loc.lon, date_str, loc.unit)
        if temp:
            results[slug] = {"name": loc.name, "actual": temp, "unit": loc.unit}
        
    return results


def main():
    parser = argparse.ArgumentParser(description="Poll actual temps")
    parser.add_argument("--date", type=str, default=None)
    parser.add_argument("--days", type=int, default=0)
    parser.add_argument("--city", type=str)
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    
    if args.date:
        date_str = args.date
    elif args.days > 0:
        d = date.today() - timedelta(days=args.days)
        date_str = d.isoformat()
    else:
        date_str = date.today().isoformat()
    
    if args.city:
        loc = LOCATIONS.get(args.city)
        if not loc:
            print(f"Unknown: {args.city}")
            exit(1)
        temp = get_actual_temp(loc.lat, loc.lon, date_str, loc.unit)
        print(f"{args.city}: {temp}°{loc.unit}" if temp else f"{args.city}: N/A")
        return
    
    print(f"=== POLLING: {date_str} ===")
    results = poll_date(date_str)
    
    if args.json:
        print(json.dumps(results, indent=2))
    else:
        print(f"{'City':<15} {'Name':<15} {'Temp':<8}")
        print("-" * 40)
        for slug, data in sorted(results.items()):
            print(f"{slug:<15} {data['name']:<15} {data['actual']}°{data['unit']}")
        print(f"\nTotal: {len(results)}/{len(LOCATIONS)}")


if __name__ == "__main__":
    main()
