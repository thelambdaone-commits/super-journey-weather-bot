"""
Station Map V3 - Surgical precision for market settlement.
Maps Polymarket cities to their official reference stations.
"""

STATION_MAP = {
    "PARIS": {"code": "LFPG", "name": "Charles de Gaulle Intl", "provider": "METAR"},
    "LONDON": {"code": "EGLL", "name": "London Heathrow", "provider": "METAR"},
    "NEW YORK CITY": {"code": "KCPK", "name": "Central Park", "provider": "NOAA"},
    "CHICAGO": {"code": "KORD", "name": "O'Hare Intl", "provider": "NOAA"},
    "DALLAS": {"code": "KDFW", "name": "Dallas/Fort Worth Intl", "provider": "NOAA"},
    "ATLANTA": {"code": "KATL", "name": "Hartsfield-Jackson Intl", "provider": "NOAA"},
    "MIAMI": {"code": "KMIA", "name": "Miami Intl", "provider": "NOAA"},
    "TOKYO": {"code": "RJTT", "name": "Haneda Intl", "provider": "JMA"},
    "SINGAPORE": {"code": "WSSS", "name": "Changi Intl", "provider": "METAR"},
    "SEOUL": {"code": "RKSS", "name": "Gimpo Intl", "provider": "METAR"},
    "SHANGHAI": {"code": "ZSSS", "name": "Hongqiao Intl", "provider": "METAR"},
    "MUNICH": {"code": "EDDM", "name": "Munich Airport", "provider": "METAR"},
    "ANKARA": {"code": "LTAC", "name": "Esenboga Intl", "provider": "METAR"},
    "WELLINGTON": {"code": "NZWN", "name": "Wellington Intl", "provider": "METAR"},
    "SAO PAULO": {"code": "SBSP", "name": "Congonhas Airport", "provider": "METAR"},
    "BUENOS AIRES": {"code": "SABE", "name": "Jorge Newbery Airfield", "provider": "METAR"},
    "TEL AVIV": {"code": "LLBG", "name": "Ben Gurion Intl", "provider": "METAR"},
    "LUCKNOW": {"code": "VILK", "name": "Chaudhary Charan Singh Intl", "provider": "METAR"},
}

def get_station_info(city: str) -> dict:
    """Get the official reference station for a city."""
    return STATION_MAP.get(city.upper(), {"code": "UNKNOWN", "name": city, "provider": "GENERIC"})
