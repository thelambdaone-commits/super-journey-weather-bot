"""
Weather locations and constants.
"""
from dataclasses import dataclass
from typing import Dict


@dataclass
class Location:
    """Weather location with station info."""
    slug: str
    name: str
    lat: float
    lon: float
    station: str
    unit: str  # "F" or "C"
    region: str  # "us", "eu", "asia", "sa", "ca", "oc"


LOCATIONS: Dict[str, Location] = {
    "nyc":          Location("nyc", "New York City", 40.7772,  -73.8726, "KLGA", "F", "us"),
    "chicago":      Location("chicago", "Chicago", 41.9742,  -87.9073, "KORD", "F", "us"),
    "miami":        Location("miami", "Miami", 25.7959,  -80.2870, "KMIA", "F", "us"),
    "dallas":       Location("dallas", "Dallas", 32.8471,  -96.8518, "KDAL", "F", "us"),
    "seattle":      Location("seattle", "Seattle", 47.4502, -122.3088, "KSEA", "F", "us"),
    "atlanta":      Location("atlanta", "Atlanta", 33.6407,  -84.4277, "KATL", "F", "us"),
    "london":       Location("london", "London", 51.5048,   0.0495, "EGLC", "C", "eu"),
    "paris":        Location("paris", "Paris", 48.9962,   2.5979, "LFPG", "C", "eu"),
    "munich":       Location("munich", "Munich", 48.3537,  11.7750, "EDDM", "C", "eu"),
    "ankara":       Location("ankara", "Ankara", 40.1281,  32.9951, "LTAC", "C", "eu"),
    "seoul":        Location("seoul", "Seoul", 37.4691, 126.4505, "RKSI", "C", "asia"),
    "tokyo":        Location("tokyo", "Tokyo", 35.7647, 140.3864, "RJTT", "C", "asia"),
    "shanghai":     Location("shanghai", "Shanghai", 31.1443, 121.8083, "ZSPD", "C", "asia"),
    "singapore":     Location("singapore", "Singapore", 1.3502, 103.9940, "WSSS", "C", "asia"),
    "lucknow":      Location("lucknow", "Lucknow", 26.7606, 80.8893, "VILK", "C", "asia"),
    "tel-aviv":     Location("tel-aviv", "Tel Aviv", 32.0114, 34.8867, "LLBG", "C", "asia"),
    "toronto":      Location("toronto", "Toronto", 43.6772, -79.6306, "CYYZ", "C", "ca"),
    "sao-paulo":    Location("sao-paulo", "Sao Paulo", -23.4356, -46.4731, "SBGR", "C", "sa"),
    "buenos-aires": Location("buenos-aires", "Buenos Aires", -34.8222, -58.5358, "SAEZ", "C", "sa"),
    "wellington":  Location("wellington", "Wellington", -41.3272, 174.8052, "NZWN", "C", "oc"),
}


TIMEZONES: Dict[str, str] = {
    "nyc": "America/New_York", "chicago": "America/Chicago",
    "miami": "America/New_York", "dallas": "America/Chicago",
    "seattle": "America/Los_Angeles", "atlanta": "America/New_York",
    "london": "Europe/London", "paris": "Europe/Paris",
    "munich": "Europe/Berlin", "ankara": "Europe/Istanbul",
    "seoul": "Asia/Seoul", "tokyo": "Asia/Tokyo",
    "shanghai": "Asia/Shanghai", "singapore": "Asia/Singapore",
    "lucknow": "Asia/Kolkata", "tel-aviv": "Asia/Jerusalem",
    "toronto": "America/Toronto", "sao-paulo": "America/Sao_Paulo",
    "buenos-aires": "America/Argentina/Buenos_Aires", "wellington": "Pacific/Auckland",
}


MONTHS = ["january", "february", "march", "april", "may", "june",
         "july", "august", "september", "october", "november", "december"]


# Forecast source priority by region
FORECAST_PRIORITY = {
    "us": ["hrrr", "nws", "ecmwf", "gfs", "metno", "metar"],
    "eu": ["ecmwf", "dwd", "gfs", "metno", "metar"],
    "asia": ["ecmwf", "gfs", "metno", "metar"],
    "sa": ["ecmwf", "gfs", "metno", "metar"],
    "oc": ["ecmwf", "gfs", "metno", "metar"],
    "ca": ["ecmwf", "gfs", "metno", "metar"],
}


def get_by_slug(slug: str) -> Location:
    """Get location by slug."""
    return LOCATIONS[slug]


def get_all() -> Dict[str, Location]:
    """Get all locations."""
    return LOCATIONS


def get_timezone(slug: str) -> str:
    """Get timezone for location."""
    return TIMEZONES.get(slug, "UTC")


def get_forecast_priority(region: str) -> list[str]:
    """Get forecast source priority for region."""
    return FORECAST_PRIORITY.get(region, ["ecmwf", "metar"])
