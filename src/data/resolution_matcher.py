"""
Resolution Matcher: Aligns Polymarket market resolutions with weather ground truth.
Matches resolved markets to actual weather data (NOAA, Open-Meteo archive).
"""
from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional


@dataclass
class ResolutionMatch:
    """Result of matching a market to weather ground truth."""

    market_id: str
    question: str
    city: str
    date: str
    predicted_bucket: str
    actual_bucket: str
    predicted_temp: Optional[float]
    actual_temp: Optional[float]
    outcome: str
    match_confidence: float
    source: str


@dataclass
class MatchingReport:
    """Report from resolution matching operation."""

    markets_processed: int
    matches_found: int
    match_rate: float
    avg_confidence: float
    outcomes: dict[str, int]
    cities_matched: list[str]
    success: bool


CITY_PATTERNS = {
    "new york": "nyc",
    "nyc": "nyc",
    "new york city": "nyc",
    "chicago": "chicago",
    "los angeles": "la",
    "london": "london",
    "paris": "paris",
    "tokyo": "tokyo",
    "miami": "miami",
    "seattle": "seattle",
    "boston": "boston",
    "sf": "san-francisco",
    "san francisco": "san-francisco",
    "atlanta": "atlanta",
    "dallas": "dallas",
    "houston": "houston",
    "phoenix": "phoenix",
    "denver": "denver",
    "toronto": "toronto",
    "singapore": "singapore",
    "hong kong": "hong-kong",
    "shanghai": "shanghai",
    "beijing": "beijing",
    "sydney": "sydney",
    "melbourne": "melbourne",
    "dubai": "dubai",
    "mumbai": "mumbai",
    "delhi": "delhi",
    "moscow": "moscow",
    "berlin": "berlin",
    "amsterdam": "amsterdam",
    "madrid": "madrid",
    "rome": "rome",
    "milan": "milan",
    "zurich": "zurich",
    "vienna": "vienna",
    "stockholm": "stockholm",
    "oslo": "oslo",
    "copenhagen": "copenhagen",
    "helsinki": "helsinki",
    "dublin": "dublin",
    "london": "london",
    "manchester": "manchester",
    "birmingham": "birmingham",
    "milan": "milan",
    "buenos aires": "buenos-aires",
    "sao paulo": "sao-paulo",
    "mexico city": "mexico-city",
}


class ResolutionMatcher:
    """Match Polymarket resolutions to weather ground truth."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.markets_dir = self.data_dir / "markets_historical"
        self._city_coords = self._load_city_coords()

    def _load_city_coords(self) -> dict[str, tuple[float, float]]:
        """Load known city coordinates."""
        return {
            "nyc": (40.7128, -74.0060),
            "chicago": (41.8781, -87.6298),
            "london": (51.5074, -0.1278),
            "paris": (48.8566, 2.3522),
            "tokyo": (35.6762, 139.6503),
            "miami": (25.7617, -80.1918),
            "seattle": (47.6062, -122.3321),
            "los-angeles": (34.0522, -118.2437),
            "san-francisco": (37.7749, -122.4194),
            "atlanta": (33.7490, -84.3880),
            "dallas": (32.7767, -96.7970),
            "houston": (29.7604, -95.3698),
            "phoenix": (33.4484, -112.0740),
            "denver": (39.7392, -104.9903),
            "boston": (42.3601, -71.0589),
            "toronto": (43.6532, -79.3832),
            "singapore": (1.3521, 103.8198),
            "shanghai": (31.2304, 121.4737),
            "beijing": (39.9042, 116.4074),
            "hong-kong": (22.3193, 114.1694),
            "sydney": (-33.8688, 151.2093),
            "mumbai": (19.0760, 72.8777),
            "delhi": (28.7041, 77.1025),
            "dubai": (25.2048, 55.2708),
            "buenos-aires": (-34.6037, -58.3816),
            "sao-paulo": (-23.5505, -46.6333),
        }

    def extract_city_from_question(self, question: str) -> Optional[str]:
        """Extract city name from market question."""
        question_lower = question.lower()

        for pattern, city in CITY_PATTERNS.items():
            if pattern in question_lower:
                return city

        return None

    def extract_date_from_question(self, question: str) -> Optional[str]:
        """Extract date from market question."""
        date_patterns = [
            r"(january|february|march|april|may|june|july|august|september|october|november|december)\s+\d{1,2}",
            r"\d{4}-\d{2}-\d{2}",
            r"\d{1,2}/\d{1,2}/\d{4}",
        ]

        for pattern in date_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                date_str = match.group()
                try:
                    if "-" in date_str:
                        dt = datetime.strptime(date_str, "%Y-%m-%d")
                    elif "/" in date_str:
                        dt = datetime.strptime(date_str, "%m/%d/%Y")
                    else:
                        dt = datetime.strptime(date_str, "%B %d")
                    return dt.strftime("%Y-%m-%d")
                except (Exception,) as e:
                    pass

        return None

    def extract_temp_from_question(self, question: str) -> Optional[float]:
        """Extract target temperature from question."""
        temp_patterns = [
            r"(\d+)\s*°?[CF]",
            r"temperature\s+(?:be\s+)?(?:above|below|at\s+)?(\d+)",
            r"(?:exceed|surpass|higher than)\s+(\d+)",
            r"(?:below|under|less than)\s+(\d+)",
        ]

        for pattern in temp_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                try:
                    return float(match.group(1))
                except (Exception,) as e:
                    pass

        return None

    def extract_bucket_from_question(self, question: str, city: str) -> tuple[Optional[float], Optional[float]]:
        """Extract temperature bucket from question."""
        bucket_patterns = [
            r"between\s+(\d+)\s*(?:and|°?)\s*(\d+)",
            r"(\d+)\s*°?[CF]\s+(?:or\s+)?(?:above|below)",
            r"(?:above|below)\s+(\d+)",
            r"(\d+)\s*°?[CF]\s+or\s+higher",
        ]

        unit = "F" if any(f in question for f in ["°F", "°F", "F on"]) else "C"

        for pattern in bucket_patterns:
            match = re.search(pattern, question, re.IGNORECASE)
            if match:
                groups = match.groups()
                if len(groups) >= 2:
                    return float(groups[0]), float(groups[1])
                elif len(groups) == 1:
                    temp = float(groups[0])
                    bucket_width = 2 if unit == "F" else 1
                    return temp, temp + bucket_width

        temp = self.extract_temp_from_question(question)
        if temp:
            bucket_width = 2 if unit == "F" else 1
            return temp, temp + bucket_width

        return None, None

    def match_market_to_weather(
        self,
        market: dict[str, Any],
    ) -> Optional[ResolutionMatch]:
        """Match a single market to weather ground truth."""

        if not market.get("is_resolved"):
            return None

        question = market.get("question", "")
        city = self.extract_city_from_question(question)

        if not city:
            return None

        date = self.extract_date_from_question(question)
        bucket_low, bucket_high = self.extract_bucket_from_question(question, city)

        actual_temp = market.get("actual_temp") or self._get_historical_temp(city, date)
        predicted_temp = market.get("predicted_temp") or self.extract_temp_from_question(question)

        if actual_temp is None:
            return None

        actual_bucket = f"{actual_temp}C"

        if bucket_low is not None and bucket_high is not None:
            won = bucket_low <= actual_temp <= bucket_high
        else:
            won = abs(predicted_temp - actual_temp) < 3 if predicted_temp else False

        match_confidence = self._compute_confidence(city, date, actual_temp)

        return ResolutionMatch(
            market_id=market.get("id", market.get("market_id", "")),
            question=question,
            city=city,
            date=date or "",
            predicted_bucket=f"{bucket_low}-{bucket_high}" if bucket_low and bucket_high else "",
            actual_bucket=actual_bucket,
            predicted_temp=predicted_temp,
            actual_temp=actual_temp,
            outcome="YES" if won else "NO",
            match_confidence=match_confidence,
            source="historical" if not market.get("actual_temp") else "market",
        )

    def _get_historical_temp(self, city: str, date: Optional[str]) -> Optional[float]:
        """Fetch historical temperature for city/date from Open-Meteo archive."""
        if not city or not date:
            return None

        coords = self._city_coords.get(city)
        if not coords:
            return None

        try:
            lat, lon = coords
            url = f"https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": date,
                "end_date": date,
                "daily": "temperature_2m_max",
                "timezone": "auto",
            }

            try:
                from src.weather.open_meteo_rate_limiter import rate_limited_get
                response = rate_limited_get(url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    temps = data.get("daily", {}).get("temperature_2m_max", [])
                    if temps:
                        return float(temps[0])
            except (Exception,) as e:
                pass

        except (Exception,) as e:
            pass

        return None

    def _compute_confidence(self, city: str, date: Optional[str], actual_temp: Optional[float]) -> float:
        """Compute confidence of the match."""
        confidence = 0.5

        if city in self._city_coords:
            confidence += 0.2

        if date:
            confidence += 0.15

        if actual_temp is not None:
            confidence += 0.15

        return min(confidence, 1.0)

    def match_all_markets(self, markets: list[dict[str, Any]]) -> list[ResolutionMatch]:
        """Match all markets to weather ground truth."""
        matches = []

        for market in markets:
            match = self.match_market_to_weather(market)
            if match:
                matches.append(match)

        return matches

    def run(self, markets_dir: Optional[Path] = None) -> MatchingReport:
        """Run matching on all historical markets."""
        markets_dir = markets_dir or self.markets_dir

        markets = []
        for path in markets_dir.glob("*.json"):
            try:
                market = json.loads(path.read_text(encoding="utf-8"))
                markets.append(market)
            except (Exception,) as e:
                continue

        matches = self.match_all_markets(markets)

        if not matches:
            return MatchingReport(
                markets_processed=len(markets),
                matches_found=0,
                match_rate=0,
                avg_confidence=0,
                outcomes={},
                cities_matched=[],
                success=False,
            )

        outcomes = {}
        cities = set()

        for match in matches:
            outcomes[match.outcome] = outcomes.get(match.outcome, 0) + 1
            cities.add(match.city)

        avg_confidence = sum(m.match_confidence for m in matches) / len(matches)

        return MatchingReport(
            markets_processed=len(markets),
            matches_found=len(matches),
            match_rate=len(matches) / len(markets) if markets else 0,
            avg_confidence=avg_confidence,
            outcomes=outcomes,
            cities_matched=list(cities),
            success=True,
        )

    def save_matches(self, matches: list[ResolutionMatch], output_path: Optional[Path] = None) -> Path:
        """Save matches to file."""
        output_path = output_path or self.data_dir / "resolution_matches.jsonl"
        output_path.parent.mkdir(exist_ok=True)

        with output_path.open("w", encoding="utf-8") as f:
            for match in matches:
                f.write(json.dumps(match.__dict__, ensure_ascii=False) + "\n")

        return output_path


def run_matching(data_dir: str = "data") -> MatchingReport:
    """Convenience function to run resolution matching."""
    matcher = ResolutionMatcher(data_dir=data_dir)
    return matcher.run()


def format_matching_report(report: MatchingReport) -> list[str]:
    """Format matching report for CLI."""
    lines = [
        f"\n{'='*50}",
        "RESOLUTION MATCHING REPORT",
        f"{'='*50}",
        f"Markets processed: {report.markets_processed}",
        f"Matches found: {report.matches_found}",
        f"Match rate: {report.match_rate:.1%}",
        f"Avg confidence: {report.avg_confidence:.1%}",
        "",
        "Outcomes:",
    ]

    for outcome, count in sorted(report.outcomes.items()):
        lines.append(f"  {outcome}: {count}")

    if report.cities_matched:
        lines.append("")
        lines.append(f"Cities matched: {', '.join(sorted(report.cities_matched)[:10])}")

    status = "✅ SUCCESS" if report.success else "❌ FAILED"
    lines.append("")
    lines.append(f"Status: {status}")
    lines.append(f"{'='*50}\n")

    return lines
