"""
Resolution Validator: Cross-validates Polymarket outcomes with weather ground truth.
Ensures data quality before ML training.
"""
from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


@dataclass
class ValidationResult:
    """Result of resolution validation."""

    market_id: str
    question: str
    city: str

    polymarket_outcome: Optional[str]
    weather_outcome: Optional[str]

    polymarket_temp: Optional[float]
    weather_archive_temp: Optional[float]

    consistency: str
    confidence: float

    notes: list[str] = field(default_factory=list)


@dataclass
class ValidationReport:
    """Full validation report."""

    total_markets: int
    validated: int
    conflicts: int
    low_confidence: int

    consistency_rate: float
    avg_confidence: float

    cities: list[str]
    outcomes: dict[str, int]

    success: bool


class ResolutionValidator:
    """Validates Polymarket resolutions against weather ground truth."""

    def __init__(self, data_dir: str = "data"):
        self.data_dir = Path(data_dir)
        self.markets_dir = self.data_dir / "markets_real"
        self._city_coords = self._load_city_coords()

    def _load_city_coords(self) -> dict[str, tuple[float, float]]:
        """Load city coordinates."""
        return {
            "nyc": (40.7128, -74.0060),
            "chicago": (41.8781, -87.6298),
            "miami": (25.7617, -80.1918),
            "seattle": (47.6062, -122.3321),
            "atlanta": (33.7490, -84.3880),
            "dallas": (32.7767, -96.7970),
            "los-angeles": (34.0522, -118.2437),
            "boston": (42.3601, -71.0589),
            "denver": (39.7392, -104.9903),
            "phoenix": (33.4484, -112.0740),
            "london": (51.5074, -0.1278),
            "paris": (48.8566, 2.3522),
            "tokyo": (35.6762, 139.6503),
        }

    def validate_single(self, market: dict) -> ValidationResult:
        """Validate a single market resolution."""
        market_id = market.get("market_id", "")
        question = market.get("question", "")
        city = market.get("city", "")

        polymarket_outcome = market.get("resolved_outcome")
        polymarket_temp = market.get("actual_temp")

        if not market.get("is_resolved"):
            return ValidationResult(
                market_id=market_id,
                question=question,
                city=city,
                polymarket_outcome=None,
                weather_outcome=None,
                polymarket_temp=polymarket_temp,
                weather_archive_temp=None,
                consistency="unresolved",
                confidence=0,
                notes=["Market not resolved"],
            )

        weather_archive_temp = self._fetch_weather_archive(city, market.get("datetime"))

        consistency, weather_outcome, confidence, notes = self._check_consistency(
            polymarket_outcome,
            polymarket_temp,
            weather_archive_temp,
            market.get("target_temp"),
        )

        return ValidationResult(
            market_id=market_id,
            question=question,
            city=city,
            polymarket_outcome=polymarket_outcome,
            weather_outcome=weather_outcome,
            polymarket_temp=polymarket_temp,
            weather_archive_temp=weather_archive_temp,
            consistency=consistency,
            confidence=confidence,
            notes=notes,
        )

    def _fetch_weather_archive(self, city: str, timestamp_str: Optional[str]) -> Optional[float]:
        """Fetch historical weather from Open-Meteo archive."""
        if not city:
            return None

        coords = self._city_coords.get(city)
        if not coords:
            coords = self._guess_coords(city)

        if not coords:
            return None

        lat, lon = coords

        if timestamp_str:
            try:
                dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                date = dt.strftime("%Y-%m-%d")
            except (Exception,) as e:
                date = datetime.now().strftime("%Y-%m-%d")
        else:
            date = datetime.now().strftime("%Y-%m-%d")

        try:
            import requests
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": date,
                "end_date": date,
                "daily": "temperature_2m_max",
                "timezone": "auto",
            }

            response = requests.get(url, params=params, timeout=10)
            if response.status_code == 200:
                data = response.json()
                temps = data.get("daily", {}).get("temperature_2m_max", [])
                if temps and temps[0]:
                    return float(temps[0])

        except (Exception,) as e:
            pass

        return None

    def _guess_coords(self, city: str) -> Optional[tuple[float, float]]:
        """Guess coordinates from city name."""
        city_lower = city.lower()
        guess_map = {
            "nyc": (40.7128, -74.0060),
            "new york": (40.7128, -74.0060),
            "chicago": (41.8781, -87.6298),
            "la": (34.0522, -118.2437),
            "los angeles": (34.0522, -118.2437),
            "houston": (29.7604, -95.3698),
            "phoenix": (33.4484, -112.0740),
        }
        return guess_map.get(city_lower)

    def _check_consistency(
        self,
        polymarket_outcome: Optional[str],
        polymarket_temp: Optional[float],
        weather_archive_temp: Optional[float],
        target_temp: Optional[float],
    ) -> tuple[str, Optional[str], float, list[str]]:
        """Check consistency between Polymarket and weather data."""
        notes = []

        if polymarket_outcome is None:
            return "unknown", None, 0.0, ["No Polymarket outcome"]

        if weather_archive_temp is None:
            if polymarket_temp is not None:
                return "unverified", polymarket_outcome, 0.5, ["Weather archive not available, using Polymarket temp"]
            return "unknown", None, 0.0, ["No weather data available"]

        if target_temp is None:
            return "unverified", polymarket_outcome, 0.3, ["No target temperature"]

        bucket_width = 2
        in_bucket = bucket_width - abs(weather_archive_temp - target_temp) <= bucket_width

        if in_bucket:
            weather_outcome = "YES"
        else:
            weather_outcome = "NO"

        if polymarket_outcome == weather_outcome:
            consistency = "consistent"
            confidence = 0.9
            notes.append("Polymarket and weather data agree")
        else:
            temp_diff = abs(weather_archive_temp - target_temp) if weather_archive_temp else 999
            if temp_diff <= bucket_width:
                consistency = "likely_error"
                confidence = 0.4
                notes.append(f"Possible error: Polymarket={polymarket_outcome}, Weather={weather_outcome}")
            else:
                consistency = "conflict"
                confidence = 0.0
                notes.append(f"CRITICAL: Outcome mismatch")

        return consistency, weather_outcome, confidence, notes

    def validate_all(self) -> list[ValidationResult]:
        """Validate all saved market states."""
        if not self.markets_dir.exists():
            return []

        results = []
        for path in sorted(self.markets_dir.glob("*.json")):
            try:
                market = json.loads(path.read_text(encoding="utf-8"))
                result = self.validate_single(market)
                results.append(result)
            except (Exception,) as e:
                continue

        return results

    def run(self) -> ValidationReport:
        """Run full validation."""
        results = self.validate_all()

        if not results:
            return ValidationReport(
                total_markets=0, validated=0, conflicts=0, low_confidence=0,
                consistency_rate=0, avg_confidence=0, cities=[], outcomes={},
                success=False,
            )

        validated = [r for r in results if r.confidence > 0]
        conflicts = [r for r in results if r.consistency == "conflict"]
        low_confidence = [r for r in results if r.confidence < 0.5 and r.confidence > 0]

        consistent = sum(1 for r in results if r.consistency == "consistent")
        consistency_rate = consistent / len(results) if results else 0

        avg_confidence = sum(r.confidence for r in results) / len(results) if results else 0

        cities = list(set(r.city for r in results if r.city))
        outcomes: dict[str, int] = {}
        for r in results:
            if r.polymarket_outcome:
                outcomes[r.polymarket_outcome] = outcomes.get(r.polymarket_outcome, 0) + 1

        return ValidationReport(
            total_markets=len(results),
            validated=len(validated),
            conflicts=len(conflicts),
            low_confidence=len(low_confidence),
            consistency_rate=consistency_rate,
            avg_confidence=avg_confidence,
            cities=sorted(cities),
            outcomes=outcomes,
            success=True,
        )

    def save_validations(self, results: list[ValidationResult]) -> Path:
        """Save validation results."""
        output_path = self.data_dir / "resolution_validated.jsonl"
        output_path.parent.mkdir(exist_ok=True)

        count = 0
        with output_path.open("w", encoding="utf-8") as f:
            for r in results:
                f.write(json.dumps(r.__dict__, ensure_ascii=False) + "\n")
                count += 1

        return output_path


def run_validation(data_dir: str = "data") -> ValidationReport:
    """Convenience function to run validation."""
    validator = ResolutionValidator(data_dir=data_dir)
    return validator.run()


def format_validation_report(report: ValidationReport) -> list[str]:
    """Format validation report."""
    lines = [
        f"\n{'='*50}",
        "RESOLUTION VALIDATION REPORT",
        f"{'='*50}",
        f"Total markets: {report.total_markets}",
        f"Validated: {report.validated}",
        f"Conflicts: {report.conflicts}",
        f"Low confidence: {report.low_confidence}",
        "",
        f"Consistency rate: {report.consistency_rate:.1%}",
        f"Avg confidence: {report.avg_confidence:.1%}",
        "",
        f"Cities: {', '.join(report.cities) if report.cities else 'none'}",
    ]

    if report.outcomes:
        lines.append("")
        lines.append("Outcomes:")
        for outcome, count in sorted(report.outcomes.items()):
            lines.append(f"  {outcome}: {count}")

    status = "✅ SUCCESS" if report.success else "❌ FAILED"
    lines.append("")
    lines.append(f"Status: {status}")
    lines.append(f"{'='*50}\n")

    return lines