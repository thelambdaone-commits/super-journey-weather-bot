"""
Polymarket Historical Data Harvester.
Collects market history and builds temporal trajectories for ML.
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional


@dataclass
class HarvestConfig:
    """Configuration for data harvesting."""

    data_dir: str = "data"
    markets_subdir: str = "markets_historical"
    price_history_subdir: str = "price_history"
    cache_ttl_seconds: int = 3600

    max_markets_per_call: int = 100
    rate_limit_seconds: float = 0.5

    include_closed: bool = True
    include_resolved: bool = True

    target_market_tags: list[str] = None

    def __post_init__(self):
        if self.target_market_tags is None:
            self.target_market_tags = ["climate", "temperature", "weather", "hot", "cold", "rain"]


@dataclass
class HarvestReport:
    """Report from harvesting operation."""

    markets_collected: int
    markets_with_price_history: int
    markets_resolved: int
    data_quality_score: float
    output_dir: str
    duration_seconds: float
    errors: list[str]
    success: bool


class PolymarketHarvester:
    """Harvest historical data from Polymarket."""

    def __init__(
        self,
        data_dir: str = "data",
        config: Optional[HarvestConfig] = None,
    ):
        self.data_dir = Path(data_dir)
        self.config = config or HarvestConfig()
        self._setup_directories()
        self._api_client = None

    def _setup_directories(self) -> None:
        """Create necessary directories."""
        markets_dir = self.data_dir / self.config.markets_subdir
        price_dir = self.data_dir / self.config.price_history_subdir
        markets_dir.mkdir(exist_ok=True, parents=True)
        price_dir.mkdir(exist_ok=True, parents=True)

    @property
    def api_client(self):
        """Lazy load API client."""
        if self._api_client is None:
            self._api_client = self._create_api_client()
        return self._api_client

    def _create_api_client(self):
        """Create Polymarket API client."""
        try:
            import requests
            return _PolymarketAPI()
        except ImportError:
            return _MockPolymarketAPI()

    def harvest_markets(
        self,
        tags: Optional[list[str]] = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Harvest markets by tags."""
        tags = tags or self.config.target_market_tags
        markets = self.api_client.fetch_markets(
            tags=tags,
            limit=limit,
            closed=self.config.include_closed,
        )
        return markets

    def harvest_price_history(
        self,
        market_id: str,
    ) -> list[dict[str, Any]]:
        """Harvest price history for a market."""
        history = self.api_client.fetch_market_history(market_id)
        return history

    def collect_full_market(
        self,
        market_id: str,
        market_data: Optional[dict] = None,
    ) -> dict[str, Any]:
        """Collect complete market data with price history."""
        market = market_data or self.api_client.fetch_market(market_id)
        if not market:
            return {}

        price_history = self.harvest_price_history(market_id)

        market_data = {
            "market_id": market_id,
            "question": market.get("question", ""),
            "condition_id": market.get("condition_id"),
            "market_slug": market.get("slug"),
            "created_at": market.get("created_at"),
            "updated_at": market.get("updated_at"),
            "closed_at": market.get("closed_at"),
            "resolved_at": market.get("resolved_at"),
            "is_resolved": market.get("is_resolved", False),
            "outcome": market.get("outcome"),
            "outcomes": market.get("outcomes"),
            "volume": market.get("volume"),
            "liquidity": market.get("liquidity"),
            "market_maker": market.get("market_maker"),
            "question_slug": market.get("question_slug"),
            "tags": market.get("tags", []),
            "city": market.get("city"),
            "target_temp": market.get("target_temp"),
            "actual_temp": market.get("actual_temp"),
            "price_history": price_history,
            "collected_at": datetime.now().isoformat(),
        }

        return market_data

    def save_market(
        self,
        market_data: dict[str, Any],
        market_id: str,
    ) -> Path:
        """Save market data to file."""
        markets_dir = self.data_dir / self.config.markets_subdir
        path = markets_dir / f"{market_id}.json"
        path.write_text(json.dumps(market_data, ensure_ascii=False, indent=2))
        return path

    def save_price_history(
        self,
        price_history: list[dict],
        market_id: str,
    ) -> Path:
        """Save price history to file."""
        price_dir = self.data_dir / self.config.price_history_subdir
        path = price_dir / f"{market_id}_prices.jsonl"
        with path.open("w", encoding="utf-8") as f:
            for tick in price_history:
                f.write(json.dumps(tick, ensure_ascii=False) + "\n")
        return path

    def run(
        self,
        tags: Optional[list[str]] = None,
        limit: int = 100,
    ) -> HarvestReport:
        """Run full harvest operation."""
        start_time = datetime.now()
        errors = []

        try:
            markets = self.harvest_markets(tags=tags, limit=limit)

            if not markets:
                return HarvestReport(
                    markets_collected=0,
                    markets_with_price_history=0,
                    markets_resolved=0,
                    data_quality_score=0,
                    output_dir=str(self.data_dir / self.config.markets_subdir),
                    duration_seconds=0,
                    errors=["No markets found"],
                    success=False,
                )

            collected = 0
            with_history = 0
            resolved = 0

            for market in markets:
                market_id = market.get("id") or market.get("condition_id")
                if not market_id:
                    continue

                try:
                    full_data = self.collect_full_market(market_id, market)
                    if full_data:
                        self.save_market(full_data, market_id)
                        collected += 1

                        if full_data.get("price_history"):
                            self.save_price_history(full_data["price_history"], market_id)
                            with_history += 1

                        if full_data.get("is_resolved"):
                            resolved += 1

                    time.sleep(self.config.rate_limit_seconds)

                except (Exception,) as e:
                    errors.append(f"{market_id}: {str(e)}")

            duration = (datetime.now() - start_time).total_seconds()
            quality = self._compute_quality_score(collected, with_history, resolved)

            return HarvestReport(
                markets_collected=collected,
                markets_with_price_history=with_history,
                markets_resolved=resolved,
                data_quality_score=quality,
                output_dir=str(self.data_dir / self.config.markets_subdir),
                duration_seconds=duration,
                errors=errors[:10],
                success=True,
            )

        except (Exception,) as e:
            return HarvestReport(
                markets_collected=0,
                markets_with_price_history=0,
                markets_resolved=0,
                data_quality_score=0,
                output_dir=str(self.data_dir / self.config.markets_subdir),
                duration_seconds=0,
                errors=[str(e)],
                success=False,
            )

    def _compute_quality_score(
        self,
        collected: int,
        with_history: int,
        resolved: int,
    ) -> float:
        """Compute data quality score."""
        if collected == 0:
            return 0.0

        history_ratio = with_history / collected if collected > 0 else 0
        resolved_ratio = resolved / collected if collected > 0 else 0

        return (history_ratio * 0.4 + resolved_ratio * 0.6) * 100

    def load_historical_markets(self) -> list[dict[str, Any]]:
        """Load all collected historical markets."""
        markets_dir = self.data_dir / self.config.markets_subdir
        markets = []

        for path in markets_dir.glob("*.json"):
            try:
                market = json.loads(path.read_text(encoding="utf-8"))
                markets.append(market)
            except (Exception,) as e:
                continue

        return markets


class _PolymarketAPI:
    """Real Polymarket API client."""

    BASE_URL = "https://clob.polymarket.com"
    MARKETS_URL = "https://gamma.polymarket.com/markets"

    def fetch_markets(
        self,
        tags: list[str],
        limit: int = 100,
        closed: bool = True,
    ) -> list[dict[str, Any]]:
        """Fetch markets by tags."""
        try:
            import requests
        except ImportError:
            return []

        try:
            response = requests.get(
                f"{self.MARKETS_URL}",
                params={
                    "tags": ",".join(tags),
                    "limit": limit,
                    "closed": closed,
                },
                timeout=10,
            )
            if response.status_code == 200:
                return response.json().get("markets", [])
        except (Exception,) as e:
            pass

        return []

    def fetch_market(self, market_id: str) -> dict[str, Any]:
        """Fetch single market details."""
        try:
            import requests
        except ImportError:
            return {}

        try:
            response = requests.get(
                f"{self.MARKETS_URL}/{market_id}",
                timeout=10,
            )
            if response.status_code == 200:
                return response.json()
        except (Exception,) as e:
            pass

        return {}

    def fetch_market_history(self, market_id: str) -> list[dict[str, Any]]:
        """Fetch price history for market."""
        try:
            import requests
        except ImportError:
            return []

        try:
            response = requests.get(
                f"{self.BASE_URL}/markets/{market_id}/history",
                timeout=10,
            )
            if response.status_code == 200:
                return response.json().get("history", [])
        except (Exception,) as e:
            pass

        return []


class _MockPolymarketAPI:
    """Mock API for testing without network."""

    CITIES = ["nyc", "chicago", "london", "paris", "tokyo", "miami", "seattle", "los-angeles"]
    TEMPS = [75, 80, 85, 90, 65, 70, 55, 60]

    def fetch_markets(
        self,
        tags: list[str],
        limit: int = 100,
        closed: bool = True,
    ) -> list[dict[str, Any]]:
        """Return mock markets."""
        markets = []
        for i in range(min(limit, 50)):
            city = self.CITIES[i % len(self.CITIES)]
            temp = self.TEMPS[i % len(self.TEMPS)]
            markets.append({
                "id": f"mock_{i}",
                "condition_id": f"cond_{i}",
                "question": f"Will the highest temperature in {city.title()} be above {temp}°F on April {i+1}, 2026?",
                "is_resolved": i % 3 == 0,
                "outcome": "YES" if i % 3 == 0 else "NO",
                "outcomes": ["YES", "NO"],
                "volume": 1000 + i * 100,
                "liquidity": 500 + i * 50,
                "created_at": (datetime.now() - timedelta(days=i)).isoformat(),
                "closed_at": (datetime.now() - timedelta(days=i-1)).isoformat() if not (i % 3 == 0) else None,
                "resolved_at": (datetime.now() - timedelta(days=i-2)).isoformat() if i % 3 == 0 else None,
                "tags": ["weather", "temperature"],
                "city": city,
                "target_temp": temp,
                "actual_temp": temp + (i % 5) - 2 if i % 3 == 0 else None,
            })
        return markets

    def fetch_market(self, market_id: str) -> dict[str, Any]:
        """Return mock market."""
        idx = int(market_id.split("_")[1]) if "_" in market_id else 0
        city = self.CITIES[idx % len(self.CITIES)]
        temp = self.TEMPS[idx % len(self.TEMPS)]
        day = (idx % 28) + 1
        return {
            "id": market_id,
            "condition_id": market_id,
            "question": f"Will the highest temperature in {city.title()} be above {temp}°F on April {day}, 2026?",
            "is_resolved": idx % 3 == 0,
            "outcome": "YES" if idx % 3 == 0 else "NO",
            "outcomes": ["YES", "NO"],
            "city": city,
            "target_temp": temp,
            "actual_temp": temp + (idx % 5) - 2 if idx % 3 == 0 else None,
        }

    def fetch_market_history(self, market_id: str) -> list[dict[str, Any]]:
        """Return mock price history."""
        idx = int(market_id.split("_")[1]) if "_" in market_id else 0
        base_price = 0.3 + (idx % 10) * 0.05
        base_time = datetime.now() - timedelta(days=7)
        history = []
        for h in range(24 * 7):
            price = base_price + (h / (24 * 7)) * 0.3
            history.append({
                "timestamp": (base_time + timedelta(hours=h)).timestamp(),
                "price": round(price, 4),
                "volume": 100 + h * 5,
            })
        return history

    def fetch_market(self, market_id: str) -> dict[str, Any]:
        """Return mock market."""
        return {
            "id": market_id,
            "condition_id": market_id,
            "question": "Will temperature exceed 80F?",
            "is_resolved": True,
            "outcome": "YES",
            "outcomes": ["YES", "NO"],
        }

    def fetch_market_history(self, market_id: str) -> list[dict[str, Any]]:
        """Return mock price history."""
        base_time = datetime.now() - timedelta(days=7)
        return [
            {
                "timestamp": (base_time + timedelta(hours=h)).timestamp(),
                "price": 0.3 + (h / 24) * 0.2,
                "volume": 100 + h * 10,
            }
            for h in range(24 * 7)
        ]


def run_harvester(
    data_dir: str = "data",
    tags: Optional[list[str]] = None,
    limit: int = 100,
    use_mock: bool = True,
) -> HarvestReport:
    """Convenience function to run harvester."""
    config = HarvestConfig(data_dir=data_dir)
    harvester = PolymarketHarvester(data_dir=data_dir, config=config)

    if use_mock:
        harvester._api_client = _MockPolymarketAPI()

    return harvester.run(tags=tags, limit=limit)


def format_harvest_report(report: HarvestReport) -> list[str]:
    """Format harvest report for CLI."""
    lines = [
        f"\n{'='*50}",
        "POLYMARKET HARVESTER REPORT",
        f"{'='*50}",
        f"Markets collected: {report.markets_collected}",
        f"  with price history: {report.markets_with_price_history}",
        f"  resolved: {report.markets_resolved}",
        f"Data quality score: {report.data_quality_score:.1f}%",
        f"Duration: {report.duration_seconds:.2f}s",
        f"Output: {report.output_dir}",
    ]

    if report.errors:
        lines.append("")
        lines.append("Errors:")
        for err in report.errors[:5]:
            lines.append(f"  ❌ {err}")

    status = "✅ SUCCESS" if report.success else "❌ FAILED"
    lines.append("")
    lines.append(f"Status: {status}")
    lines.append(f"{'='*50}\n")

    return lines