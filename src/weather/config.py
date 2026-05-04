"""
Config module - centralized configuration management.
"""

import json
import os
from pathlib import Path
from dataclasses import dataclass, field
from typing import Optional

# Try to load dotenv
try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass  # dotenv not available
except ImportError:
    pass


@dataclass
class Config:
    """Global configuration for WeatherBot."""
    # Trading core
    balance: float = 10000.0
    max_bet: float = 20.0
    min_ev: float = 0.015  # Lowered for opportunistic trading (was 0.10)
    max_price: float = 0.70  # Increased to allow more opportunities (was 0.45)
    min_volume: int = 50  # Lowered for opportunistic trading (was 500)
    min_hours: int = 2
    max_hours: int = 72
    kelly_fraction: float = 0.25
    max_slippage: float = 0.10  # Increased tolerance (was 0.03)
    scan_interval: int = 10800  # 3 hours aligned with weather model runs (was 3600)

    # New required config fields (original plan + additions)
    min_edge: float = 0.015  # Opportune edge threshold (was 0.06)
    max_spread: float = 0.10  # Increased tolerance (was 0.05)
    max_position_pct: float = 0.02  # 2% bankroll per trade
    max_market_exposure_pct: float = 0.05  # 5% max exposure per market
    max_daily_drawdown: float = 0.05  # 5% daily drawdown limit
    estimated_fee_bps: float = 10.0  # 10 bps = 0.1% fee
    estimated_slippage_bps: float = 5.0  # 5 bps = 0.05% slippage
    require_positive_net_ev: bool = True
    min_orderbook_depth_usd: float = 5.0  # Min orderbook depth (was 100.0)
    max_orders_per_minute: int = 10  # Live safety (point 7)
    kill_switch_enabled: bool = False  # Global kill switch (point 7)
    calibration_min: int = 30
    # Exposure limits (aligned with .env.example)
    max_exposure_per_city: float = 100.0
    max_exposure_per_region: float = 250.0
    max_exposure_per_cluster: float = 300.0
    max_total_exposure: float = 1000.0
    paper_mode: bool = False
    live_trade: bool = False
    live_trade_confirm: str = ""  # Double lock: must be "I_ACCEPT_REAL_LOSS" (aligned with .env.example)
    signal_mode: bool = True
    tui_mode: bool = False
    dashboard_enabled: bool = True
    signal_min_ev: float = 0.05
    signal_min_prob: float = 0.75
    signal_max_uncertainty: float = 0.18
    signal_rate_limit_hour: int = 3
    signal_city_cooldown_hours: int = 8
    signal_duplicate_window_hours: int = 24
    signal_min_price_move: float = 0.02
    signal_top_k: int = 3
    report_signal_max_age_hours: int = 48
    report_min_signal_price: float = 0.01

    # Dual Flow Configuration
    ai_flow_enabled: bool = False
    ai_min_confidence: float = 0.50
    ai_max_ev_threshold: float = 2.0
    ai_allow_low_confidence_high_ev: bool = False
    ai_force_blocking: bool = False  # False = parallel mode (max profit)
    ai_max_reviews_per_scan: int = 5

    signal_flow_enabled: bool = True
    signal_min_quality_score: float = 0.60
    signal_min_confidence: float = 0.50
    signal_min_edge: float = 0.05
    signal_bayesian_penalty_max: float = 0.30

    # Paper training mode: looser thresholds, smaller stakes, live unaffected.
    paper_training_mode: bool = True
    paper_training_min_ev: float = 0.02
    paper_training_max_price: float = 0.75
    paper_training_max_spread: float = 0.10
    paper_training_min_volume: int = 100
    paper_training_min_confidence: float = 0.25
    paper_training_min_quality_score: float = 0.25
    paper_training_max_bet_usd: float = 5.0
    paper_training_min_bet_usd: float = 1.0

    # Surebet detection is passive by default: detect/log only, no multi-leg execution.
    surebet_detection_enabled: bool = True
    max_clob_requests_per_scan: int = 30
    surebet_prefilter_margin: float = 0.0
    surebet_min_profit_pct: float = 0.01
    surebet_fee_buffer_pct: float = 0.003
    surebet_max_total_stake_usd: float = 50.0
    surebet_min_liquidity_usd: float = 5.0
    surebet_paper_execution_enabled: bool = True
    surebet_live_execution_enabled: bool = False

    # Micro-Live Caps (hard limits - override Kelly)
    max_live_bet_usd: float = 10.0  # $10 per trade max for micro-live
    max_live_total_exposure_usd: float = 50.0  # $50 total max for micro-live
    gem_threshold: float = 0.85  # Score threshold for GEM alerts

    # Ouroboros (auto-improvement)
    ouroboros_enabled: bool = True  # Set to False to disable in trading loop
    ouroboros_timeout: int = 300  # Max seconds for Ouroboros subprocess

    # API Keys
    meteoblue_key: str = ""
    weatherbit_key: str = ""

    # Telegram
    telegram_bot_token: str = ""
    telegram_chat_id: str = ""
    telegram_signal_chat_id: str = ""

    # AI
    groq_api_key: str = ""

    # Paths
    data_dir: str = "data"
    logs_dir: str = "logs"

    @classmethod
    def load(cls, config_path: str = "config.json") -> "Config":
        """Load configuration from JSON file and environment variables."""
        path = Path(config_path)
        data = {}

        # 1. Start with JSON if it exists
        if path.exists():
            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
            except (Exception,) as e:
                pass

        # 2. Override with environment variables (uppercase)
        for field_name, field_def in cls.__dataclass_fields__.items():
            env_key = field_name.upper()
            env_val = os.environ.get(env_key)

            if env_val is not None:
                # Type conversion based on dataclass field type
                target_type = field_def.type
                try:
                    if target_type == bool:
                        data[field_name] = env_val.lower() in ("true", "1", "yes", "on")
                    elif target_type == int:
                        data[field_name] = int(env_val)
                    elif target_type == float:
                        data[field_name] = float(env_val)
                    else:
                        data[field_name] = env_val
                except (ValueError, TypeError):
                    pass

        # Filter only valid fields for constructor
        valid_data = {k: v for k, v in data.items() if k in cls.__dataclass_fields__}
        return cls(**valid_data)

    def save(self, config_path: str = "config.json"):
        """Save configuration to JSON file."""
        data = {f.name: getattr(self, f.name, None) for f in self.__dataclass_fields__.values()}
        with open(config_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)


def get_telegram_safe() -> tuple[str, str]:
    """Get Telegram credentials safely."""
    config = Config.load()
    token = os.environ.get("TELEGRAM_TOKEN", config.telegram_bot_token)
    chat_id = os.environ.get("TELEGRAM_CHAT_ID", config.telegram_chat_id)
    return token, chat_id


# Global config instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get global config instance (singleton)."""
    global _config
    if _config is None:
        _config = Config.load()
    return _config


def reload_config():
    """Reload configuration from disk."""
    global _config
    _config = Config.load()
