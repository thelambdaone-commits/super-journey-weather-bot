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
    pass


@dataclass
class Config:
    """Central configuration for WeatherBot."""
    
    # Trading
    balance: float = 10000.0
    max_bet: float = 20.0
    min_ev: float = 0.10
    max_price: float = 0.45
    min_volume: int = 500
    min_hours: float = 2.0
    max_hours: float = 72.0
    kelly_fraction: float = 0.25
    max_slippage: float = 0.03
    scan_interval: int = 3600
    calibration_min: int = 30
    paper_mode: bool = False
    live_trade: bool = False
    signal_mode: bool = True
    tui_mode: bool = False
    dashboard_enabled: bool = True
    signal_min_ev: float = 0.05
    signal_min_prob: float = 0.75
    signal_min_confidence: float = 0.75
    signal_max_uncertainty: float = 0.18
    signal_rate_limit_hour: int = 3
    signal_city_cooldown_hours: int = 8
    signal_duplicate_window_hours: int = 24
    signal_min_price_move: float = 0.02
    signal_top_k: int = 3
    
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
            except Exception:
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
        data = {
            f.name: getattr(self, f.name, None)
            for f in self.__dataclass_fields__.values()
        }
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
