"""
Notifications package for Telegram alerts and messaging.
"""
import requests

from .telegram_notifier import (
    TelegramNotifier,
    get_notifier,
    escape_markdown,
)

from .formatter import format_weather_signal
from .desk_metrics import log_event
from .anti_duplicate import is_duplicate_response

__all__ = [
    'TelegramNotifier',
    'get_notifier',
    'escape_markdown',
    'format_weather_signal',
    'log_event',
    'is_duplicate_response',
]
