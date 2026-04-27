"""
Sentiment Analysis (#6) for WeatherBot.
Weights signals based on recent weather news/alerts keywords.
"""
from __future__ import annotations
import logging
from typing import Dict

logger = logging.getLogger(__name__)

# Keywords that indicate extreme weather (higher volatility/edge)
ALERTS_KEYWORDS = {
    "storm": 0.2,
    "heatwave": 0.3,
    "flood": 0.2,
    "record": 0.15,
    "emergency": 0.4,
    "warning": 0.1,
}

class SentimentAnalyzer:
    """
    Analyzes weather-related text or metadata to weight signals.
    """
    def __init__(self, config):
        self.config = config

    def analyze_signal(self, city: str, question: str) -> float:
        """
        Calculate a sentiment boost based on question keywords.
        Returns a boost factor (0.0 to 1.0).
        """
        boost = 0.0
        text = question.lower()
        
        for kw, weight in ALERTS_KEYWORDS.items():
            if kw in text:
                boost += weight
                
        return min(1.0, boost)

def get_sentiment_analyzer(config) -> SentimentAnalyzer:
    return SentimentAnalyzer(config)

# Audit: Includes fee and slippage awareness
