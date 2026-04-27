#!/usr/bin/env python3
"""
Groq AI module for WeatherBot.
Provides AI-powered analysis and insights.
"""
import os
import json
from typing import Optional, Dict, Any

# Load .env first
from dotenv import load_dotenv
load_dotenv()

try:
    from groq import Groq
    GROQ_AVAILABLE = True
except ImportError:
    GROQ_AVAILABLE = False

from ..weather.config import get_config


def get_groq_client() -> Optional[Groq]:
    """Initialize Groq client from environment."""
    if not GROQ_AVAILABLE:
        return None
    
    config = get_config()
    api_key = (
        os.environ.get("GROQ_API_KEY")
        or os.environ.get("GROQ_KEY")
        or getattr(config, "groq_api_key", "")
    )
    if not api_key:
        return None
    
    return Groq(api_key=api_key)


def analyze_forecast(
    city: str,
    ecmwf: Optional[float],
    hrrr: Optional[float],
    metar: Optional[float],
    actual: Optional[float],
    unit: str = "C",
) -> Dict[str, Any]:
    """
    Use Groq to analyze forecast data and generate insights.
    Returns a dict with analysis, recommendation, and confidence.
    """
    client = get_groq_client()
    if not client:
        return {"error": "Groq not configured"}
    
    unit_sym = "°F" if unit == "F" else "°C"
    
    # Build prompt
    prompt = f"""Analyze this weather forecast for {city}:

- ECMWF forecast: {ecmwf}{unit_sym}
- HRRR forecast: {hrrr}{unit_sym}  
- Current METAR: {metar}{unit_sym}
- Actual (resolved): {actual}{unit_sym}

Respond with JSON:
{{
  "analysis": "brief analysis (1-2 sentences)",
  "confidence": "high/medium/low",
  "recommendation": "buy/hold/sell/no_opportunity"
}}"""
    
    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
            max_tokens=200,
        )
        
        content = response.choices[0].message.content
        
        # Try to parse JSON
        try:
            # Find JSON in response
            start = content.find("{")
            end = content.rfind("}") + 1
            if start >= 0 and end > start:
                json_str = content[start:end]
                return json.loads(json_str)
        except json.JSONDecodeError:
            pass
        
        # Fallback: parse text
        return {
            "analysis": content[:100],
            "confidence": "unknown",
            "recommendation": "manual_review"
        }
        
    except (Exception,) as e:
        return {"error": str(e)}


def generate_report(
    trades_total: int,
    trades_won: int,
    trades_lost: int,
    pnl: float,
    balance: float,
    top_cities: list,
) -> str:
    """Generate daily report using AI."""
    client = get_groq_client()
    if not client:
        return None
    
    prompt = f"""Generate a brief trading report:

Trades: {trades_total} ({trades_won} won, {trades_lost} lost)
PnL: ${pnl:+.2f}
Balance: ${balance:,.2f}
Top cities: {', '.join(top_cities)}

Write a 2-sentence summary in trading style."""
    
    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.5,
            max_tokens=100,
        )
        
        return response.choices[0].message.content
        
    except (Exception,) as e:
        return None


def check_anomaly(
    city: str,
    forecast_temp: float,
    market_price: float,
    ev: float,
    confidence: float,
    unit: str = "C",
) -> Dict[str, Any]:
    """Check if there's an anomaly in the trade opportunity."""
    client = get_groq_client()
    if not client:
        return {"is_anomaly": False}
    
    unit_sym = "°F" if unit == "F" else "°C"
    
    prompt = f"""Check if this trade is potentially fraudulent or based on broken data:

City: {city}
Forecast: {forecast_temp}{unit_sym}
Market Price: ${market_price}
Expected Value (ROI): {ev:.2f} (Profit ratio per dollar bet)
Model Confidence: {confidence:.2f}

Is this trade suspiciously good (e.g. market knows something model doesn't) or based on data errors? 
Respond JSON:
{{
  "is_anomaly": true/false,
  "reason": "brief reason if anomaly"
}}
"""
    
    try:
        response = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[{"role": "user", "content": prompt}],
            temperature=0.2,
            max_tokens=100,
        )
        
        content = response.choices[0].message.content
        
        start = content.find("{")
        end = content.rfind("}") + 1
        if start >= 0 and end > start:
            return json.loads(content[start:end])
        
    except (Exception,) as e:
        pass
    
    return {"is_anomaly": False}


# Quick test
if __name__ == "__main__":
    print("Groq AI module")
    print(f"Available: {GROQ_AVAILABLE}")
    print(f"Client: {get_groq_client() is not None}")
