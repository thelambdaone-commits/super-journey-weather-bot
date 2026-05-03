"""
GEM (Great Market) detection - high-quality signals.
Finds rare opportunities with real edge, not just high displayed EV.
"""
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional


# === GEM STRICT THRESHOLDS ===
GEM_MIN_EV = 0.10  # 10% minimum net EV
GEM_MAX_SPREAD = 0.04  # 4% maximum spread
GEM_MIN_VOLUME = 3000  # $3000 minimum volume
GEM_KELLY_MULTIPLIER = 0.05  # 5% of full Kelly
GEM_MIN_CONFIDENCE = 0.50  # 50% minimum calibration confidence
GEM_MIN_DIVERGENCE = 0.15  # 15% minimum model/market divergence


# === EXCLUSIONS ===
EXCLUDED_PATTERNS = [
    "or below",  # Ambiguous resolution
    "or higher",  # Ambiguous resolution
    "average",  # May use mean vs max
    "mean",  # Different from max
    "exact",  # Too precise to be reliable
]


@dataclass
class GEMScore:
    """GEM quality score."""
    total: float
    net_ev: float
    divergence: float
    confidence: float
    liquidity: float
    spread_penalty: float
    is_valid: bool
    exclusion_reason: Optional[str] = None


class GEMDetector:
    """
    Detect rare, high-quality market opportunities.
    
    GEM criteria:
    - Net EV > 10%
    - Spread < 4%
    - Volume > $3000
    - Divergence > 15% (model vs market)
    - Clear resolution rules
    """
    
    def __init__(self):
        self.min_ev = GEM_MIN_EV
        self.max_spread = GEM_MAX_SPREAD
        self.min_volume = GEM_MIN_VOLUME
        self.kelly_multiplier = GEM_KELLY_MULTIPLIER
        self.min_confidence = GEM_MIN_CONFIDENCE
        self.min_divergence = GEM_MIN_DIVERGENCE
        self.min_total_score = 8.0
    
    def is_excluded(self, question: str, outcomes: list) -> tuple[bool, str]:
        """Check if market should be excluded (fake GEM patterns)."""
        if not question:
            return True, "empty_question"
        
        question_lower = question.lower()
        
        # Check exclusion patterns
        for pattern in EXCLUDED_PATTERNS:
            if pattern in question_lower:
                return True, f"excluded_pattern:{pattern}"
        
        # Check outcomes
        if not outcomes or len(outcomes) == 0:
            return True, "no_outcomes"
        
        # Check spread
        for outcome in outcomes:
            spread = outcome.get("spread", 0.0)
            if spread > self.max_spread:
                return True, f"spread_too_high:{spread:.1%}"
            
            volume = outcome.get("volume", 0)
            if volume > 0 and volume < self.min_volume:
                return True, f"volume_too_low:{volume}"
        
        return False, ""
    
    def calc_divergence(
        self, 
        model_probability: float, 
        market_price: float
    ) -> float:
        """Calculate model vs market divergence."""
        if market_price <= 0:
            return 0.0
        return abs(model_probability - market_price)
    
    def score(
        self,
        model_probability: float,
        market_price: float,
        net_ev: float,
        spread: float,
        volume: float,
        confidence: float,
        question: str,
    ) -> GEMScore:
        """Calculate GEM score."""
        
        # Check exclusions
        is_excluded, reason = self.is_excluded(question, [{"spread": spread, "volume": volume}])
        if is_excluded:
            return GEMScore(
                total=0.0,
                net_ev=net_ev,
                divergence=0.0,
                confidence=confidence,
                liquidity=volume,
                spread_penalty=spread,
                is_valid=False,
                exclusion_reason=self.get_readable_reason(reason),
            )
        
        # Calculate components
        divergence = self.calc_divergence(model_probability, market_price)
        
        # Base score: EV * 100 (higher = better)
        ev_score = max(0, net_ev * 100)
        
        # Divergence score (reward divergence)
        div_score = divergence * 50 if divergence >= self.min_divergence else 0
        
        # Confidence score (reward calibrated predictions)
        conf_score = confidence * 20
        
        # Liquidity score (reward volume)
        liq_score = min(volume / 10000, 1.0) * 10
        
        # Spread penalty (penalize wide spreads)
        spread_penalty = spread * 50
        
        # Total
        total = ev_score + div_score + conf_score + liq_score - spread_penalty
        
        return GEMScore(
            total=total,
            net_ev=net_ev,
            divergence=divergence,
            confidence=confidence,
            liquidity=volume,
            spread_penalty=spread,
            is_valid=(
                net_ev >= self.min_ev and
                spread <= self.max_spread and
                divergence >= self.min_divergence and
                confidence >= self.min_confidence and
                total >= self.min_total_score
            ),
        )
    
    def get_readable_reason(self, reason: str) -> str:
        """Map technical reason strings to human-readable French."""
        if not reason:
            return ""
            
        if reason.startswith("volume_too_low"):
            val = reason.split(":")[-1]
            return f"Volume insuffisant (${val})"
        elif reason.startswith("spread_too_high"):
            val = reason.split(":")[-1]
            return f"Spread trop élevé ({val})"
        elif reason.startswith("excluded_pattern"):
            val = reason.split(":")[-1]
            return f"Modèle exclu ({val})"
        
        mapping = {
            "empty_question": "Question vide",
            "no_outcomes": "Aucun résultat",
            "gem_valid": "Signal GEM validé",
            "net_ev_too_low": "Edge insuffisant",
            "divergence_too_low": "Divergence trop faible",
            "confidence_too_low": "Confiance trop basse",
        }
        
        # Handle cases with values (e.g. net_ev_too_low:0.05)
        base_reason = reason.split(":")[0]
        if base_reason in mapping:
            msg = mapping[base_reason]
            if ":" in reason:
                msg += f" ({reason.split(':')[-1]})"
            return msg
            
        return reason

    def should_trade(
        self,
        model_probability: float,
        market_price: float,
        net_ev: float,
        spread: float,
        volume: float,
        confidence: float,
        question: str,
    ) -> tuple[bool, str]:
        """Determine if this is a valid GEM trade."""
        
        # Check exclusions
        is_excluded, reason = self.is_excluded(question, [{"spread": spread, "volume": volume}])
        if is_excluded:
            return False, self.get_readable_reason(reason)
        
        # Calculate divergence
        divergence = self.calc_divergence(model_probability, market_price)
        
        # Strict thresholds
        if net_ev < self.min_ev:
            return False, self.get_readable_reason(f"net_ev_too_low:{net_ev:.1%}")
        
        if spread > self.max_spread:
            return False, self.get_readable_reason(f"spread_too_high:{spread:.1%}")
        
        if divergence < self.min_divergence:
            return False, self.get_readable_reason(f"divergence_too_low:{divergence:.1%}")
        
        if confidence < self.min_confidence:
            return False, self.get_readable_reason(f"confidence_too_low:{confidence:.0%}")
        
        return True, self.get_readable_reason("gem_valid")
    
    def get_thresholds(self) -> dict:
        """Get current GEM thresholds."""
        return {
            "min_ev": self.min_ev,
            "max_spread": self.max_spread,
            "min_volume": self.min_volume,
            "kelly_multiplier": self.kelly_multiplier,
            "min_confidence": self.min_confidence,
            "min_divergence": self.min_divergence,
        }
    
    def format_report(self) -> str:
        """Format GEM thresholds report."""
        t = self.get_thresholds()
        return (
            f"=== GEM THRESHOLDS ===\n"
            f"MIN_EV: {t['min_ev']:.0%}\n"
            f"MAX_SPREAD: {t['max_spread']:.0%}\n"
            f"MIN_VOLUME: ${t['min_volume']:,}\n"
            f"KELLY: {t['kelly_multiplier']:.0%}\n"
            f"MIN_CONFIDENCE: {t['min_confidence']:.0%}\n"
            f"MIN_DIVERGENCE: {t['min_divergence']:.0%}"
        )
# Audit: Includes fee and slippage awareness
