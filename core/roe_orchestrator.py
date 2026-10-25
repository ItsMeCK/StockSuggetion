import math
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass, field
from datetime import datetime

@dataclass
class TradeHypothesis:
    hypothesis_id: str
    symbol: str
    direction: str  # "LONG" | "SHORT"
    macro_regime: str  # "BULL_EXPANSION" | "MEAN_REVERTING_CHOP" | "BEAR_DISTRIBUTION"
    archetype: str  # "STAGE_2_EXPANSION" | "VOLATILITY_SQUEEZE" | "STEALTH_ACCUM" | "SWEEP_REVERSAL"
    anchor_price: float
    proposed_entry_price: float
    hard_invalidation_level: float
    holding_tier: str  # "TYPE_A_SCALP" | "TYPE_B_DAY_TREND" | "TYPE_C_BTST"
    proposed_instrument: str = "OPTION"  # "OPTION" | "EQUITY"
    falsification_conditions: List[str] = field(default_factory=list)

@dataclass
class DoubtToken:
    agent_id: str  # "SECTOR_MACRO", "MICRO_STRUCTURE", "NEWS_GROUNDING", "DERIVATIVES_GREEKS"
    severity: str  # "LOW", "MEDIUM", "HIGH", "FATAL"
    weight: float
    confidence: float  # 0.0 to 1.0
    proposal: str  # e.g. "HOLD_PLANNED", "MUTATE_TO_EQUITY", "DEMOTE_HOLDING_TIER", "ABSOLUTE_VETO"
    details: str = ""

@dataclass
class ArbiterDecision:
    action: str  # "CONFIRM_AS_PLANNED", "ADAPT_AND_REROUTE", "FATAL_VETO"
    d_score: float
    final_instrument: str
    final_holding_tier: str
    is_executable: bool
    reasoning: str
    doubt_tokens: List[DoubtToken] = field(default_factory=list)

class RoePlanner:
    """
    Lead ROE Planner (Subagent 1)
    Formulates structured trade hypotheses and audits baseline sector gates.
    """
    def formulate_hypothesis(
        self,
        candidate: Dict[str, Any],
        holding_tier: str = "TYPE_A_SCALP",
        direction: str = "LONG"
    ) -> TradeHypothesis:
        symbol = candidate.get("symbol", "UNKNOWN")
        close_px = float(candidate.get("close", 0.0))
        macro_regime = candidate.get("macro_regime", "BULL_EXPANSION")
        
        # Hard invalidation set at 1.5% below entry for Long, 1.5% above for Short
        invalidation = round(close_px * 0.985, 2) if direction == "LONG" else round(close_px * 1.015, 2)
        hyp_id = f"HYP_{symbol}_{int(datetime.now().timestamp())}"

        return TradeHypothesis(
            hypothesis_id=hyp_id,
            symbol=symbol,
            direction=direction,
            macro_regime=macro_regime,
            archetype="STAGE_2_EXPANSION",
            anchor_price=close_px,
            proposed_entry_price=close_px,
            hard_invalidation_level=invalidation,
            holding_tier=holding_tier,
            proposed_instrument="OPTION",
            falsification_conditions=["Close crosses invalidation level", "Volume decay > 50% in next 30m"]
        )

    def audit_sector_gate(self, candidate: Dict[str, Any]) -> DoubtToken:
        """
        Calibrated Sector Gate with Alpha Decoupling Override:
        - If sector drops < -0.80%:
          - If stock displays massive relative strength (stock_return >= +2.5% and volume_surge >= 2.5),
            apply Alpha Decoupling Override and approve!
          - Else emit HIGH severity doubt.
        """
        sec_ret = float(candidate.get("sector_return", 0.0))
        stock_ret = float(candidate.get("stock_return", 0.0))
        vol_surge = float(candidate.get("volume_surge", 1.0))

        if sec_ret < -0.80:
            if stock_ret >= 2.5 and vol_surge >= 2.5:
                return DoubtToken(
                    agent_id="SECTOR_MACRO",
                    severity="LOW",
                    weight=0.5,
                    confidence=0.95,
                    proposal="ALPHA_DECOUPLING_OVERRIDE",
                    details=f"Stock decouples from sector drag: Stock +{stock_ret:.2f}% on {vol_surge:.1f}x vol vs Sector {sec_ret:.2f}%"
                )
            else:
                return DoubtToken(
                    agent_id="SECTOR_MACRO",
                    severity="HIGH",
                    weight=1.0,
                    confidence=0.85,
                    proposal="MUTATE_TO_EQUITY",
                    details=f"Severe sector drag ({sec_ret:.2f}%) without alpha decoupling"
                )

        return DoubtToken(
            agent_id="SECTOR_MACRO",
            severity="LOW",
            weight=1.0,
            confidence=0.90,
            proposal="HOLD_PLANNED",
            details="Sector tailwinds healthy"
        )

class RoeConsolidatedArbiter:
    """
    Bi-Directional Self-Doubt Arbiter (Subagent 3)
    Computes D_score across subagent doubt tokens and executes the 4-way decision matrix.
    """
    SEVERITY_VALUES = {
        "LOW": 0.25,
        "MEDIUM": 0.50,
        "HIGH": 0.85,
        "FATAL": 1.00
    }

    def arbitrate(
        self,
        hyp: TradeHypothesis,
        doubt_tokens: List[DoubtToken]
    ) -> ArbiterDecision:
        # 1. Check for immediate FATAL veto
        fatal_token = next((t for t in doubt_tokens if t.severity == "FATAL"), None)
        if fatal_token:
            return ArbiterDecision(
                action="FATAL_VETO",
                d_score=100.0,
                final_instrument="NONE",
                final_holding_tier="NONE",
                is_executable=False,
                reasoning=f"FATAL doubt triggered by {fatal_token.agent_id}: {fatal_token.details or fatal_token.proposal}",
                doubt_tokens=doubt_tokens
            )

        # 2. Compute Doubt Score D_score
        total_weighted_doubt = 0.0
        total_weight = 0.0

        for t in doubt_tokens:
            s_val = self.SEVERITY_VALUES.get(t.severity, 0.25)
            # Inversely scaled by agent confidence: doubt = weight * severity * (1 - confidence_margin)
            token_doubt = t.weight * s_val * (1.0 - t.confidence * 0.5)
            total_weighted_doubt += token_doubt
            total_weight += t.weight

        d_score = (total_weighted_doubt / total_weight * 100.0) if total_weight > 0 else 0.0

        # 3. Decision Matrix
        if d_score < 25.0:
            return ArbiterDecision(
                action="CONFIRM_AS_PLANNED",
                d_score=round(d_score, 2),
                final_instrument=hyp.proposed_instrument,
                final_holding_tier=hyp.holding_tier,
                is_executable=True,
                reasoning="Unanimous subagent validation with low doubt score",
                doubt_tokens=doubt_tokens
            )
        elif 25.0 <= d_score <= 55.0:
            # Re-planning Adaptation: Mutate to Cash Equity and Demote to Scalp
            adapted_instrument = "EQUITY"
            adapted_tier = "TYPE_A_SCALP"
            return ArbiterDecision(
                action="ADAPT_AND_REROUTE",
                d_score=round(d_score, 2),
                final_instrument=adapted_instrument,
                final_holding_tier=adapted_tier,
                is_executable=True,
                reasoning=f"Moderate doubt ({d_score:.2f}) resolved by adapting instrument to {adapted_instrument} and tier to {adapted_tier}",
                doubt_tokens=doubt_tokens
            )
        else:
            return ArbiterDecision(
                action="FATAL_VETO",
                d_score=round(d_score, 2),
                final_instrument="NONE",
                final_holding_tier="NONE",
                is_executable=False,
                reasoning=f"High cumulative doubt score ({d_score:.2f} > 55.0). Candidate discarded.",
                doubt_tokens=doubt_tokens
            )

def evaluate_candidate_roe(
    candidate: Dict[str, Any],
    holding_tier: str = "TYPE_A_SCALP",
    direction: str = "LONG",
    subagent_tokens: Optional[List[DoubtToken]] = None
) -> ArbiterDecision:
    """Convenience helper to evaluate candidate through Planner and Consolidated Arbiter."""
    planner = RoePlanner()
    arbiter = RoeConsolidatedArbiter()

    hyp = planner.formulate_hypothesis(candidate, holding_tier=holding_tier, direction=direction)
    tokens = subagent_tokens or []
    
    # Audit sector gate
    tokens.append(planner.audit_sector_gate(candidate))
    return arbiter.arbitrate(hyp, tokens)
