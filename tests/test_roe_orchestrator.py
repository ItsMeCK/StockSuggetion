import pytest
from datetime import datetime
from core.roe_orchestrator import (
    TradeHypothesis,
    DoubtToken,
    RoePlanner,
    RoeConsolidatedArbiter,
    evaluate_candidate_roe
)

def test_trade_hypothesis_initialization():
    """TradeHypothesis initializes correctly with typed holding taxonomy."""
    hyp = TradeHypothesis(
        hypothesis_id="HYP_RELIANCE_001",
        symbol="RELIANCE",
        direction="LONG",
        macro_regime="BULL_EXPANSION",
        archetype="STAGE_2_EXPANSION",
        anchor_price=2800.0,
        proposed_entry_price=2805.0,
        hard_invalidation_level=2770.0,
        holding_tier="TYPE_A_SCALP",
        proposed_instrument="OPTION"
    )
    assert hyp.symbol == "RELIANCE"
    assert hyp.holding_tier == "TYPE_A_SCALP"
    assert hyp.proposed_instrument == "OPTION"
    assert hyp.direction == "LONG"

def test_arbiter_unanimous_approval():
    """When subagents have zero or low doubt, arbiter confirms as planned."""
    arbiter = RoeConsolidatedArbiter()
    hyp = TradeHypothesis(
        hypothesis_id="HYP_TCS_001",
        symbol="TCS",
        direction="LONG",
        macro_regime="BULL_EXPANSION",
        archetype="STAGE_2_EXPANSION",
        anchor_price=3500.0,
        proposed_entry_price=3505.0,
        hard_invalidation_level=3470.0,
        holding_tier="TYPE_C_BTST",
        proposed_instrument="OPTION"
    )
    # Zero high-severity doubt tokens
    doubt_tokens = [
        DoubtToken(agent_id="SECTOR_MACRO", severity="LOW", weight=1.0, confidence=0.9, proposal="HOLD_PLANNED"),
        DoubtToken(agent_id="MICRO_STRUCTURE", severity="LOW", weight=1.2, confidence=0.85, proposal="HOLD_PLANNED")
    ]
    decision = arbiter.arbitrate(hyp, doubt_tokens)
    assert decision.action == "CONFIRM_AS_PLANNED"
    assert decision.d_score < 25.0
    assert decision.final_instrument == "OPTION"
    assert decision.final_holding_tier == "TYPE_C_BTST"

def test_arbiter_fatal_toxic_veto():
    """Toxic catalyst from GroundingScout triggers instant FATAL_VETO regardless of other agents."""
    arbiter = RoeConsolidatedArbiter()
    hyp = TradeHypothesis(
        hypothesis_id="HYP_ADANI_001",
        symbol="ADANIENT",
        direction="LONG",
        macro_regime="BULL_EXPANSION",
        archetype="STAGE_2_EXPANSION",
        anchor_price=2500.0,
        proposed_entry_price=2510.0,
        hard_invalidation_level=2450.0,
        holding_tier="TYPE_B_DAY_TREND",
        proposed_instrument="OPTION"
    )
    doubt_tokens = [
        DoubtToken(agent_id="SECTOR_MACRO", severity="LOW", weight=1.0, confidence=0.9, proposal="HOLD_PLANNED"),
        DoubtToken(
            agent_id="NEWS_GROUNDING",
            severity="FATAL",
            weight=2.0,
            confidence=0.95,
            proposal="ABSOLUTE_VETO",
            details="Accounting investigation initiated by regulator"
        )
    ]
    decision = arbiter.arbitrate(hyp, doubt_tokens)
    assert decision.action == "FATAL_VETO"
    assert "FATAL" in decision.reasoning
    assert decision.is_executable is False

def test_arbiter_replanning_and_adaptation():
    """Moderate doubt (25 <= D_score <= 55) triggers adaptation to Equity and Scalp tier."""
    arbiter = RoeConsolidatedArbiter()
    hyp = TradeHypothesis(
        hypothesis_id="HYP_INFY_001",
        symbol="INFY",
        direction="LONG",
        macro_regime="MEAN_REVERTING_CHOP",
        archetype="STAGE_2_EXPANSION",
        anchor_price=1800.0,
        proposed_entry_price=1802.0,
        hard_invalidation_level=1785.0,
        holding_tier="TYPE_C_BTST",
        proposed_instrument="OPTION"
    )
    # Moderate doubt on overnight hold & option liquidity
    doubt_tokens = [
        DoubtToken(agent_id="SECTOR_MACRO", severity="MEDIUM", weight=1.0, confidence=0.7, proposal="MUTATE_TO_EQUITY"),
        DoubtToken(agent_id="DERIVATIVES_GREEKS", severity="MEDIUM", weight=1.5, confidence=0.65, proposal="DEMOTE_HOLDING_TIER")
    ]
    decision = arbiter.arbitrate(hyp, doubt_tokens)
    assert decision.action == "ADAPT_AND_REROUTE"
    assert 25.0 <= decision.d_score <= 55.0
    # Adapted instrument to equity and demoted to scalp
    assert decision.final_instrument == "EQUITY"
    assert decision.final_holding_tier == "TYPE_A_SCALP"
    assert decision.is_executable is True

def test_alpha_decoupling_override():
    """Sector drag is overridden if stock displays massive relative strength (RS >= +2.5%)."""
    planner = RoePlanner()
    # Sector is down -1.2%, but stock is up +3.1% on 3.0x volume
    candidate = {
        "symbol": "BHEL",
        "close": 275.0,
        "sector_return": -1.2,
        "stock_return": 3.1,
        "volume_surge": 3.0,
        "macro_regime": "BULL_EXPANSION"
    }
    hyp = planner.formulate_hypothesis(candidate, holding_tier="TYPE_A_SCALP")
    assert hyp.symbol == "BHEL"
    assert hyp.direction == "LONG"
    
    # Audit sector gate
    token = planner.audit_sector_gate(candidate)
    assert token.proposal == "ALPHA_DECOUPLING_OVERRIDE"
    assert token.severity == "LOW"
