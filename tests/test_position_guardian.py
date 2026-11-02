import pytest
from datetime import datetime, time
from core.position_guardian import (
    PositionGuardian,
    CandleMetrics,
    GuardianAssessment,
    TrapType,
    HoldAction
)

def test_guardian_detects_bullish_exhaustion_trap():
    guardian = PositionGuardian()
    
    # Large upper wick: high=105, open=100, close=101, low=99.5.
    # Total range = 5.5. Upper wick = 105 - 101 = 4.0. Wick % = 4.0 / 5.5 = 72.7%
    # Volume surge = 2.5x
    candle = CandleMetrics(
        symbol="ABC",
        open=100.0,
        high=105.0,
        low=99.5,
        close=101.0,
        volume=25000,
        vol_sma=10000,
        vwap=100.2,
        current_time=datetime(2026, 11, 2, 11, 15)
    )
    
    assessment = guardian.evaluate_position(
        symbol="ABC",
        direction="LONG",
        entry_price=100.0,
        current_price=101.0,
        candle=candle
    )
    
    assert assessment.trap_type == TrapType.BULL_EXHAUSTION_WICK
    assert assessment.hold_action == HoldAction.EXIT_OR_LOCK_PROFIT
    assert assessment.conviction_score < 50
    assert "Institutional selling into highs" in assessment.reasons[0]

def test_guardian_detects_bearish_absorption_trap():
    guardian = PositionGuardian()
    
    # Large lower wick: high=100.5, open=100.0, close=99.0, low=95.0.
    # Total range = 5.5. Lower wick = 99.0 - 95.0 = 4.0. Wick % = 72.7%
    # Volume surge = 2.8x
    candle = CandleMetrics(
        symbol="XYZ",
        open=100.0,
        high=100.5,
        low=95.0,
        close=99.0,
        volume=28000,
        vol_sma=10000,
        vwap=99.5,
        current_time=datetime(2026, 11, 2, 11, 15)
    )
    
    assessment = guardian.evaluate_position(
        symbol="XYZ",
        direction="SHORT",
        entry_price=100.0,
        current_price=99.0,
        candle=candle
    )
    
    assert assessment.trap_type == TrapType.BEAR_ABSORPTION_WICK
    assert assessment.hold_action == HoldAction.EXIT_OR_LOCK_PROFIT
    assert assessment.conviction_score < 50
    assert "Institutional absorption at lows" in assessment.reasons[0]

def test_guardian_confirms_strong_multi_day_btst_hold():
    guardian = PositionGuardian()
    
    # Clean continuation candle: close near high, tiny upper wick, solid volume, above VWAP
    candle = CandleMetrics(
        symbol="CHOLAFIN",
        open=1500.0,
        high=1530.0,
        low=1498.0,
        close=1528.0,
        volume=22000,
        vol_sma=12000,
        vwap=1510.0,
        current_time=datetime(2026, 11, 2, 14, 15)
    )
    
    assessment = guardian.evaluate_position(
        symbol="CHOLAFIN",
        direction="LONG",
        entry_price=1502.0,
        current_price=1528.0,
        candle=candle
    )
    
    assert assessment.trap_type == TrapType.NONE
    assert assessment.hold_action == HoldAction.HOLD_MULTI_DAY
    assert assessment.conviction_score >= 80

def test_guardian_flags_stagnant_low_volume_trade_as_intraday_only():
    guardian = PositionGuardian()
    
    # Low volume drift, mediocre candle body
    candle = CandleMetrics(
        symbol="INFY",
        open=1850.0,
        high=1855.0,
        low=1848.0,
        close=1851.0,
        volume=4000,
        vol_sma=10000,
        vwap=1850.0,
        current_time=datetime(2026, 11, 2, 12, 15)
    )
    
    assessment = guardian.evaluate_position(
        symbol="INFY",
        direction="LONG",
        entry_price=1850.0,
        current_price=1851.0,
        candle=candle
    )
    
    assert assessment.hold_action == HoldAction.HOLD_INTRADAY
    assert 50 <= assessment.conviction_score < 80
