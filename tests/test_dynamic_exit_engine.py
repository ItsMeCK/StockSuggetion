import pytest
from datetime import datetime, timedelta
from core.dynamic_exit_engine import DynamicOptionExitEngine

def test_hard_stop_loss_trigger():
    engine = DynamicOptionExitEngine()
    pos = {"entry_premium": 100.0, "current_sl": 85.0}
    # Drops to 84.0 (-16% loss)
    decision = engine.evaluate_position(pos, ltp=84.0)
    assert decision.action == "HARD_STOP"
    assert "Hard Stop Hit" in decision.reason

def test_breakeven_ratchet_trigger():
    engine = DynamicOptionExitEngine()
    pos = {"entry_premium": 100.0, "current_sl": 85.0, "trailing_active": False}
    # Surges to 116.0 (+16% gain)
    decision = engine.evaluate_position(pos, ltp=116.0)
    assert decision.action == "MOVE_TO_BREAKEVEN"
    assert decision.new_sl == 101.50 # Covers cost + 1.5% fees

def test_profit_lock_trigger():
    engine = DynamicOptionExitEngine()
    pos = {"entry_premium": 100.0, "current_sl": 101.50, "trailing_active": True}
    # Surges to 122.0 (+22% gain)
    decision = engine.evaluate_position(pos, ltp=122.0)
    assert decision.action == "PROFIT_LOCK"
    assert decision.book_lot_pct == 0.50
    assert decision.new_sl == 115.0 # Guaranteed +15% net gain locked in

def test_time_stop_stagnation_cut():
    engine = DynamicOptionExitEngine()
    entry_time = datetime(2026, 10, 11, 10, 15, 0)
    now = entry_time + timedelta(minutes=95) # 95 minutes of stagnation
    pos = {
        "entry_premium": 100.0,
        "entry_time": entry_time.isoformat(),
        "highest_high": 105.0, # max reached was only +5%
        "holding_type": "TYPE_A_SCALP"
    }
    decision = engine.evaluate_position(pos, ltp=101.0, now=now)
    assert decision.action == "TIME_STOP"
    assert "Stagnation Exit" in decision.reason

def test_taxonomy_eod_cut_type_a():
    engine = DynamicOptionExitEngine()
    entry_time = datetime(2026, 10, 11, 11, 15, 0)
    now = datetime(2026, 10, 11, 15, 1, 0) # 15:01 IST
    pos = {
        "entry_premium": 100.0,
        "entry_time": entry_time.isoformat(),
        "holding_type": "TYPE_A_SCALP"
    }
    decision = engine.evaluate_position(pos, ltp=105.0, now=now)
    assert decision.action == "TAXONOMY_CUT"
    assert "Type A Intraday Scalp 15:00 IST" in decision.reason

def test_profit_lock_single_lot():
    engine = DynamicOptionExitEngine()
    pos = {
        "entry_premium": 100.0,
        "current_sl": 101.50,
        "trailing_active": True,
        "qty": 1,
        "lot_size": 1,
        "profit_locked": False
    }
    decision = engine.evaluate_position(pos, ltp=122.0)
    assert decision.action == "PROFIT_LOCK"
    # Single lot must NOT attempt fractional lot booking
    assert decision.book_lot_pct == 0.0
    assert decision.new_sl == 115.0

def test_profit_lock_already_fired_guard():
    engine = DynamicOptionExitEngine()
    pos = {
        "entry_premium": 100.0,
        "current_sl": 115.0,
        "highest_high": 122.0,
        "trailing_active": True,
        "profit_locked": True # Already fired!
    }
    # On next tick at 122.0, should not re-trigger PROFIT_LOCK
    decision = engine.evaluate_position(pos, ltp=122.0)
    assert decision.action != "PROFIT_LOCK"
    assert decision.action in ("HOLD", "UPDATE_TRAILING_SL")

def test_statutory_friction_exact_calculation():
    from core.dynamic_exit_engine import calculate_statutory_friction
    # Scenario: 1,000 units at Rs 60 premium (Claude benchmark: ~Rs 190 round trip)
    friction = calculate_statutory_friction(buy_premium=60.0, sell_premium=60.0, qty=1000)
    assert friction["brokerage"] == 40.0
    assert friction["stt"] == 90.0 # 0.15% on Rs 60,000 sell
    assert 185.0 <= friction["total_cost"] <= 195.0
    assert friction["cost_per_unit"] < 0.20 # Less than 0.20 premium points

