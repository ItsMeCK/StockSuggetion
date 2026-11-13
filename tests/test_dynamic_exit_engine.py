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

def test_btst_positions_exempt_from_90m_time_stop():
    engine = DynamicOptionExitEngine()
    entry_time = datetime(2026, 10, 11, 15, 15, 0)
    # Next day at 09:10 AM (1075 minutes later)
    now = datetime(2026, 10, 12, 9, 10, 0)
    pos = {
        "entry_premium": 100.0,
        "entry_time": entry_time.isoformat(),
        "highest_high": 102.0,
        "holding_type": "TYPE_C_BTST"
    }
    decision = engine.evaluate_position(pos, ltp=100.0, now=now)
    # BTST position held overnight must NOT be cut by 90m time stop
    assert decision.action != "TIME_STOP"
    assert decision.action == "HOLD"

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

def test_btst_opening_freeze_blocks_hard_stop_before_0930():
    engine = DynamicOptionExitEngine()
    pos = {
        "entry_premium": 100.0,
        "current_sl": 85.0,
        "holding_type": "TYPE_C_BTST"
    }
    # 09:16 AM opening tick drop to 80.0 (-20% loss)
    now_0916 = datetime(2026, 11, 13, 9, 16, 0)
    decision = engine.evaluate_position(pos, ltp=80.0, now=now_0916)
    assert decision.action == "HOLD"
    assert "BTST Opening" in decision.reason or "Freeze" in decision.reason

def test_btst_allows_hard_stop_after_0930():
    engine = DynamicOptionExitEngine()
    pos = {
        "entry_premium": 100.0,
        "current_sl": 85.0,
        "holding_type": "TYPE_C_BTST"
    }
    # 09:31 AM drop to 80.0 (-20% loss, freeze has elapsed)
    now_0931 = datetime(2026, 11, 13, 9, 31, 0)
    decision = engine.evaluate_position(pos, ltp=80.0, now=now_0931)
    assert decision.action == "HARD_STOP"
    assert "Hard Stop Hit" in decision.reason

def test_btst_opening_freeze_allows_profit_lock_before_0930():
    engine = DynamicOptionExitEngine()
    pos = {
        "entry_premium": 100.0,
        "current_sl": 85.0,
        "holding_type": "TYPE_C_BTST",
        "trailing_active": False,
        "profit_locked": False,
        "qty": 100,
        "lot_size": 100
    }
    # 09:18 AM surge to 125.0 (+25% gain)
    now_0918 = datetime(2026, 11, 13, 9, 18, 0)
    decision = engine.evaluate_position(pos, ltp=125.0, now=now_0918)
    assert decision.action == "PROFIT_LOCK"
    assert decision.new_sl == 115.0

def test_scalp_not_subject_to_btst_opening_freeze():
    engine = DynamicOptionExitEngine()
    pos = {
        "entry_premium": 100.0,
        "current_sl": 85.0,
        "holding_type": "TYPE_A_SCALP"
    }
    # Scalp at 09:16 AM dropping to 80.0 (-20% loss)
    now_0916 = datetime(2026, 11, 13, 9, 16, 0)
    decision = engine.evaluate_position(pos, ltp=80.0, now=now_0916)
    assert decision.action == "HARD_STOP"


