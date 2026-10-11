import pytest
from datetime import date, timedelta
from agents.derivatives_routing_agent import calculate_bs_call_delta, select_optimal_strike

def test_bs_call_delta_calculation():
    spot = 1000.0
    dte = 20.0
    
    # Deep ITM: Delta > 0.80
    delta_itm = calculate_bs_call_delta(spot, strike=900.0, dte_days=dte)
    assert delta_itm > 0.80
    
    # Target ITM (Delta ~ 0.65)
    delta_target = calculate_bs_call_delta(spot, strike=970.0, dte_days=dte)
    assert 0.60 <= delta_target <= 0.70
    
    # ATM (Delta ~ 0.50 - 0.55)
    delta_atm = calculate_bs_call_delta(spot, strike=1000.0, dte_days=dte)
    assert 0.50 <= delta_atm <= 0.58
    
    # OTM (Delta < 0.40)
    delta_otm = calculate_bs_call_delta(spot, strike=1050.0, dte_days=dte)
    assert delta_otm < 0.40

def test_select_optimal_strike_itm():
    today = date(2026, 10, 11)
    expiry = today + timedelta(days=18)
    
    fno_insts = [
        {"name": "RELIANCE", "instrument_type": "CE", "strike": "2700", "lot_size": 250, "expiry": expiry, "tradingsymbol": "RELIANCE26OCT2700CE"}, # ITM (Delta ~0.65)
        {"name": "RELIANCE", "instrument_type": "CE", "strike": "2800", "lot_size": 250, "expiry": expiry, "tradingsymbol": "RELIANCE26OCT2800CE"}, # ATM (Delta ~0.52)
        {"name": "RELIANCE", "instrument_type": "CE", "strike": "2900", "lot_size": 250, "expiry": expiry, "tradingsymbol": "RELIANCE26OCT2900CE"}, # OTM (Delta ~0.30)
    ]
    
    entry_price = 2800.0
    contract, exp_str, route = select_optimal_strike(fno_insts, entry_price, today_dt=today)
    
    assert route == "NFO_OPTION"
    assert contract["strike"] == "2700"
    assert contract["tradingsymbol"] == "RELIANCE26OCT2700CE"

def test_select_optimal_strike_fallback_to_equity_when_only_otm():
    today = date(2026, 10, 11)
    expiry = today + timedelta(days=18)
    
    # Only OTM strikes exist
    fno_insts = [
        {"name": "ZOMATO", "instrument_type": "CE", "strike": "350", "lot_size": 1000, "expiry": expiry, "tradingsymbol": "ZOMATO26OCT350CE"},
        {"name": "ZOMATO", "instrument_type": "CE", "strike": "380", "lot_size": 1000, "expiry": expiry, "tradingsymbol": "ZOMATO26OCT380CE"},
    ]
    
    entry_price = 300.0 # spot is 300, strikes are 350+ (deep OTM)
    contract, exp_str, route = select_optimal_strike(fno_insts, entry_price, today_dt=today)
    
    # Absolute Invariant: NEVER BUY OTM -> Falls back to Cash EQUITY
    assert route == "EQUITY"
    assert contract is None

def test_select_optimal_strike_rolls_expiry_if_dte_low():
    today = date(2026, 10, 27)
    exp_near = today + timedelta(days=2) # Only 2 DTE (decay trap)
    exp_next = today + timedelta(days=30) # Next month (30 DTE)
    
    fno_insts = [
        {"name": "TCS", "instrument_type": "CE", "strike": "3400", "lot_size": 175, "expiry": exp_near, "tradingsymbol": "TCS26OCT3400CE"},
        {"name": "TCS", "instrument_type": "CE", "strike": "3400", "lot_size": 175, "expiry": exp_next, "tradingsymbol": "TCS26NOV3400CE"},
    ]
    
    entry_price = 3500.0
    contract, exp_str, route = select_optimal_strike(fno_insts, entry_price, today_dt=today)
    
    # Should skip exp_near and pick exp_next
    assert contract is not None
    assert contract["expiry"] == exp_next
    assert "NOV" in contract["tradingsymbol"]
