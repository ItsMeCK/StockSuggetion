import pytest
from core.stress_grid import calculate_overnight_stress_grid

def test_calculate_overnight_stress_grid_call():
    """
    Tests 3x3 Spot x IV stress grid for an ITM Call.
    Spot shifts: -5%, 0%, +5%
    IV shifts: -20%, 0%, +30%
    """
    spot = 1000.0
    strike = 960.0  # ITM Call
    dte_days = 10
    iv = 0.25  # 25% IV
    
    grid = calculate_overnight_stress_grid(
        spot_price=spot,
        strike=strike,
        dte_days=dte_days,
        iv=iv,
        is_call=True,
        qty=100
    )
    
    assert "baseline_price" in grid
    assert "scenarios" in grid
    assert len(grid["scenarios"]) == 9  # 3 x 3
    assert "worst_case_loss_pct" in grid
    assert "worst_case_loss_rupees" in grid
    
    # For a Call, worst case should be spot down (-5%) and IV down (-20%)
    worst_scenario = min(grid["scenarios"], key=lambda s: s["pnl_pct"])
    assert worst_scenario["spot_shift_pct"] == -5.0
    assert worst_scenario["iv_shift_pct"] == -20.0
    assert grid["worst_case_loss_pct"] < 0.0

def test_calculate_overnight_stress_grid_put():
    """
    Tests 3x3 Spot x IV stress grid for an ITM Put.
    For a Put, worst case is spot up (+5%) and IV down (-20%).
    """
    spot = 1000.0
    strike = 1040.0  # ITM Put
    dte_days = 10
    iv = 0.25
    
    grid = calculate_overnight_stress_grid(
        spot_price=spot,
        strike=strike,
        dte_days=dte_days,
        iv=iv,
        is_call=False,
        qty=100
    )
    
    worst_scenario = min(grid["scenarios"], key=lambda s: s["pnl_pct"])
    assert worst_scenario["spot_shift_pct"] == +5.0
    assert worst_scenario["iv_shift_pct"] == -20.0
    assert grid["worst_case_loss_pct"] < 0.0
