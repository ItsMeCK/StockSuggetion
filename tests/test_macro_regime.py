import pytest
from core.macro_regime import classify_macro_regime

def test_bull_expansion_regime():
    # Nifty at 25,000, 20 SMA at 24,800, 50 SMA at 24,500
    res = classify_macro_regime(nifty_close=25000.0, nifty_sma20=24800.0, nifty_sma50=24500.0, vix=13.5)
    assert res["regime"] == "BULL_EXPANSION"
    assert res["allow_ce"] is True
    assert res["allow_pe"] is False
    assert "LONG" in res["allowed_directions"]
    assert res["size_multiplier"] == 1.0

def test_bear_distribution_regime():
    # Nifty at 24,200, 20 SMA at 24,800, 50 SMA at 25,000
    res = classify_macro_regime(nifty_close=24200.0, nifty_sma20=24800.0, nifty_sma50=25000.0, vix=16.0)
    assert res["regime"] == "BEAR_DISTRIBUTION"
    assert res["allow_ce"] is False
    assert res["allow_pe"] is True
    assert "SHORT" in res["allowed_directions"]
    assert res["size_multiplier"] == 1.0

def test_neutral_chop_regime():
    # Nifty at 24,850, 20 SMA at 24,800, 50 SMA at 24,900
    res = classify_macro_regime(nifty_close=24850.0, nifty_sma20=24800.0, nifty_sma50=24900.0, vix=14.0)
    assert res["regime"] == "NEUTRAL_CHOP"
    assert res["allow_ce"] is True
    assert res["allow_pe"] is True
    assert res["size_multiplier"] == 0.5
    assert res["max_holding_hours"] == 1.5

def test_high_vix_panic_forces_bear_distribution():
    # Even if Nifty is slightly above SMA20, VIX > 22 implies tail risk / distribution
    res = classify_macro_regime(nifty_close=24850.0, nifty_sma20=24800.0, nifty_sma50=24700.0, vix=24.5)
    assert res["regime"] == "BEAR_DISTRIBUTION"
    assert res["allow_pe"] is True
    assert res["allow_ce"] is False
