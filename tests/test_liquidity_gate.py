import pytest
from core.liquidity_gate import evaluate_bid_ask_spread

def test_evaluate_bid_ask_spread_tight_spread_passes():
    """Tight spread below 3.5% should pass."""
    depth = {
        "buy": [{"price": 100.0, "quantity": 1000}],
        "sell": [{"price": 101.0, "quantity": 1000}]
    }
    # Mid = 100.5, spread = 1.0 / 100.5 * 100 = 0.995%
    is_liquid, spread_pct, reason = evaluate_bid_ask_spread(depth, max_spread_pct=3.5)
    assert is_liquid is True
    assert spread_pct == pytest.approx(0.995, abs=0.01)
    assert "acceptable" in reason

def test_evaluate_bid_ask_spread_wide_spread_vetoed():
    """Wide spread above 3.5% should be vetoed."""
    depth = {
        "buy": [{"price": 100.0, "quantity": 500}],
        "sell": [{"price": 105.0, "quantity": 500}]
    }
    # Mid = 102.5, spread = 5.0 / 102.5 * 100 = 4.878%
    is_liquid, spread_pct, reason = evaluate_bid_ask_spread(depth, max_spread_pct=3.5)
    assert is_liquid is False
    assert spread_pct == pytest.approx(4.878, abs=0.01)
    assert "exceeds max threshold" in reason

def test_evaluate_bid_ask_spread_empty_depth_fails():
    """Empty buy or sell side should fail."""
    depth = {
        "buy": [],
        "sell": [{"price": 100.0, "quantity": 500}]
    }
    is_liquid, spread_pct, reason = evaluate_bid_ask_spread(depth)
    assert is_liquid is False
    assert "Missing" in reason

def test_evaluate_bid_ask_spread_zero_price_fails():
    """Zero bid price should fail."""
    depth = {
        "buy": [{"price": 0.0, "quantity": 0}],
        "sell": [{"price": 100.0, "quantity": 500}]
    }
    is_liquid, spread_pct, reason = evaluate_bid_ask_spread(depth)
    assert is_liquid is False
    assert "Non-positive" in reason
