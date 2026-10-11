import pytest
from core.risk_gatekeeper import CapitalRiskGatekeeper

def test_zero_cash_hard_veto():
    gatekeeper = CapitalRiskGatekeeper()
    # Account HU0092 reality: ₹0 free cash, ₹27,216 equity
    approved, capital, reason = gatekeeper.evaluate_entry(
        engine_type="ENGINE_2_BTST",
        free_cash=0.0,
        total_equity=27216.0,
        active_positions=[]
    )
    assert approved is False
    assert capital == 0.0
    assert "INSUFFICIENT_FREE_CASH" in reason

def test_dynamic_25_pct_allocation_btst():
    gatekeeper = CapitalRiskGatekeeper()
    # With ₹10,000 free cash and ₹27,216 equity:
    # 25% of 10,000 = 2,500; 25% of 27,216 = 6,804 -> allocates min = 2,500
    approved, capital, reason = gatekeeper.evaluate_entry(
        engine_type="ENGINE_2_BTST",
        free_cash=10000.0,
        total_equity=27216.0,
        active_positions=[]
    )
    assert approved is True
    assert capital == 2500.0
    assert "APPROVED_BTST" in reason

def test_engine_1_paper_only_zero_live_capital():
    gatekeeper = CapitalRiskGatekeeper()
    # Engine 1 has unverified standalone edge -> must route to paper simulation with ₹0 live money
    approved, capital, reason = gatekeeper.evaluate_entry(
        engine_type="ENGINE_1_HOURLY",
        free_cash=20000.0,
        total_equity=27216.0,
        active_positions=[]
    )
    assert approved is True
    assert capital == 0.0
    assert "PAPER_TRADE_ONLY" in reason

def test_concurrency_cap_max_2_trades():
    gatekeeper = CapitalRiskGatekeeper(max_concurrency=2)
    # Already holding 2 active trades
    approved, capital, reason = gatekeeper.evaluate_entry(
        engine_type="ENGINE_2_BTST",
        free_cash=15000.0,
        total_equity=27216.0,
        active_positions=[{"order_id": "ORD1"}, {"order_id": "ORD2"}]
    )
    assert approved is False
    assert "CONCURRENCY_CAP_REACHED" in reason

def test_5_pct_daily_drawdown_circuit_breaker():
    gatekeeper = CapitalRiskGatekeeper(daily_loss_limit_pct=0.05)
    # Total equity ₹27,216 -> 5% is ₹1,360.80
    # Current loss is ₹1,400 -> trips circuit breaker
    approved, capital, reason = gatekeeper.evaluate_entry(
        engine_type="ENGINE_2_BTST",
        free_cash=15000.0,
        total_equity=27216.0,
        current_daily_loss=1400.0,
        active_positions=[]
    )
    assert approved is False
    assert "CIRCUIT_BREAKER_HALT" in reason
