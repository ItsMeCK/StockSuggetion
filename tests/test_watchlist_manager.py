import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from core.watchlist_manager import (
    WatchlistProposal,
    WatchlistManager
)

def test_watchlist_manager_w1_weekly_build_filters_bans_and_near_expiry():
    """
    Requirement W1: Builds weekly watchlist from eligible list,
    filtering out banned stocks and contracts with DTE < 4 days.
    """
    manager = WatchlistManager()
    universe = [
        {"symbol": "RELIANCE", "dte": 10, "oi": 50000, "bid_ask_spread_pct": 0.8},
        {"symbol": "TATASTEEL", "dte": 2, "oi": 40000, "bid_ask_spread_pct": 1.0}, # Near expiry DTE < 4 -> blocked
        {"symbol": "SAIL", "dte": 12, "oi": 30000, "bid_ask_spread_pct": 0.9}, # In F&O ban -> blocked
        {"symbol": "INFY", "dte": 15, "oi": 60000, "bid_ask_spread_pct": 0.5}
    ]
    banned_stocks = {"SAIL"}

    result = manager.filter_weekly_universe(universe, banned_stocks=banned_stocks, min_dte=4)
    symbols = [r["symbol"] for r in result]

    assert "RELIANCE" in symbols
    assert "INFY" in symbols
    assert "TATASTEEL" not in symbols  # Blocked by DTE < 4 (D4 / W1)
    assert "SAIL" not in symbols       # Blocked by F&O Ban (D5 / W1)

def test_watchlist_manager_w2_daily_refresh_routes_large_gaps_to_review():
    """
    Requirement W2: Pre-open refresh routes symbols with gaps > 2.5%
    to 'REVIEW' list instead of active entry, preventing gap-chasing.
    """
    manager = WatchlistManager()
    candidates = [
        {"symbol": "RELIANCE", "prev_close": 2800.0, "open": 2820.0}, # Gap = +0.71% -> Passes to DAILY
        {"symbol": "INFY", "prev_close": 1800.0, "open": 1860.0}      # Gap = +3.33% > 2.5% -> REVIEW
    ]

    active_daily, review_list = manager.refresh_preopen_gaps(candidates, max_gap_pct=2.5)

    assert "RELIANCE" in [c["symbol"] for c in active_daily]
    assert "INFY" in [c["symbol"] for c in review_list]
    assert "INFY" not in [c["symbol"] for c in active_daily]

def test_watchlist_manager_w4_w5_fast_removal_on_toxic_news():
    """
    Requirements W4 & W5:
    Fast Removal: Any negative/toxic news flag or ban removes the symbol immediately on the next check.
    """
    manager = WatchlistManager()
    proposal = WatchlistProposal(
        symbol="ADANIENT",
        action="REMOVE",
        reason="Toxic flag: Regulatory probe announced",
        confidence=0.95
    )
    decision = manager.process_proposal(proposal, current_status="ACTIVE", consecutive_passes=5)
    
    assert decision.action == "PRUNED"
    assert "Regulatory probe" in decision.reason
    assert decision.is_active is False

def test_watchlist_manager_w5_slow_addition_requires_consecutive_passes():
    """
    Requirement W5:
    Slow Addition: A newly proposed symbol must pass verification on at least
    2 consecutive evaluation cycles before becoming ACTIVE, preventing churn.
    """
    manager = WatchlistManager()
    proposal = WatchlistProposal(
        symbol="HAL",
        action="ADD",
        reason="Strong defense order win catalyst",
        confidence=0.90
    )
    # First cycle pass (consecutive_passes = 0 -> 1) -> stays PENDING
    decision_1 = manager.process_proposal(proposal, current_status="NEW", consecutive_passes=0)
    assert decision_1.action == "PENDING"
    assert decision_1.is_active is False
    assert decision_1.new_consecutive_passes == 1

    # Second cycle pass (consecutive_passes = 1 -> 2) -> promoted to ACTIVE
    decision_2 = manager.process_proposal(proposal, current_status="PENDING", consecutive_passes=1)
    assert decision_2.action == "ACTIVE"
    assert decision_2.is_active is True
    assert decision_2.new_consecutive_passes == 2

def test_watchlist_manager_w6_caps_and_cooldown():
    """
    Requirement W6: Enforces max trades per day, cooldown after exit, and concurrency cap.
    """
    manager = WatchlistManager(max_daily_trades=3, cooldown_minutes=30)
    now = datetime(2026, 10, 25, 11, 0)

    # 1. Cooldown test: exit occurred 10 minutes ago (< 30m cooldown)
    last_exit_time = now - timedelta(minutes=10)
    can_trade, reason = manager.check_trade_eligibility(
        symbol="TCS",
        trades_today_count=1,
        last_exit_time=last_exit_time,
        now=now
    )
    assert can_trade is False
    assert "cooldown" in reason.lower()

    # 2. Daily cap test: 3 trades already executed
    last_exit_time_old = now - timedelta(minutes=45)
    can_trade_cap, reason_cap = manager.check_trade_eligibility(
        symbol="TCS",
        trades_today_count=3,
        last_exit_time=last_exit_time_old,
        now=now
    )
    assert can_trade_cap is False
    assert "daily trade cap" in reason_cap.lower()

    # 3. Eligible test: 1 trade today, exit was 45 mins ago
    can_trade_ok, reason_ok = manager.check_trade_eligibility(
        symbol="TCS",
        trades_today_count=1,
        last_exit_time=last_exit_time_old,
        now=now
    )
    assert can_trade_ok is True
