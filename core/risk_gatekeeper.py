import os
import logging
from typing import Dict, Any, Tuple
from core.db_manager import get_all_active_positions

class CapitalRiskGatekeeper:
    """
    Capital Preservation Gatekeeper (v3.3.0-PROD)
    Strictly safeguards execution account HU0092 (Current Equity: ₹27,216 | Cash: ₹0).
    
    Invariants Enforced:
    1. Zero-Cash Hard Veto: Rejects trades if free cash < ₹2,500.
    2. Dynamic 25% Allocation: Max capital per trade = min(25% Free Cash, 25% Total Equity).
    3. Strict Concurrency Cap: Max 2 concurrent positions across the entire system.
    4. Capital Allocation by Confidence Tier:
       - Engine 2 BTST (Confirmed Edge, BH p=0.0121): 100% of Live Capacity.
       - Engine 1 Hourly (Unconfirmed Standalone, p>0.30): 100% PAPER-TRADE ONLY (₹0 live).
    5. Daily Drawdown Circuit Breaker: 5% equity loss halts execution for the day.
    """
    def __init__(self, max_concurrency: int = 2, max_allocation_pct: float = 0.25, daily_loss_limit_pct: float = 0.05):
        self.max_concurrency = max_concurrency
        self.max_allocation_pct = max_allocation_pct
        self.daily_loss_limit_pct = daily_loss_limit_pct
        self.min_cash_threshold = 2500.0

    def evaluate_entry(
        self,
        engine_type: str, # "ENGINE_2_BTST" or "ENGINE_1_HOURLY"
        free_cash: float,
        total_equity: float,
        current_daily_loss: float = 0.0,
        active_positions: list = None
    ) -> Tuple[bool, float, str]:
        """
        Evaluates capital allocation and returns (is_approved, allocated_capital, reason).
        """
        # 1. Daily Drawdown Circuit Breaker
        max_daily_loss = total_equity * self.daily_loss_limit_pct
        if current_daily_loss >= max_daily_loss:
            return False, 0.0, f"CIRCUIT_BREAKER_HALT: Daily loss ₹{current_daily_loss:.2f} >= 5% equity limit (₹{max_daily_loss:.2f})"

        # 2. Concurrency Check
        if active_positions is None:
            active_positions = get_all_active_positions()
        
        active_count = len(active_positions)
        if active_count >= self.max_concurrency:
            return False, 0.0, f"CONCURRENCY_CAP_REACHED: System holds {active_count} active positions (Max allowed: {self.max_concurrency})"

        # 3. Capital Allocation by Confidence Tier
        if engine_type == "ENGINE_1_HOURLY":
            # Per Claude 300-day study & Master Plan: Tracks A/B/Sweep lack standalone significance.
            # Paper-trade only until prospective forward confirmation.
            return True, 0.0, "PAPER_TRADE_ONLY: Engine 1 unverified standalone. Routed to paper simulation (₹0 live capital)."

        # 4. Zero-Cash Hard Veto for Live Trading
        if free_cash < self.min_cash_threshold:
            return False, 0.0, f"INSUFFICIENT_FREE_CASH: Available cash ₹{free_cash:.2f} < minimum required ₹{self.min_cash_threshold:.2f}"

        # 5. Dynamic 25% Allocation Cap
        max_from_cash = free_cash * self.max_allocation_pct
        max_from_equity = total_equity * self.max_allocation_pct
        allocated_capital = round(min(max_from_cash, max_from_equity), 2)

        if allocated_capital < 1000.0:
            return False, 0.0, f"ALLOCATION_TOO_LOW: Computed allocation ₹{allocated_capital:.2f} < ₹1,000"

        return True, allocated_capital, f"APPROVED_BTST: Allocated ₹{allocated_capital:.2f} (25% cash/equity ceiling)"
