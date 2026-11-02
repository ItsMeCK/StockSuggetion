"""
Portfolio Risk Guard & Drawdown Circuit Breaker
Requirements EQ15, EQ16
"""

from dataclasses import dataclass
from typing import Tuple, List, Optional
from equity_portfolio.config import POLICY

@dataclass
class RiskState:
    peak_nav: float
    current_nav: float
    current_drawdown_pct: float
    is_buying_halted: bool
    halt_reason: Optional[str]
    kill_switch_active: bool

class PortfolioRiskGuard:
    """
    Step 4 Risk Engine:
    1. Tracks high-water mark NAV and computes portfolio drawdown.
    2. -15% Drawdown Circuit Breaker automatically halts all new purchases.
    3. Emergency software kill switch.
    4. CRITICAL INVARIANT: Sells and risk liquidations are NEVER blocked.
    """
    def __init__(
        self,
        drawdown_halt_pct: float = POLICY.DRAWDOWN_HALT_PCT,
        initial_nav: float = 1_000_000.0
    ):
        self.drawdown_halt_pct = drawdown_halt_pct
        self.peak_nav = initial_nav
        self.current_nav = initial_nav
        self.is_buying_halted = False
        self.halt_reason: Optional[str] = None
        self.kill_switch_active = False

    def update_nav(self, current_nav: float) -> RiskState:
        self.current_nav = current_nav
        if current_nav > self.peak_nav:
            self.peak_nav = current_nav

        dd_pct = ((self.current_nav - self.peak_nav) / self.peak_nav) * 100.0 if self.peak_nav > 0 else 0.0

        # Trigger -15% Portfolio Drawdown Circuit Breaker
        if dd_pct <= self.drawdown_halt_pct and not self.is_buying_halted:
            self.is_buying_halted = True
            self.halt_reason = (
                f"Portfolio drawdown reached {dd_pct:.2f}% (exceeds {self.drawdown_halt_pct:.1f}% threshold). "
                f"New buying suspended. Peak NAV Rs {self.peak_nav:,.2f}, Current Rs {self.current_nav:,.2f}."
            )

        return self.get_state()

    def set_kill_switch(self, active: bool, reason: str = "Manual Admin Intervention"):
        self.kill_switch_active = active
        if active:
            self.halt_reason = f"Kill switch activated: {reason}"

    def authorize_resume(self, operator_id: str):
        """
        Clears the buying halt upon manual authorization.
        """
        self.is_buying_halted = False
        self.kill_switch_active = False
        self.halt_reason = None

    def can_open_new_position(self) -> Tuple[bool, Optional[str]]:
        """
        Enforces buying restrictions. Returns (can_buy, reason_if_blocked).
        """
        if self.kill_switch_active:
            return False, f"Blocked: Emergency Kill Switch is ACTIVE ({self.halt_reason})"
        if self.is_buying_halted:
            return False, f"Blocked: Portfolio Drawdown Circuit Breaker ACTIVE ({self.halt_reason})"
        return True, None

    def can_exit_position(self) -> Tuple[bool, Optional[str]]:
        """
        INVARIANT: Sells are never blocked by risk controls or circuit breakers.
        """
        return True, None

    def get_state(self) -> RiskState:
        dd_pct = ((self.current_nav - self.peak_nav) / self.peak_nav) * 100.0 if self.peak_nav > 0 else 0.0
        return RiskState(
            peak_nav=round(self.peak_nav, 2),
            current_nav=round(self.current_nav, 2),
            current_drawdown_pct=round(dd_pct, 2),
            is_buying_halted=self.is_buying_halted or self.kill_switch_active,
            halt_reason=self.halt_reason,
            kill_switch_active=self.kill_switch_active
        )
