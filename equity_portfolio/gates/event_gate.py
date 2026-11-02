"""
Event & Binary Risk Gate
Requirements EQ49
"""

from dataclasses import dataclass
from datetime import date, timedelta
from typing import List, Tuple, Optional, Set

# Master RBI Monetary Policy Committee (MPC) Bi-Monthly Decision Calendar
# Sourced from RBI announcements (Resolution day: Oct 7, 2026, Dec 4, 2026, etc.)
OFFICIAL_RBI_MPC_CALENDAR: List[date] = [
    date(2026, 4, 8),
    date(2026, 6, 5),
    date(2026, 8, 8),
    date(2026, 10, 7),  # Official MPC 25bp hike decision announcement (Oct 7, 2026)
    date(2026, 12, 4),  # Next scheduled MPC decision
    date(2027, 2, 5)
]

@dataclass
class EventContext:
    symbol: str
    current_date: date
    next_earnings_date: Optional[date] = None
    next_rbi_mpc_date: Optional[date] = None
    ipo_listing_date: Optional[date] = None
    anchor_lockin_expiry_date: Optional[date] = None

class EventRiskGatekeeper:
    """
    Step 7 Gate: Blocks new delivery buys in high binary risk windows:
    1. <= 48h before earnings announcements
    2. <= 48h before RBI MPC policy interest rate decisions
    3. < 60 days since IPO listing (post-IPO seasoning period)
    4. Window [-5d, +2d] around anchor lock-in expiration
    """
    def __init__(
        self,
        earnings_blackout_days: int = 2,
        rbi_blackout_days: int = 2,
        min_listing_days: int = 60,
        anchor_lockin_buffer_days: int = 5
    ):
        self.earnings_blackout_days = earnings_blackout_days
        self.rbi_blackout_days = rbi_blackout_days
        self.min_listing_days = min_listing_days
        self.anchor_lockin_buffer_days = anchor_lockin_buffer_days

    @staticmethod
    def get_next_rbi_mpc_date(current_date: date) -> Optional[date]:
        """Looks up the next upcoming RBI MPC decision from the official schedule."""
        for mpc_d in OFFICIAL_RBI_MPC_CALENDAR:
            if mpc_d >= current_date:
                return mpc_d
        return None

    def evaluate_new_buy(self, context: EventContext) -> Tuple[bool, List[str]]:
        reasons = []

        # 1. Earnings announcement blackout (<= 48h / 2 days)
        if context.next_earnings_date is not None:
            days_to_earnings = (context.next_earnings_date - context.current_date).days
            if 0 <= days_to_earnings <= self.earnings_blackout_days:
                reasons.append(
                    f"Earnings announcement scheduled in {days_to_earnings} day(s) "
                    f"({context.next_earnings_date}) - binary event blackout active"
                )

        # 2. RBI Monetary Policy Committee (MPC) blackout
        if context.next_rbi_mpc_date is not None:
            days_to_mpc = (context.next_rbi_mpc_date - context.current_date).days
            if 0 <= days_to_mpc <= self.rbi_blackout_days:
                reasons.append(
                    f"RBI MPC policy decision scheduled in {days_to_mpc} day(s) "
                    f"({context.next_rbi_mpc_date}) - macro interest rate freeze"
                )

        # 3. Post-IPO Seasoning (< 60 days)
        if context.ipo_listing_date is not None:
            days_since_listing = (context.current_date - context.ipo_listing_date).days
            if 0 <= days_since_listing < self.min_listing_days:
                reasons.append(
                    f"Stock listed only {days_since_listing} days ago (minimum {self.min_listing_days} days seasoning required)"
                )

        # 4. Anchor Lock-in Expiry Window
        if context.anchor_lockin_expiry_date is not None:
            days_to_expiry = (context.anchor_lockin_expiry_date - context.current_date).days
            if -2 <= days_to_expiry <= self.anchor_lockin_buffer_days:
                reasons.append(
                    f"Anchor investor lock-in expires on {context.anchor_lockin_expiry_date} "
                    f"({days_to_expiry} days away) - supply dilution risk"
                )

        passed = len(reasons) == 0
        return passed, reasons
