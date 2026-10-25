from datetime import datetime, timedelta
from typing import Dict, Any, List, Tuple, Set, Optional
from dataclasses import dataclass

# Low-beta, sluggish FMCG and Insurance defensives (empirically shown to bleed option premium)
DEFAULT_DEFENSIVE_BLACKLIST: Set[str] = {
    'SBILIFE', 'HINDUNILVR', 'TATACONSUM', 'GODREJCP', 'ETERNAL',
    'ICICIGI', 'NESTLEIND', 'DABUR', 'MARICO', 'COLPAL'
}

@dataclass
class WatchlistProposal:
    symbol: str
    action: str  # "ADD", "REMOVE", "PAUSE"
    reason: str
    confidence: float = 0.8

@dataclass
class WatchlistDecision:
    symbol: str
    action: str  # "ACTIVE", "PENDING", "PRUNED", "REVIEW"
    is_active: bool
    new_consecutive_passes: int
    reason: str

class WatchlistManager:
    """
    3-Tier Watchlist & Fast Trigger Manager (Requirements W1 - W6)
    
    Architecture:
    - W1: Weekly Build from dated eligible list with liquidity & DTE >= 4 filters.
    - W2: Daily Pre-Open Refresh (gaps > 2.5% routed to REVIEW, not entry).
    - W4: News & Data agent proposes updates; deterministic rules decide.
    - W5: Fast removal (immediate on toxic/ban), Slow addition (>=2 consecutive passes).
    - W6: Caps per day (max 3 trades) and cooldown (30 mins after exit).
    """
    def __init__(
        self,
        max_daily_trades: int = 5,
        cooldown_minutes: int = 30,
        required_consecutive_passes: int = 2
    ):
        self.max_daily_trades = max_daily_trades
        self.cooldown_minutes = cooldown_minutes
        self.required_consecutive_passes = required_consecutive_passes

    def filter_weekly_universe(
        self,
        universe: List[Dict[str, Any]],
        banned_stocks: Optional[Set[str]] = None,
        excluded_stocks: Optional[Set[str]] = None,
        min_dte: int = 4,
        max_spread_pct: float = 2.0
    ) -> List[Dict[str, Any]]:
        """
        Requirement W1, D4, D5:
        Filters weekly candidate universe:
        - Excludes stocks on F&O Ban list.
        - Excludes blacklisted low-beta defensive names.
        - Excludes near-expiry contracts (DTE < min_dte).
        - Excludes contracts with wide bid-ask spread (> max_spread_pct).
        """
        banned = banned_stocks or set()
        excluded = excluded_stocks or set()
        approved: List[Dict[str, Any]] = []

        for item in universe:
            sym = item.get("symbol", "")
            dte = int(item.get("dte", 0))
            spread = float(item.get("bid_ask_spread_pct", 0.0))

            if sym in banned:
                continue
            if sym in excluded:
                continue
            if dte < min_dte:
                continue
            if spread > max_spread_pct:
                continue

            approved.append(item)

        return approved

    def refresh_preopen_gaps(
        self,
        candidates: List[Dict[str, Any]],
        max_gap_pct: float = 2.5
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """
        Requirement W2 (Post-NSE Discovery at 09:08 - 09:12 AM):
        - Runs after NSE completes pre-open equilibrium price matching (09:08 AM).
        - Gaps > max_gap_pct (2.5%) are routed to REVIEW list to prevent chasing.
        - Moderate gap-ups (0.0 to +2.5%) are assigned direction="LONG" (CE candidates).
        - Moderate gap-downs (-2.5% to 0.0%) are assigned direction="SHORT" (PE breakdown candidates).
        """
        active_daily: List[Dict[str, Any]] = []
        review_list: List[Dict[str, Any]] = []

        for c in candidates:
            prev_close = float(c.get("prev_close", 0.0))
            open_px = float(c.get("open", 0.0))

            if prev_close > 0:
                raw_gap = (open_px - prev_close) / prev_close * 100.0
                gap_pct = abs(raw_gap)
            else:
                raw_gap = 0.0
                gap_pct = 0.0

            cand_copy = c.copy()
            cand_copy["gap_pct"] = round(gap_pct, 2)
            cand_copy["direction"] = "LONG" if raw_gap >= 0 else "SHORT"

            if gap_pct > max_gap_pct:
                cand_copy["route_reason"] = f"Excessive gap ({gap_pct:.2f}% > {max_gap_pct}%) routed to review"
                review_list.append(cand_copy)
            else:
                active_daily.append(cand_copy)

        return active_daily, review_list

    def process_proposal(
        self,
        proposal: WatchlistProposal,
        current_status: str = "NEW",
        consecutive_passes: int = 0
    ) -> WatchlistDecision:
        """
        Requirements W4 & W5:
        - Fast Removal: Action "REMOVE" immediately demotes to PRUNED.
        - Slow Addition: Action "ADD" requires at least `required_consecutive_passes` cycles
          to become ACTIVE; otherwise stays PENDING.
        """
        sym = proposal.symbol
        if proposal.action == "REMOVE":
            return WatchlistDecision(
                symbol=sym,
                action="PRUNED",
                is_active=False,
                new_consecutive_passes=0,
                reason=proposal.reason
            )

        if proposal.action == "ADD":
            passes = consecutive_passes + 1
            if passes >= self.required_consecutive_passes:
                return WatchlistDecision(
                    symbol=sym,
                    action="ACTIVE",
                    is_active=True,
                    new_consecutive_passes=passes,
                    reason=f"Approved after {passes} consecutive cycles: {proposal.reason}"
                )
            else:
                return WatchlistDecision(
                    symbol=sym,
                    action="PENDING",
                    is_active=False,
                    new_consecutive_passes=passes,
                    reason=f"Passed cycle {passes}/{self.required_consecutive_passes}. Stays pending."
                )

        return WatchlistDecision(
            symbol=sym,
            action=current_status,
            is_active=(current_status == "ACTIVE"),
            new_consecutive_passes=consecutive_passes,
            reason="No action taken"
        )

    def check_trade_eligibility(
        self,
        symbol: str,
        trades_today_count: int,
        last_exit_time: Optional[datetime] = None,
        now: Optional[datetime] = None
    ) -> Tuple[bool, str]:
        """
        Requirement W6:
        Enforces:
        - Cap on daily trades (max_daily_trades, e.g. 3 trades).
        - Cooldown after exit on the same symbol (cooldown_minutes, e.g. 30 mins).
        """
        current_time = now or datetime.now()

        # 1. Daily Cap Check
        if trades_today_count >= self.max_daily_trades:
            return False, f"Daily trade cap reached ({trades_today_count}/{self.max_daily_trades})"

        # 2. Cooldown Check
        if last_exit_time is not None:
            elapsed_minutes = (current_time - last_exit_time).total_seconds() / 60.0
            if elapsed_minutes < self.cooldown_minutes:
                remaining = int(self.cooldown_minutes - elapsed_minutes)
                return False, f"Symbol {symbol} in post-exit cooldown ({remaining} mins remaining)"

        return True, "Eligible for entry"
