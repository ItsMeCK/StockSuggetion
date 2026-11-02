"""
Portfolio Construction & Allocation Engine
Requirements EQ10, EQ11, EQ14
"""

import math
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from equity_portfolio.config import POLICY

@dataclass
class ProposedPosition:
    symbol: str
    target_weight_pct: float
    target_value_inr: float
    target_shares: int
    sector: str
    sleeve: str = "CORE"

class PortfolioAllocator:
    """
    Constructs and sizes equity delivery positions:
    1. 1/N Equal Weighting across 25 to 35 approved holdings
    2. Enforces purchase cap (5.0% - 8.0%), hard ceiling (10.0%), and sector cap (25.0%)
    3. Guarantees minimum ticket size >= Rs 25,000 to eliminate DP fee friction
    """
    def __init__(
        self,
        target_holdings_min: int = POLICY.TARGET_HOLDINGS_MIN,
        target_holdings_max: int = POLICY.TARGET_HOLDINGS_MAX,
        purchase_weight_cap: float = POLICY.PURCHASE_WEIGHT_MAX_PCT,
        hard_ceiling_pct: float = POLICY.HARD_CEILING_WEIGHT_PCT,
        sector_cap_pct: float = POLICY.SECTOR_CAP_PCT,
        min_order_value: float = POLICY.MIN_ORDER_VALUE
    ):
        self.target_holdings_min = target_holdings_min
        self.target_holdings_max = target_holdings_max
        self.purchase_weight_cap = purchase_weight_cap
        self.hard_ceiling_pct = hard_ceiling_pct
        self.sector_cap_pct = sector_cap_pct
        self.min_order_value = min_order_value

    def allocate_equal_weight(
        self,
        approved_symbols: List[str],
        symbol_sector_map: Dict[str, str],
        price_map: Dict[str, float],
        total_portfolio_nav: float,
        existing_positions: Optional[Dict[str, float]] = None, # symbol -> current market value
        sleeve: str = "CORE"
    ) -> List[ProposedPosition]:
        """
        Generates 1/N target allocations constrained by single-stock and sector caps.
        """
        existing_positions = existing_positions or {}
        n = len(approved_symbols)
        if n == 0:
            return []

        # Effective number of target holdings (clamped to 25 - 35)
        num_holdings = min(self.target_holdings_max, max(self.target_holdings_min, n))
        raw_weight_pct = min(self.purchase_weight_cap, 100.0 / float(num_holdings))

        allocated_positions: List[ProposedPosition] = []
        sector_weights: Dict[str, float] = {}

        # First pass: sort by symbol or priority
        for symbol in approved_symbols[:self.target_holdings_max]:
            price = price_map.get(symbol, 0.0)
            if price <= 0:
                continue

            sector = symbol_sector_map.get(symbol, "Diversified")
            current_sector_wt = sector_weights.get(sector, 0.0)

            # Check sector cap (max 25%)
            if current_sector_wt + raw_weight_pct > self.sector_cap_pct:
                allowed_wt = max(0.0, self.sector_cap_pct - current_sector_wt)
                if allowed_wt < 1.0: # Skip if less than 1% room left
                    continue
                weight = allowed_wt
            else:
                weight = raw_weight_pct

            target_val = (weight / 100.0) * total_portfolio_nav

            # Enforce minimum order value (Rs 25,000)
            if target_val < self.min_order_value and total_portfolio_nav >= (self.min_order_value * 10):
                target_val = self.min_order_value
                weight = (target_val / total_portfolio_nav) * 100.0

            target_shares = int(math.floor(target_val / price))
            if target_shares <= 0:
                continue

            actual_val = target_shares * price
            sector_weights[sector] = current_sector_wt + weight

            allocated_positions.append(ProposedPosition(
                symbol=symbol,
                target_weight_pct=round(weight, 2),
                target_value_inr=round(actual_val, 2),
                target_shares=target_shares,
                sector=sector,
                sleeve=sleeve
            ))

        return allocated_positions

    def check_drift_rebalance(
        self,
        current_holdings: Dict[str, float], # symbol -> current market value
        total_nav: float,
        drift_band_pct: float = 30.0 # +/- 30% relative drift
    ) -> List[Tuple[str, str, float]]:
        """
        Identifies positions requiring rebalance trim or expansion.
        Returns list of (symbol, action, amount_inr).
        """
        rebalance_actions = []
        if total_nav <= 0 or not current_holdings:
            return []

        # Standard target weight based on institutional portfolio capacity (25 to 35 holdings)
        # Prevents wild swings during early portfolio ramp-up or post-stopout holding counts
        effective_capacity = max(self.target_holdings_min, len(current_holdings))
        target_wt = min(self.purchase_weight_cap, 100.0 / float(effective_capacity))

        for symbol, mkt_val in current_holdings.items():
            current_wt = (mkt_val / total_nav) * 100.0
            
            # Hard Ceiling Check: > 10.0% trim immediately
            if current_wt >= self.hard_ceiling_pct:
                excess_val = mkt_val - ((self.hard_ceiling_pct * 0.8) / 100.0 * total_nav)
                rebalance_actions.append((symbol, "TRIM_HARD_CEILING", excess_val))
                continue

            # Relative drift band check
            relative_drift = (current_wt - target_wt) / target_wt * 100.0
            if relative_drift > drift_band_pct:
                excess = mkt_val - (target_wt / 100.0 * total_nav)
                rebalance_actions.append((symbol, "TRIM_DRIFT", excess))

        return rebalance_actions
