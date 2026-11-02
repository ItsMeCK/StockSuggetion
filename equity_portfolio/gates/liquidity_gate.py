"""
Liquidity & DP Friction Gate
Requirements EQ7, EQ8
"""

from dataclasses import dataclass
from typing import List, Tuple
from equity_portfolio.config import POLICY, FEES

@dataclass
class LiquidityMetrics:
    symbol: str
    bid_ask_spread_pct: float     # (Ask - Bid) / Mid * 100
    daily_median_turnover_inr: float # 20-day median traded value in INR
    daily_median_volume: int      # 20-day median traded share volume
    order_value_inr: float        # Proposed buy/sell order value in INR
    order_shares: int             # Proposed share quantity

class LiquidityGatekeeper:
    """
    Enforces market microstructure gates:
    1. Maximum bid-ask spread <= 0.5%
    2. Volume participation <= 1% of 20-day median
    3. Minimum ticket size >= Rs 25,000 to suppress flat DP fee impact
    """
    def __init__(
        self,
        max_spread_pct: float = POLICY.MAX_SPREAD_PCT,
        max_volume_participation: float = POLICY.MAX_VOLUME_PARTICIPATION,
        min_order_value: float = POLICY.MIN_ORDER_VALUE,
        flat_dp_fee: float = FEES.CDSL_DP_CHARGE
    ):
        self.max_spread_pct = max_spread_pct
        self.max_volume_participation = max_volume_participation
        self.min_order_value = min_order_value
        self.flat_dp_fee = flat_dp_fee

    def evaluate(self, metrics: LiquidityMetrics) -> Tuple[bool, List[str]]:
        violations = []

        # 1. Spread Check
        if metrics.bid_ask_spread_pct > self.max_spread_pct:
            violations.append(
                f"Bid-Ask spread {metrics.bid_ask_spread_pct:.2f}% exceeds maximum limit of {self.max_spread_pct:.2f}%"
            )

        # 2. Minimum Order Value (DP Fee Dilution)
        if metrics.order_value_inr < self.min_order_value:
            dp_impact_pct = (self.flat_dp_fee / max(1.0, metrics.order_value_inr)) * 100.0
            violations.append(
                f"Order value Rs {metrics.order_value_inr:,.0f} below Rs {self.min_order_value:,.0f} floor "
                f"(DP charge Rs {self.flat_dp_fee:.2f} creates unacceptable {dp_impact_pct:.2f}% drag)"
            )

        # 3. Volume Participation Check
        if metrics.daily_median_volume > 0:
            participation_pct = metrics.order_shares / metrics.daily_median_volume
            if participation_pct > self.max_volume_participation:
                violations.append(
                    f"Order quantity {metrics.order_shares:,} is {participation_pct*100:.2f}% of daily volume "
                    f"(exceeds {self.max_volume_participation*100:.1f}% participation limit)"
                )
        elif metrics.daily_median_turnover_inr > 0:
            participation_val = metrics.order_value_inr / metrics.daily_median_turnover_inr
            if participation_val > self.max_volume_participation:
                violations.append(
                    f"Order value Rs {metrics.order_value_inr:,.0f} is {participation_val*100:.2f}% of daily turnover "
                    f"(exceeds {self.max_volume_participation*100:.1f}% limit)"
                )

        passed = len(violations) == 0
        return passed, violations
