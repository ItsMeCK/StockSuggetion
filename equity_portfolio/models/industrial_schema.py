"""
Dual Business Quality Schema: Industrial & Non-Financial Corporations
Requirements EQ34, EQ40, EQ41, EQ42, EQ50, EQ51
"""

from dataclasses import dataclass
from typing import List, Tuple, Optional

@dataclass
class IndustrialMetrics:
    symbol: str
    roce_3yr_avg: float            # Return on Capital Employed (Target: > 15%)
    cfo_to_ebitda: float           # Operating Cash Flow / EBITDA (Target: >= 0.70)
    net_debt_to_ebitda: float      # Net Debt / EBITDA (Target: <= 3.0x)
    interest_coverage_ratio: float # ICR (Target: >= 3.0x)
    altman_z_score: float          # Altman Z-Score (Target: >= 1.81 safe zone)
    order_book_to_revenue: Optional[float] = None # For Capex/Infra: Target >= 1.2x
    volume_growth_pct: Optional[float] = None     # Organic volume growth vs pricing power
    consecutive_margin_drops: int = 0             # Consecutive quarters of margin decline (Max: 1)

class IndustrialQualityEvaluator:
    """
    Evaluates manufacturing, technology, infra, consumer, and pharma companies.
    Enforces deterministic quality gates and assigns a 0-100 quality score.
    """
    def __init__(
        self,
        min_roce: float = 15.0,
        min_cfo_to_ebitda: float = 0.70,
        max_net_debt_ebitda: float = 3.0,
        min_icr: float = 3.0,
        min_altman_z: float = 1.81
    ):
        self.min_roce = min_roce
        self.min_cfo_to_ebitda = min_cfo_to_ebitda
        self.max_net_debt_ebitda = max_net_debt_ebitda
        self.min_icr = min_icr
        self.min_altman_z = min_altman_z

    def evaluate(self, metrics: IndustrialMetrics) -> Tuple[bool, float, List[str]]:
        reasons = []
        score = 100.0

        # Hard Gate 1: ROCE hurdle
        if metrics.roce_3yr_avg < self.min_roce:
            reasons.append(f"ROCE {metrics.roce_3yr_avg:.1f}% below minimum {self.min_roce:.1f}%")
            score -= 30.0

        # Hard Gate 2: Cash flow conversion (CFO / EBITDA)
        if metrics.cfo_to_ebitda < self.min_cfo_to_ebitda:
            reasons.append(f"CFO/EBITDA {metrics.cfo_to_ebitda:.2f} below {self.min_cfo_to_ebitda:.2f} (poor cash conversion)")
            score -= 25.0

        # Hard Gate 3: Solvency (Net Debt / EBITDA)
        if metrics.net_debt_to_ebitda > self.max_net_debt_ebitda:
            reasons.append(f"Net Debt/EBITDA {metrics.net_debt_to_ebitda:.2f}x exceeds {self.max_net_debt_ebitda:.2f}x cap")
            score -= 25.0

        # Hard Gate 4: Interest Coverage
        if metrics.interest_coverage_ratio < self.min_icr:
            reasons.append(f"ICR {metrics.interest_coverage_ratio:.2f}x below {self.min_icr:.2f}x debt servicing floor")
            score -= 20.0

        # Warning / Soft Deductions
        if metrics.altman_z_score < self.min_altman_z:
            reasons.append(f"Altman Z-Score {metrics.altman_z_score:.2f} in distress zone (< {self.min_altman_z:.2f})")
            score -= 15.0

        if metrics.consecutive_margin_drops >= 2:
            reasons.append(f"Consecutive operating margin drops ({metrics.consecutive_margin_drops} quarters) triggered earnings veto")
            score -= 20.0

        if metrics.order_book_to_revenue is not None and metrics.order_book_to_revenue < 1.2:
            reasons.append(f"Capex order book conversion {metrics.order_book_to_revenue:.2f}x below 1.2x threshold")
            score -= 10.0

        score = max(0.0, min(100.0, score))
        # Pass requires all 4 hard gates to be met and score >= 60.0
        passed = (
            metrics.roce_3yr_avg >= self.min_roce and
            metrics.cfo_to_ebitda >= self.min_cfo_to_ebitda and
            metrics.net_debt_to_ebitda <= self.max_net_debt_ebitda and
            metrics.interest_coverage_ratio >= self.min_icr and
            metrics.consecutive_margin_drops < 2 and
            score >= 60.0
        )
        return passed, score, reasons
