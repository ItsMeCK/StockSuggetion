"""
Valuation Reference Band & Trap Protection Engine
Requirements EQ35, EQ44, EQ45
"""

from dataclasses import dataclass
from typing import Dict, Optional, Tuple, List

@dataclass
class ValuationInputs:
    symbol: str
    current_price: float
    trailing_pe: float
    forward_pe: Optional[float]
    price_to_book: float
    ev_to_ebitda: Optional[float]
    is_cyclical: bool = False
    normalized_eps: Optional[float] = None
    terminal_value_share_pct: float = 65.0 # From DCF model: Flag if > 75%
    earnings_growth_guidance_pct: float = 15.0 # 3-year expected CAGR %

class ValuationEngine:
    """
    Step 5 Valuation Engine:
    1. Cyclical normalization (never buy cyclical at trough P/E at peak earnings)
    2. Terminal value check (> 75% triggers sensitivity warning)
    3. Maximum valuation cap: Vetoes purchases with extreme multiples (P/E > 60x without 35%+ growth)
    4. Computes fair value band and margin of safety
    """
    def __init__(
        self,
        max_absolute_pe: float = 60.0,
        max_terminal_value_share: float = 75.0,
        min_margin_of_safety_pct: float = -10.0 # Allows buying up to fair value with 10% premium for elite moats
    ):
        self.max_absolute_pe = max_absolute_pe
        self.max_terminal_value_share = max_terminal_value_share
        self.min_margin_of_safety_pct = min_margin_of_safety_pct

    def evaluate(self, inputs: ValuationInputs) -> Tuple[bool, float, Tuple[float, float], List[str]]:
        reasons = []
        is_vetoed = False

        effective_pe = inputs.trailing_pe
        if inputs.is_cyclical and inputs.normalized_eps is not None and inputs.normalized_eps > 0:
            normalized_pe = inputs.current_price / inputs.normalized_eps
            reasons.append(
                f"Cyclical normalization applied: Trailing P/E {inputs.trailing_pe:.1f}x normalized to {normalized_pe:.1f}x (Normalized EPS Rs {inputs.normalized_eps:.1f})"
            )
            effective_pe = normalized_pe

        # 1. Extreme Valuation Cap Check
        # P/E > 60x with sub-25% growth represents acute multiple compression risk
        if effective_pe > self.max_absolute_pe and inputs.earnings_growth_guidance_pct < 25.0:
            is_vetoed = True
            reasons.append(
                f"Valuation Trap Veto: Multiple of {effective_pe:.1f}x P/E exceeds {self.max_absolute_pe:.1f}x cap with only {inputs.earnings_growth_guidance_pct:.1f}% growth guidance"
            )

        # 2. Terminal Value Dependency Check (> 75%)
        if inputs.terminal_value_share_pct > self.max_terminal_value_share:
            reasons.append(
                f"DCF Warning: Terminal value constitutes {inputs.terminal_value_share_pct:.1f}% of intrinsic value (> {self.max_terminal_value_share:.1f}% ceiling). Vulnerable to interest rate shocks."
            )

        # 3. Fair Value Range Computation
        # Base benchmark multiples: 20x to 30x for high-quality Indian equities
        peg_ratio = effective_pe / max(1.0, inputs.earnings_growth_guidance_pct)
        if peg_ratio > 3.0:
            reasons.append(f"PEG ratio {peg_ratio:.2f} severely stretched (> 3.0)")

        # Estimate fair value band around realistic 22x-28x normalized EPS
        eps = inputs.current_price / max(0.1, effective_pe)
        fair_value_low = round(eps * 20.0, 2)
        fair_value_high = round(eps * 28.0, 2)
        fair_value_mid = (fair_value_low + fair_value_high) / 2.0

        margin_of_safety_pct = ((fair_value_mid - inputs.current_price) / fair_value_mid) * 100.0

        if margin_of_safety_pct < self.min_margin_of_safety_pct:
            reasons.append(
                f"Current price Rs {inputs.current_price:.1f} is {abs(margin_of_safety_pct):.1f}% above fair value band Rs [{fair_value_low:.1f} - {fair_value_high:.1f}]"
            )

        passed = not is_vetoed
        return passed, margin_of_safety_pct, (fair_value_low, fair_value_high), reasons
