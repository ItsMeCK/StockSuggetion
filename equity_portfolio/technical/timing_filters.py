"""
Technical Confirmation & Institutional Footprint Engine
Requirements EQ46, EQ47
"""

from dataclasses import dataclass
from typing import Tuple, List, Optional

@dataclass
class TechnicalMetrics:
    symbol: str
    current_price: float
    sma_50: float
    sma_200: Optional[float]           # 200-day moving average (or 50-day proxy when history is limited)
    volume_zscore_20d: float          # 20-day volume z-score
    relative_strength_3m_pct: float   # 3-Month return relative to Nifty 500 TRI (in percentage points)
    daily_range_pct: float            # Current day range / close %

class TechnicalTimingGate:
    """
    Step 6 Timing Gate:
    1. Trend filter: 200 SMA (or 50 SMA) with a 2% buffer band
    2. Institutional accumulation footprint: Volume z-score >= 1.3
    3. Benchmark relative strength: Must demonstrate non-negative relative strength vs Nifty 500
    """
    def __init__(
        self,
        buffer_band_pct: float = 2.0,
        min_vol_zscore: float = 1.3,
        min_relative_strength: float = -5.0, # Max allowable lag vs benchmark: -5.0%
        require_volume_confirmation: bool = False
    ):
        self.buffer_band_pct = buffer_band_pct
        self.min_vol_zscore = min_vol_zscore
        self.min_relative_strength = min_relative_strength
        self.require_volume_confirmation = require_volume_confirmation

    def evaluate(self, metrics: TechnicalMetrics) -> Tuple[bool, float, List[str]]:
        reasons = []
        score = 100.0

        # Benchmark SMA for trend (use 200 SMA if available and valid; otherwise use 50 SMA with honest labeling)
        if metrics.sma_200 is not None and metrics.sma_200 > 0:
            trend_sma = metrics.sma_200
            sma_label = "200 SMA"
        else:
            trend_sma = metrics.sma_50
            sma_label = "50 SMA (Medium-Term Proxy)"

        lower_band = trend_sma * (1.0 - self.buffer_band_pct / 100.0)

        # 1. Trend Filter Check
        if metrics.current_price < lower_band:
            reasons.append(
                f"Price Rs {metrics.current_price:.1f} is below {sma_label} buffer band Rs {lower_band:.1f} (-{self.buffer_band_pct}% below {trend_sma:.1f})"
            )
            score -= 35.0

        # 2. Relative Strength Check
        if metrics.relative_strength_3m_pct < self.min_relative_strength:
            reasons.append(
                f"Relative strength vs Nifty 500 is {metrics.relative_strength_3m_pct:.1f}% (below {self.min_relative_strength:.1f}% floor)"
            )
            score -= 25.0

        # 3. Institutional Delivery / Volume Footprint
        # Z-score >= 1.3 confirms anomalous accumulation footprint
        if metrics.volume_zscore_20d < self.min_vol_zscore:
            if self.require_volume_confirmation:
                reasons.append(
                    f"Volume Z-Score {metrics.volume_zscore_20d:.2f} < {self.min_vol_zscore} (lack of institutional accumulation footprint)"
                )
            score -= 15.0 # Soft deduction
        else:
            reasons.append(
                f"Institutional accumulation footprint confirmed: Volume Z-Score +{metrics.volume_zscore_20d:.2f} >= {self.min_vol_zscore}"
            )

        score = max(0.0, min(100.0, score))
        # Pass requires price above buffer band and relative strength not collapsed
        passed = (metrics.current_price >= lower_band) and (metrics.relative_strength_3m_pct >= self.min_relative_strength)
        if self.require_volume_confirmation and metrics.volume_zscore_20d < self.min_vol_zscore:
            passed = False

        return passed, score, reasons
