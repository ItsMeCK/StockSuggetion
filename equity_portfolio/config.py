"""
Investment Policy Statement (IPS) Configuration & Statutory Fee Schedule
Requirements EQ3, EQ7, EQ8, EQ9, EQ10, EQ11, EQ15
"""

from dataclasses import dataclass
from typing import Dict, Any

@dataclass(frozen=True)
class StatutoryFees:
    STT_BUY_PCT: float = 0.10          # 0.1% on delivery buy
    STT_SELL_PCT: float = 0.10         # 0.1% on delivery sell
    STAMP_DUTY_BUY_PCT: float = 0.015  # 0.015% on buy
    NSE_TURNOVER_PCT: float = 0.00307  # 0.00307% exchange turnover
    SEBI_FEE_PCT: float = 0.0001       # Rs 10 per crore (0.0001%)
    GST_PCT: float = 18.0              # 18% on exchange, SEBI & brokerage
    CDSL_DP_CHARGE: float = 15.34      # Rs 15.34 flat per scrip sold
    BROKERAGE_PER_ORDER_INR: float = 0.0 # Rs 0 for Zerodha delivery (configurable, e.g. Rs 20 flat)
    SLIPPAGE_PCT: float = 0.05         # 5 bps conservative limit slippage

@dataclass(frozen=True)
class CapitalGainsTax:
    STCG_RATE: float = 0.20            # 20% on <= 12 months (post-July 2024 budget)
    LTCG_RATE: float = 0.125           # 12.5% on > 12 months (> 365 days)
    LTCG_EXEMPTION: float = 125_000.0  # Rs 1.25 Lakh annual exemption

@dataclass(frozen=True)
class PortfolioPolicy:
    BENCHMARK: str = "NSE NIFTY 500 Official Index"
    TARGET_HOLDINGS_MIN: int = 25
    TARGET_HOLDINGS_MAX: int = 35
    CORE_ALLOCATION_PCT: float = 0.80  # 80% Core Sleeve (1-Year Hold)
    TACTICAL_ALLOCATION_PCT: float = 0.20 # 20% Tactical Sleeve (1-Month Hold)
    
    # Sizing & Caps
    PURCHASE_WEIGHT_MIN_PCT: float = 2.5 # ~1/N for 35 stocks
    PURCHASE_WEIGHT_MAX_PCT: float = 5.0 # Max purchase cap
    HARD_CEILING_WEIGHT_PCT: float = 10.0 # Strict trim ceiling on outperforming drift
    SECTOR_CAP_PCT: float = 25.0         # Max 25% exposure per sector
    
    # Minimum Ticket Size to defeat the flat DP fee trap
    MIN_ORDER_VALUE: float = 25_000.0    # Flat Rs 15.34 DP fee is < 0.06% at Rs 25,000
    
    # Liquidity Gates
    MAX_SPREAD_PCT: float = 0.50         # Max 0.5% bid-ask spread
    MAX_VOLUME_PARTICIPATION: float = 0.01 # Max 1% of 20-day median volume
    
    # Risk & Governance Controls
    DRAWDOWN_HALT_PCT: float = -15.0     # -15% portfolio DD stops new buying
    TREND_FILTER_BUFFER_PCT: float = 2.0 # 200 SMA +/- 2% buffer band
    DAY_366_HOLD_WINDOW_DAYS: int = 15   # Day 350-364 held until Day 366 for LTCG
    
    # Provenance Governance (Fail-Closed on Unverified Data EQ2)
    REQUIRE_VERIFIED_FILINGS: bool = True # Rejects unverified sector consensus estimates

    # Macro Regime Filter (EQ48)
    ENABLE_MACRO_REGIME_FILTER: bool = True # If Nifty 500 is below 50 SMA, enter defensive cash cushion
    DEFENSIVE_MAX_EXPOSURE_PCT: float = 35.0 # Max 35% exposure during market regime correction

FEES = StatutoryFees()
TAX = CapitalGainsTax()
POLICY = PortfolioPolicy()
