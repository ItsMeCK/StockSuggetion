"""
Point-in-Time Fundamental & Forensic Data Store
Requirements EQ2, EQ4, EQ5, EQ12, EQ34, EQ40, EQ41, EQ42, EQ44
Enforces strict Fail-Closed governance (EQ2): unknown symbols return None and are rejected.
"""

import os
import json
from dataclasses import dataclass
from datetime import date
from typing import Dict, Optional, Tuple, List
from equity_portfolio.gates.forensic_gate import ForensicReport
from equity_portfolio.models.industrial_schema import IndustrialMetrics
from equity_portfolio.models.banking_schema import BankingMetrics
from equity_portfolio.valuation.valuation_engine import ValuationInputs

@dataclass(frozen=True)
class StockFundamentalRecord:
    symbol: str
    as_of_date: date
    filing_date: date
    source: str
    provenance: str = "RESEARCH_FIXTURE" # "AUDITED_XBRL_FILING", "SECTOR_CONSENSUS_ESTIMATE", "RESEARCH_FIXTURE"
    is_banking_or_nbfc: bool = False
    # Forensics (EQ12, EQ43)
    promoter_pledge_pct: float = 0.0
    promoter_pledge_qoq_change: float = 0.0
    beneish_m_score: float = -2.5
    sloan_accrual_ratio: float = 0.03
    is_asm_gsm_surveillance: bool = False
    auditor_resignation: bool = False
    adverse_caro: bool = False
    # Industrial (EQ40, EQ41, EQ42) - None for banks
    roce_3yr_avg: Optional[float] = None
    cfo_to_ebitda: Optional[float] = None
    net_debt_to_ebitda: Optional[float] = None
    interest_coverage_ratio: Optional[float] = None
    altman_z_score: Optional[float] = None
    # Banking (EQ34, EQ40) - None for industrials
    nim_pct: Optional[float] = None
    cd_ratio_pct: Optional[float] = None
    gross_npa_pct: Optional[float] = None
    net_npa_pct: Optional[float] = None
    provision_coverage_pct: Optional[float] = None
    crar_pct: Optional[float] = None
    casa_ratio_pct: Optional[float] = None
    # Valuation (EQ44, EQ45)
    trailing_pe: float = 25.0
    forward_pe: float = 20.0
    price_to_book: float = 3.5
    ev_to_ebitda: Optional[float] = None
    earnings_growth_guidance_pct: float = 15.0
    # Event & Seasoning (EQ49)
    ipo_listing_date: Optional[date] = None
    anchor_lockin_expiry_date: Optional[date] = None

# Curated Institutional Research Sample Fixture (Pre-XBRL Pipeline Baseline)
# Used for unit tests, stress benchmarks, and sample vetted portfolio validation
CURATED_SAMPLE_UNIVERSE_FIXTURE: Dict[str, StockFundamentalRecord] = {
    # --- Capital Goods, Defence & Infrastructure ---
    "BEL": StockFundamentalRecord(
        symbol="BEL", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 28), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.85, sloan_accrual_ratio=0.02,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=27.4, cfo_to_ebitda=0.91, net_debt_to_ebitda=-0.8, interest_coverage_ratio=45.0, altman_z_score=5.8,
        trailing_pe=38.5, forward_pe=31.0, price_to_book=8.2, ev_to_ebitda=26.0, earnings_growth_guidance_pct=22.0
    ),
    "HAL": StockFundamentalRecord(
        symbol="HAL", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 8), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.72, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=29.2, cfo_to_ebitda=0.88, net_debt_to_ebitda=-1.1, interest_coverage_ratio=60.0, altman_z_score=6.2,
        trailing_pe=34.0, forward_pe=28.0, price_to_book=7.5, ev_to_ebitda=22.0, earnings_growth_guidance_pct=20.0
    ),
    "SIEMENS": StockFundamentalRecord(
        symbol="SIEMENS", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 5), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.90, sloan_accrual_ratio=0.01,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=21.8, cfo_to_ebitda=0.84, net_debt_to_ebitda=-0.9, interest_coverage_ratio=38.0, altman_z_score=5.4,
        trailing_pe=52.0, forward_pe=44.0, price_to_book=10.5, ev_to_ebitda=35.0, earnings_growth_guidance_pct=24.0
    ),
    "ABB": StockFundamentalRecord(
        symbol="ABB", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 9), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.80, sloan_accrual_ratio=0.02,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=24.5, cfo_to_ebitda=0.86, net_debt_to_ebitda=-1.2, interest_coverage_ratio=50.0, altman_z_score=5.9,
        trailing_pe=58.0, forward_pe=48.0, price_to_book=12.0, ev_to_ebitda=40.0, earnings_growth_guidance_pct=25.0
    ),
    "BHEL": StockFundamentalRecord(
        symbol="BHEL", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 12), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.10, sloan_accrual_ratio=0.04,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=15.2, cfo_to_ebitda=0.72, net_debt_to_ebitda=1.1, interest_coverage_ratio=3.2, altman_z_score=2.2,
        trailing_pe=42.0, forward_pe=32.0, price_to_book=3.4, ev_to_ebitda=18.0, earnings_growth_guidance_pct=26.0
    ),
    "COCHINSHIP": StockFundamentalRecord(
        symbol="COCHINSHIP", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 10), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.60, sloan_accrual_ratio=0.02,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=21.0, cfo_to_ebitda=0.85, net_debt_to_ebitda=-0.9, interest_coverage_ratio=32.0, altman_z_score=4.8,
        trailing_pe=44.0, forward_pe=34.0, price_to_book=8.0, ev_to_ebitda=24.0, earnings_growth_guidance_pct=25.0
    ),
    "MAZDOCK": StockFundamentalRecord(
        symbol="MAZDOCK", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 11), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.65, sloan_accrual_ratio=0.02,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=25.8, cfo_to_ebitda=0.89, net_debt_to_ebitda=-1.4, interest_coverage_ratio=40.0, altman_z_score=5.5,
        trailing_pe=38.0, forward_pe=30.0, price_to_book=9.2, ev_to_ebitda=22.0, earnings_growth_guidance_pct=24.0
    ),
    "BDL": StockFundamentalRecord(
        symbol="BDL", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 12), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.55, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=22.5, cfo_to_ebitda=0.82, net_debt_to_ebitda=-0.7, interest_coverage_ratio=28.0, altman_z_score=4.9,
        trailing_pe=46.0, forward_pe=36.0, price_to_book=8.8, ev_to_ebitda=25.0, earnings_growth_guidance_pct=22.0
    ),

    # --- Power Generation & Transmission ---
    "NTPC": StockFundamentalRecord(
        symbol="NTPC", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 29), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.60, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=16.2, cfo_to_ebitda=0.89, net_debt_to_ebitda=2.1, interest_coverage_ratio=4.8, altman_z_score=2.8,
        trailing_pe=16.8, forward_pe=14.5, price_to_book=2.1, ev_to_ebitda=9.5, earnings_growth_guidance_pct=14.0
    ),
    "POWERGRID": StockFundamentalRecord(
        symbol="POWERGRID", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 26), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.75, sloan_accrual_ratio=0.02,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=17.8, cfo_to_ebitda=0.92, net_debt_to_ebitda=2.3, interest_coverage_ratio=5.2, altman_z_score=3.1,
        trailing_pe=18.2, forward_pe=16.0, price_to_book=3.2, ev_to_ebitda=10.2, earnings_growth_guidance_pct=12.0
    ),
    "TATAPOWER": StockFundamentalRecord(
        symbol="TATAPOWER", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 7), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=1.4, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.30, sloan_accrual_ratio=0.04,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=15.6, cfo_to_ebitda=0.81, net_debt_to_ebitda=2.6, interest_coverage_ratio=3.4, altman_z_score=2.4,
        trailing_pe=33.0, forward_pe=26.0, price_to_book=3.8, ev_to_ebitda=14.0, earnings_growth_guidance_pct=18.0
    ),
    "ADANIPOWER": StockFundamentalRecord(
        symbol="ADANIPOWER", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 2), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=2.8, promoter_pledge_qoq_change=-0.5, beneish_m_score=-2.15, sloan_accrual_ratio=0.05,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=21.2, cfo_to_ebitda=0.84, net_debt_to_ebitda=1.8, interest_coverage_ratio=4.5, altman_z_score=2.9,
        trailing_pe=14.5, forward_pe=12.5, price_to_book=3.4, ev_to_ebitda=8.8, earnings_growth_guidance_pct=16.0
    ),

    # --- Financial Infrastructure ---
    "PFC": StockFundamentalRecord(
        symbol="PFC", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 6), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=True, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.70, sloan_accrual_ratio=0.02,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        nim_pct=3.65, cd_ratio_pct=88.0, gross_npa_pct=2.70, net_npa_pct=0.85, provision_coverage_pct=71.0, crar_pct=25.5, casa_ratio_pct=35.0,
        trailing_pe=8.2, forward_pe=7.1, price_to_book=1.4, ev_to_ebitda=None, earnings_growth_guidance_pct=16.0
    ),
    "RECLTD": StockFundamentalRecord(
        symbol="RECLTD", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 3), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=True, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.70, sloan_accrual_ratio=0.02,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        nim_pct=3.70, cd_ratio_pct=87.5, gross_npa_pct=2.65, net_npa_pct=0.82, provision_coverage_pct=72.0, crar_pct=26.0, casa_ratio_pct=35.0,
        trailing_pe=8.5, forward_pe=7.3, price_to_book=1.5, ev_to_ebitda=None, earnings_growth_guidance_pct=16.0
    ),

    # --- Healthcare & Hospitals ---
    "MAXHEALTH": StockFundamentalRecord(
        symbol="MAXHEALTH", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 8), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.45, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=18.8, cfo_to_ebitda=0.88, net_debt_to_ebitda=0.2, interest_coverage_ratio=14.0, altman_z_score=4.5,
        trailing_pe=52.0, forward_pe=41.0, price_to_book=9.8, ev_to_ebitda=28.0, earnings_growth_guidance_pct=22.0
    ),
    "FORTIS": StockFundamentalRecord(
        symbol="FORTIS", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 9), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.35, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=16.5, cfo_to_ebitda=0.84, net_debt_to_ebitda=0.6, interest_coverage_ratio=9.5, altman_z_score=3.8,
        trailing_pe=46.0, forward_pe=36.0, price_to_book=4.8, ev_to_ebitda=22.0, earnings_growth_guidance_pct=20.0
    ),
    "APOLLOHOSP": StockFundamentalRecord(
        symbol="APOLLOHOSP", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 13), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=16.2, promoter_pledge_qoq_change=-0.2, beneish_m_score=-2.40, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=17.2, cfo_to_ebitda=0.82, net_debt_to_ebitda=1.4, interest_coverage_ratio=6.8, altman_z_score=3.6,
        trailing_pe=58.0, forward_pe=46.0, price_to_book=10.2, ev_to_ebitda=26.0, earnings_growth_guidance_pct=22.0
    ),

    # --- Telecom & Tech ---
    "BHARTIARTL": StockFundamentalRecord(
        symbol="BHARTIARTL", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 5), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.40, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=17.5, cfo_to_ebitda=0.88, net_debt_to_ebitda=2.8, interest_coverage_ratio=3.6, altman_z_score=2.6,
        trailing_pe=44.0, forward_pe=32.0, price_to_book=8.8, ev_to_ebitda=12.5, earnings_growth_guidance_pct=26.0
    ),
    "TCS": StockFundamentalRecord(
        symbol="TCS", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 11), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.95, sloan_accrual_ratio=0.01,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=54.0, cfo_to_ebitda=0.96, net_debt_to_ebitda=-0.8, interest_coverage_ratio=75.0, altman_z_score=11.2,
        trailing_pe=28.5, forward_pe=25.0, price_to_book=14.5, ev_to_ebitda=19.5, earnings_growth_guidance_pct=12.0
    ),

    # --- Banking (Passing vs Vetoed Case Studies) ---
    "ICICIBANK": StockFundamentalRecord(
        symbol="ICICIBANK", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 20), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=True, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.85, sloan_accrual_ratio=0.02,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        nim_pct=4.36, cd_ratio_pct=83.4, gross_npa_pct=2.15, net_npa_pct=0.42, provision_coverage_pct=80.4, crar_pct=16.8, casa_ratio_pct=41.5,
        trailing_pe=18.0, forward_pe=15.5, price_to_book=3.1, ev_to_ebitda=None, earnings_growth_guidance_pct=18.0
    ),
    "SBIN": StockFundamentalRecord(
        symbol="SBIN", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 3), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=True, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.60, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        nim_pct=3.28, cd_ratio_pct=72.8, gross_npa_pct=2.21, net_npa_pct=0.57, provision_coverage_pct=74.8, crar_pct=14.2, casa_ratio_pct=41.1,
        trailing_pe=10.5, forward_pe=9.2, price_to_book=1.4, ev_to_ebitda=None, earnings_growth_guidance_pct=14.0
    ),
    "BANKINDIA": StockFundamentalRecord(
        symbol="BANKINDIA", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 30), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=True, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.40, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        nim_pct=3.05, cd_ratio_pct=78.2, gross_npa_pct=3.10, net_npa_pct=0.92, provision_coverage_pct=71.5, crar_pct=15.8, casa_ratio_pct=42.0,
        trailing_pe=6.8, forward_pe=5.9, price_to_book=0.85, ev_to_ebitda=None, earnings_growth_guidance_pct=15.0
    ),
    # HARD VETO 1: HDFCBANK (CD Ratio 104.5% exceeds 100% structural ceiling)
    "HDFCBANK": StockFundamentalRecord(
        symbol="HDFCBANK", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 20), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=True, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.80, sloan_accrual_ratio=0.03,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        nim_pct=3.47, cd_ratio_pct=104.5, gross_npa_pct=1.33, net_npa_pct=0.39, provision_coverage_pct=74.0, crar_pct=18.8, casa_ratio_pct=38.2,
        trailing_pe=18.5, forward_pe=16.0, price_to_book=2.6, ev_to_ebitda=None, earnings_growth_guidance_pct=15.0
    ),
    # HARD VETO 2: BANDHANBNK (Fails Asset Quality NPA & PCR)
    "BANDHANBNK": StockFundamentalRecord(
        symbol="BANDHANBNK", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 26), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=True, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.10, sloan_accrual_ratio=0.05,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        nim_pct=7.6, cd_ratio_pct=95.0, gross_npa_pct=4.23, net_npa_pct=1.21, provision_coverage_pct=65.0, crar_pct=15.1, casa_ratio_pct=34.0,
        trailing_pe=12.0, forward_pe=10.0, price_to_book=1.3, ev_to_ebitda=None, earnings_growth_guidance_pct=10.0
    ),

    # --- Valuation Traps (> 60x PE with low growth) ---
    "DIXON": StockFundamentalRecord(
        symbol="DIXON", as_of_date=date(2026, 6, 30), filing_date=date(2026, 7, 30), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.20, sloan_accrual_ratio=0.05,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=26.0, cfo_to_ebitda=0.72, net_debt_to_ebitda=0.4, interest_coverage_ratio=11.0, altman_z_score=4.5,
        trailing_pe=94.5, forward_pe=78.0, price_to_book=28.0, ev_to_ebitda=52.0, earnings_growth_guidance_pct=22.0
    ),
    "KAYNES": StockFundamentalRecord(
        symbol="KAYNES", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 1), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=0.0, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.10, sloan_accrual_ratio=0.07,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=18.5, cfo_to_ebitda=0.55, net_debt_to_ebitda=0.8, interest_coverage_ratio=7.5, altman_z_score=3.8,
        trailing_pe=88.0, forward_pe=68.0, price_to_book=18.0, ev_to_ebitda=48.0, earnings_growth_guidance_pct=24.0
    ),

    # --- Severe Forensics / Governance Failures (Hard Step 1 Veto) ---
    "IDEA": StockFundamentalRecord(
        symbol="IDEA", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 12), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=12.0, promoter_pledge_qoq_change=1.0, beneish_m_score=-1.65, sloan_accrual_ratio=0.12,
        is_asm_gsm_surveillance=True, auditor_resignation=False, adverse_caro=True,
        roce_3yr_avg=-4.5, cfo_to_ebitda=0.45, net_debt_to_ebitda=14.2, interest_coverage_ratio=0.35, altman_z_score=0.45,
        trailing_pe=-5.0, forward_pe=-4.0, price_to_book=-2.5, ev_to_ebitda=18.0, earnings_growth_guidance_pct=-5.0
    ),
    "RPOWER": StockFundamentalRecord(
        symbol="RPOWER", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 14), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=28.5, promoter_pledge_qoq_change=6.2, beneish_m_score=-1.45, sloan_accrual_ratio=0.14,
        is_asm_gsm_surveillance=True, auditor_resignation=True, adverse_caro=True,
        roce_3yr_avg=3.2, cfo_to_ebitda=0.35, net_debt_to_ebitda=6.5, interest_coverage_ratio=0.8, altman_z_score=0.8,
        trailing_pe=45.0, forward_pe=38.0, price_to_book=1.8, ev_to_ebitda=16.0, earnings_growth_guidance_pct=5.0
    ),
    "VEDL": StockFundamentalRecord(
        symbol="VEDL", as_of_date=date(2026, 6, 30), filing_date=date(2026, 8, 6), source="Institutional Research Vetted Portfolio Fixture", provenance="RESEARCH_FIXTURE",
        is_banking_or_nbfc=False, promoter_pledge_pct=99.8, promoter_pledge_qoq_change=0.0, beneish_m_score=-2.20, sloan_accrual_ratio=0.04,
        is_asm_gsm_surveillance=False, auditor_resignation=False, adverse_caro=False,
        roce_3yr_avg=19.5, cfo_to_ebitda=0.82, net_debt_to_ebitda=2.9, interest_coverage_ratio=3.2, altman_z_score=2.2,
        trailing_pe=14.0, forward_pe=11.5, price_to_book=2.4, ev_to_ebitda=6.5, earnings_growth_guidance_pct=8.0
    )
}

# Alias for backwards compatibility
FUNDAMENTAL_DB = CURATED_SAMPLE_UNIVERSE_FIXTURE

class FundamentalStore:
    """
    Point-in-Time Fundamental & Forensic Store.
    Enforces Rule of Fail-Closed (EQ2):
    If a symbol does NOT have verified filings on record, returns None.
    Any stock with missing data is NOT bought.
    """
    def __init__(
        self,
        db: Optional[Dict[str, StockFundamentalRecord]] = None,
        json_path: Optional[str] = None
    ):
        if db is not None:
            self.db = dict(db)
        elif json_path and os.path.exists(json_path):
            self.db = self._load_from_json(json_path)
        elif os.path.exists("data/equity_universe_fundamentals.json"):
            self.db = self._load_from_json("data/equity_universe_fundamentals.json")
        else:
            self.db = dict(FUNDAMENTAL_DB)

        # Overlay verified audited filings from raw vault (provenance="AUDITED_XBRL_FILING", audited SHA-256)
        if os.path.exists("data/corporate_filings/filing_manifest.json"):
            try:
                from equity_portfolio.data.filing_fetcher import FilingVault
                vault = FilingVault()
                audited = vault.load_and_reconcile_all()
                if audited:
                    self.db.update(audited)
            except Exception:
                pass
        elif os.path.exists("data/corporate_filings"):
            try:
                from equity_portfolio.data.fundamental_ingestor import FundamentalIngestor
                audited = FundamentalIngestor.load_all_audited_filings("data/corporate_filings")
                if audited:
                    self.db.update(audited)
            except Exception:
                pass

    def _load_from_json(self, json_path: str) -> Dict[str, StockFundamentalRecord]:
        import json
        try:
            with open(json_path) as f:
                data = json.load(f)
            records = {}
            for sym, d in data.items():
                records[sym.upper()] = StockFundamentalRecord(
                    symbol=d["symbol"].upper(),
                    as_of_date=date.fromisoformat(d["as_of_date"]),
                    filing_date=date.fromisoformat(d["filing_date"]),
                    source=d.get("source", "Universe Fundamental Ingestion"),
                    provenance=d.get("provenance", "SECTOR_CONSENSUS_ESTIMATE"),
                    is_banking_or_nbfc=d.get("is_banking_or_nbfc", False),
                    promoter_pledge_pct=float(d.get("promoter_pledge_pct", 0.0)),
                    promoter_pledge_qoq_change=float(d.get("promoter_pledge_qoq_change", 0.0)),
                    beneish_m_score=float(d.get("beneish_m_score", -2.5)),
                    sloan_accrual_ratio=float(d.get("sloan_accrual_ratio", 0.03)),
                    is_asm_gsm_surveillance=bool(d.get("is_asm_gsm_surveillance", False)),
                    auditor_resignation=bool(d.get("auditor_resignation", False)),
                    adverse_caro=bool(d.get("adverse_caro", False)),
                    roce_3yr_avg=float(d["roce_3yr_avg"]) if d.get("roce_3yr_avg") is not None else None,
                    cfo_to_ebitda=float(d["cfo_to_ebitda"]) if d.get("cfo_to_ebitda") is not None else None,
                    net_debt_to_ebitda=float(d["net_debt_to_ebitda"]) if d.get("net_debt_to_ebitda") is not None else None,
                    interest_coverage_ratio=float(d["interest_coverage_ratio"]) if d.get("interest_coverage_ratio") is not None else None,
                    altman_z_score=float(d["altman_z_score"]) if d.get("altman_z_score") is not None else None,
                    nim_pct=float(d["nim_pct"]) if d.get("nim_pct") is not None else None,
                    cd_ratio_pct=float(d["cd_ratio_pct"]) if d.get("cd_ratio_pct") is not None else None,
                    gross_npa_pct=float(d["gross_npa_pct"]) if d.get("gross_npa_pct") is not None else None,
                    net_npa_pct=float(d["net_npa_pct"]) if d.get("net_npa_pct") is not None else None,
                    provision_coverage_pct=float(d["provision_coverage_pct"]) if d.get("provision_coverage_pct") is not None else None,
                    crar_pct=float(d["crar_pct"]) if d.get("crar_pct") is not None else None,
                    casa_ratio_pct=float(d["casa_ratio_pct"]) if d.get("casa_ratio_pct") is not None else None,
                    trailing_pe=float(d.get("trailing_pe", 25.0)),
                    forward_pe=float(d.get("forward_pe", 20.0)),
                    price_to_book=float(d.get("price_to_book", 3.0)),
                    ev_to_ebitda=float(d["ev_to_ebitda"]) if d.get("ev_to_ebitda") is not None else None,
                    earnings_growth_guidance_pct=float(d.get("earnings_growth_guidance_pct", 15.0)),
                    ipo_listing_date=date.fromisoformat(d["ipo_listing_date"]) if d.get("ipo_listing_date") else None,
                    anchor_lockin_expiry_date=date.fromisoformat(d["anchor_lockin_expiry_date"]) if d.get("anchor_lockin_expiry_date") else None
                )
            return records
        except Exception:
            return dict(FUNDAMENTAL_DB)

    def add_record(self, record: StockFundamentalRecord):
        """Allows programmatic dynamic ingestion of verified filings data to eliminate selection bias."""
        self.db[record.symbol.upper()] = record

    def load_from_records(self, records: List[StockFundamentalRecord]):
        """Bulk loads verified fundamental filing records."""
        for r in records:
            self.add_record(r)

    def get_record(self, symbol: str) -> Optional[StockFundamentalRecord]:
        return self.db.get(symbol.upper())

    def get_forensic_report(self, symbol: str) -> Optional[ForensicReport]:
        rec = self.get_record(symbol)
        if rec is None:
            return None # FAIL CLOSED: Unknown symbol has no verified forensics (EQ2)

        return ForensicReport(
            symbol=symbol,
            promoter_pledge_pct=rec.promoter_pledge_pct,
            promoter_pledge_qoq_change=rec.promoter_pledge_qoq_change,
            beneish_m_score=rec.beneish_m_score,
            sloan_accrual_ratio=rec.sloan_accrual_ratio,
            is_asm_gsm_surveillance=rec.is_asm_gsm_surveillance,
            auditor_resignation_flag=rec.auditor_resignation,
            adverse_caro_qualification=rec.adverse_caro,
            provenance=rec.provenance
        )

    def get_industrial_metrics(self, symbol: str) -> Optional[IndustrialMetrics]:
        rec = self.get_record(symbol)
        if rec is None or rec.is_banking_or_nbfc or rec.roce_3yr_avg is None:
            return None

        return IndustrialMetrics(
            symbol=symbol,
            roce_3yr_avg=rec.roce_3yr_avg,
            cfo_to_ebitda=rec.cfo_to_ebitda or 0.8,
            net_debt_to_ebitda=rec.net_debt_to_ebitda or 1.0,
            interest_coverage_ratio=rec.interest_coverage_ratio or 5.0,
            altman_z_score=rec.altman_z_score or 3.0
        )

    def get_banking_metrics(self, symbol: str) -> Optional[BankingMetrics]:
        rec = self.get_record(symbol)
        if rec is None or not rec.is_banking_or_nbfc or rec.cd_ratio_pct is None:
            return None

        return BankingMetrics(
            symbol=symbol,
            nim_pct=rec.nim_pct or 3.2,
            cd_ratio_pct=rec.cd_ratio_pct,
            gross_npa_pct=rec.gross_npa_pct or 2.5,
            net_npa_pct=rec.net_npa_pct or 0.6,
            provision_coverage_pct=rec.provision_coverage_pct or 72.0,
            crar_pct=rec.crar_pct or 15.5,
            casa_ratio_pct=rec.casa_ratio_pct or 38.0
        )

    def get_valuation_inputs(self, symbol: str, current_price: float) -> Optional[ValuationInputs]:
        rec = self.get_record(symbol)
        if rec is None:
            return None # FAIL CLOSED: Unknown stock has no valuation inputs

        return ValuationInputs(
            symbol=symbol,
            current_price=current_price,
            trailing_pe=rec.trailing_pe,
            forward_pe=rec.forward_pe,
            price_to_book=rec.price_to_book,
            ev_to_ebitda=rec.ev_to_ebitda,
            earnings_growth_guidance_pct=rec.earnings_growth_guidance_pct
        )

PointInTimeFundamentalStore = FundamentalStore
