"""
Dual Business Quality Schema: Banking & NBFC Financial Institutions
Requirements EQ34, EQ40
"""

from dataclasses import dataclass
from typing import List, Tuple

@dataclass
class BankingMetrics:
    symbol: str
    nim_pct: float                 # Net Interest Margin (Target: >= 3.0%)
    cd_ratio_pct: float            # Credit-to-Deposit Ratio (Target: < 100.0% strict ceiling)
    gross_npa_pct: float           # Gross NPA (Target: <= 3.5%)
    net_npa_pct: float             # Net NPA (Target: <= 1.0%)
    provision_coverage_pct: float  # Provision Coverage Ratio PCR (Target: >= 70.0%)
    crar_pct: float                # Capital Adequacy Ratio (Target: >= 15.0%)
    casa_ratio_pct: float          # CASA Ratio (Target: >= 35.0% for low-cost deposit franchise)

class BankingQualityEvaluator:
    """
    Evaluates Banks and NBFCs using prudential and asset quality metrics.
    Replaces irrelevant industrial ratios (ROCE, EBITDA, Net Debt) with
    NIM, CD Ratio, NPA provisioning, and capital adequacy.
    """
    def __init__(
        self,
        min_nim: float = 3.0,
        max_cd_ratio: float = 100.0,
        max_gnpa: float = 3.5,
        max_nnpa: float = 1.0,
        min_pcr: float = 70.0,
        min_crar: float = 15.0,
        min_casa: float = 35.0
    ):
        self.min_nim = min_nim
        self.max_cd_ratio = max_cd_ratio
        self.max_gnpa = max_gnpa
        self.max_nnpa = max_nnpa
        self.min_pcr = min_pcr
        self.min_crar = min_crar
        self.min_casa = min_casa

    def evaluate(self, metrics: BankingMetrics) -> Tuple[bool, float, List[str]]:
        reasons = []
        score = 100.0

        # Hard Gate 1: Credit-Deposit Ratio Ceiling (< 100%)
        # Over-leveraged loan book without deposit cushion triggers immediate rejection
        if metrics.cd_ratio_pct >= self.max_cd_ratio:
            reasons.append(f"Credit-Deposit (CD) Ratio {metrics.cd_ratio_pct:.1f}% exceeds structural ceiling {self.max_cd_ratio:.1f}%")
            score -= 40.0

        # Hard Gate 2: Asset Quality (Gross & Net NPA)
        if metrics.gross_npa_pct > self.max_gnpa:
            reasons.append(f"Gross NPA {metrics.gross_npa_pct:.2f}% exceeds {self.max_gnpa:.2f}% cap")
            score -= 25.0

        if metrics.net_npa_pct > self.max_nnpa:
            reasons.append(f"Net NPA {metrics.net_npa_pct:.2f}% exceeds {self.max_nnpa:.2f}% cap")
            score -= 25.0

        # Hard Gate 3: Provision Coverage Ratio
        if metrics.provision_coverage_pct < self.min_pcr:
            reasons.append(f"PCR {metrics.provision_coverage_pct:.1f}% below {self.min_pcr:.1f}% prudential buffer")
            score -= 20.0

        # Hard Gate 4: Capital Adequacy (CRAR)
        if metrics.crar_pct < self.min_crar:
            reasons.append(f"CRAR {metrics.crar_pct:.1f}% below {self.min_crar:.1f}% buffer ceiling")
            score -= 20.0

        # Soft Gate: Margin & CASA Franchise
        if metrics.nim_pct < self.min_nim:
            reasons.append(f"NIM {metrics.nim_pct:.2f}% below {self.min_nim:.2f}% target")
            score -= 15.0

        if metrics.casa_ratio_pct < self.min_casa:
            reasons.append(f"CASA ratio {metrics.casa_ratio_pct:.1f}% below {self.min_casa:.1f}% franchise baseline")
            score -= 10.0

        score = max(0.0, min(100.0, score))
        passed = (
            metrics.cd_ratio_pct < self.max_cd_ratio and
            metrics.gross_npa_pct <= self.max_gnpa and
            metrics.net_npa_pct <= self.max_nnpa and
            metrics.provision_coverage_pct >= self.min_pcr and
            metrics.crar_pct >= self.min_crar and
            metrics.nim_pct >= self.min_nim and
            score >= 65.0
        )
        return passed, score, reasons
