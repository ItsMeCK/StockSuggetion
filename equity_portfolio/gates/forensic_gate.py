"""
Forensic & Governance Gate
Requirements EQ12, EQ43
"""

from dataclasses import dataclass
from typing import List, Tuple, Optional, Set

@dataclass
class ForensicReport:
    symbol: str
    promoter_pledge_pct: float            # Percentage of promoter shares pledged (Cap: < 20%)
    promoter_pledge_qoq_change: float     # QoQ change in pledge % (Cap: <= 5.0%)
    beneish_m_score: Optional[float]      # For non-financials: Cap < -1.78 (more negative is cleaner)
    sloan_accrual_ratio: Optional[float]  # (Net Income - CFO) / Total Assets: Cap < 0.10
    is_asm_gsm_surveillance: bool         # Exchange surveillance (ASM/GSM/ESM/CIRP)
    auditor_resignation_flag: bool        # Mid-term auditor resignation in past 4 quarters
    adverse_caro_qualification: bool      # Severe CARO 2020 qualifications / restatements
    provenance: str = "RESEARCH_FIXTURE"  # "RESEARCH_FIXTURE", "AUDITED_XBRL_FILING", "SECTOR_CONSENSUS_ESTIMATE"

class ForensicGatekeeper:
    """
    Step 1 Gate: Deterministic screening for accounting fraud, governance red flags,
    promoter leverage, exchange surveillance, and provenance integrity.
    Rejection is absolute (Fail-Closed EQ2).
    """
    def __init__(
        self,
        max_pledge_pct: float = 20.0,
        max_pledge_qoq_spike: float = 5.0,
        beneish_cutoff: float = -1.78,
        sloan_accrual_cutoff: float = 0.10,
        known_surveillance_set: Optional[Set[str]] = None,
        require_verified_filings: bool = True
    ):
        self.max_pledge_pct = max_pledge_pct
        self.max_pledge_qoq_spike = max_pledge_qoq_spike
        self.beneish_cutoff = beneish_cutoff
        self.sloan_accrual_cutoff = sloan_accrual_cutoff
        self.known_surveillance_set = known_surveillance_set or set()
        self.require_verified_filings = require_verified_filings

    def evaluate(self, report: Optional[ForensicReport]) -> Tuple[bool, List[str]]:
        if report is None:
            return False, ["No forensic report available - strict Fail-Closed rejection (EQ2)"]

        violations = []

        # 0. Provenance Integrity Check: Reject unverified sector consensus estimates (EQ2)
        if self.require_verified_filings and report.provenance == "SECTOR_CONSENSUS_ESTIMATE":
            violations.append(f"Symbol {report.symbol} has unverified generic sector consensus estimates, not verified filings (Fail-Closed on Unverified Data EQ2)")

        # 1. Exchange Surveillance (ASM, GSM, ESM, CIRP insolvency)
        if report.is_asm_gsm_surveillance or report.symbol in self.known_surveillance_set:
            violations.append(f"Symbol {report.symbol} is under exchange surveillance (ASM/GSM/ESM/CIRP)")

        # 2. Promoter Pledging Check
        if report.promoter_pledge_pct >= self.max_pledge_pct:
            violations.append(f"Promoter pledge {report.promoter_pledge_pct:.1f}% exceeds strict safety ceiling {self.max_pledge_pct:.1f}%")

        if report.promoter_pledge_qoq_change > self.max_pledge_qoq_spike:
            violations.append(f"Promoter pledge QoQ spike of +{report.promoter_pledge_qoq_change:.1f}% exceeds risk threshold {self.max_pledge_qoq_spike:.1f}%")

        # 3. Governance: Auditor Resignation & CARO Qualifications
        if report.auditor_resignation_flag:
            violations.append("Mid-term statutory auditor resignation detected in past 4 quarters")

        if report.adverse_caro_qualification:
            violations.append("Adverse CARO 2020 qualification or restatement of accounts")

        # 4. Beneish M-Score (Non-financials)
        # Scores > -1.78 indicate high probability of earnings manipulation
        if report.beneish_m_score is not None:
            if report.beneish_m_score >= self.beneish_cutoff:
                violations.append(f"Beneish M-Score {report.beneish_m_score:.2f} >= {self.beneish_cutoff:.2f} (manipulation probability)")

        # 5. Sloan Accruals Test: (Net Income - CFO) / Total Assets
        if report.sloan_accrual_ratio is not None:
            if report.sloan_accrual_ratio >= self.sloan_accrual_cutoff:
                violations.append(f"Sloan accrual ratio {report.sloan_accrual_ratio:.3f} >= {self.sloan_accrual_cutoff:.3f} (low earnings quality)")

        passed = len(violations) == 0
        return passed, violations
