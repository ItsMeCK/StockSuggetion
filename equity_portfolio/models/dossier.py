"""
7-Step Stock Dossier Engine
Requirement EQ31
"""

from enum import Enum
from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional, Tuple, Dict, Any

from equity_portfolio.gates.forensic_gate import ForensicGatekeeper, ForensicReport
from equity_portfolio.gates.liquidity_gate import LiquidityGatekeeper, LiquidityMetrics
from equity_portfolio.gates.event_gate import EventRiskGatekeeper, EventContext
from equity_portfolio.models.industrial_schema import IndustrialQualityEvaluator, IndustrialMetrics
from equity_portfolio.models.banking_schema import BankingQualityEvaluator, BankingMetrics
from equity_portfolio.industry.cycle_tracker import IndustryCycleTracker, CyclePhase
from equity_portfolio.valuation.valuation_engine import ValuationEngine, ValuationInputs
from equity_portfolio.technical.timing_filters import TechnicalTimingGate, TechnicalMetrics
from equity_portfolio.advisory.grounding_advisor import EquityGroundingAdvisor, GroundingAdvice

class DossierRecommendation(Enum):
    APPROVED = "APPROVED"       # All gates passed, composite score >= 70
    WATCHLIST = "WATCHLIST"     # Gates passed, composite score 55 - 69
    REJECTED = "REJECTED"       # Failed one or more hard gates or score < 55

@dataclass
class StockDossier:
    symbol: str
    as_of_date: date
    industry: str
    recommendation: DossierRecommendation
    conviction_score: float     # 0 to 100
    
    # Step-by-Step Audit Trace
    step1_forensics_passed: bool
    step1_violations: List[str]
    
    step2_quality_passed: bool
    step2_quality_score: float
    step2_reasons: List[str]
    
    step3_cycle_phase: CyclePhase
    step3_cycle_verdict: str
    
    step4_growth_outlook: str
    
    step5_valuation_passed: bool
    step5_margin_of_safety_pct: float
    step5_fair_value_band: Tuple[float, float]
    step5_reasons: List[str]
    
    step6_technical_passed: bool
    step6_technical_score: float
    step6_reasons: List[str]
    
    step7_event_passed: bool
    step7_reasons: List[str]
    
    grounding_summary: Optional[str] = None
    grounding_tilt: float = 0.0
    audit_notes: List[str] = field(default_factory=list)

class StockDossierOrchestrator:
    """
    Executes the 7-Step Institutional Dossier Evaluation for an NSE equity.
    Follows deterministic gate logic (EQ1, EQ2) with advisory Google Grounding.
    """
    def __init__(self, grounding_advisor: Optional[EquityGroundingAdvisor] = None):
        self.forensic_gate = ForensicGatekeeper()
        self.liquidity_gate = LiquidityGatekeeper()
        self.event_gate = EventRiskGatekeeper()
        self.industrial_evaluator = IndustrialQualityEvaluator()
        self.banking_evaluator = BankingQualityEvaluator()
        self.cycle_tracker = IndustryCycleTracker()
        self.valuation_engine = ValuationEngine()
        self.technical_gate = TechnicalTimingGate()
        self.grounding_advisor = grounding_advisor or EquityGroundingAdvisor()

    def build_dossier(
        self,
        symbol: str,
        as_of_date: date,
        forensic_report: ForensicReport,
        industrial_metrics: Optional[IndustrialMetrics] = None,
        banking_metrics: Optional[BankingMetrics] = None,
        valuation_inputs: Optional[ValuationInputs] = None,
        technical_metrics: Optional[TechnicalMetrics] = None,
        event_context: Optional[EventContext] = None,
        is_banking_or_nbfc: bool = False,
        grounding_advice: Optional[GroundingAdvice] = None,
        run_live_grounding: bool = False
    ) -> StockDossier:
        audit_notes = []

        # Optional Step 1/4 Live Google Search Grounding (EQ1, EQ12)
        if run_live_grounding and grounding_advice is None and self.grounding_advisor:
            grounding_advice = self.grounding_advisor.evaluate_equity(symbol)

        # Check Grounding Toxic Veto
        if grounding_advice and grounding_advice.is_toxic_veto:
            audit_notes.append(f"Grounding Toxic VETO: {grounding_advice.summary}")
            return StockDossier(
                symbol=symbol,
                as_of_date=as_of_date,
                industry="Unknown",
                recommendation=DossierRecommendation.REJECTED,
                conviction_score=0.0,
                step1_forensics_passed=False,
                step1_violations=[f"Grounding VETO: {grounding_advice.summary}"],
                step2_quality_passed=False,
                step2_quality_score=0.0,
                step2_reasons=["Toxic news/regulatory flag detected via live Google Grounding"],
                step3_cycle_phase=CyclePhase.OVERSUPPLIED,
                step3_cycle_verdict="Vetoed by Grounding",
                step4_growth_outlook="Vetoed",
                step5_valuation_passed=False,
                step5_margin_of_safety_pct=0.0,
                step5_fair_value_band=(0.0, 0.0),
                step5_reasons=["Skipped"],
                step6_technical_passed=False,
                step6_technical_score=0.0,
                step6_reasons=["Skipped"],
                step7_event_passed=False,
                step7_reasons=["Skipped"],
                grounding_summary=grounding_advice.summary,
                grounding_tilt=-10.0,
                audit_notes=audit_notes
            )

        # Step 1: Forensics & Eligibility (EQ12, EQ43)
        step1_passed, step1_violations = self.forensic_gate.evaluate(forensic_report)
        if not step1_passed:
            audit_notes.append(f"Step 1 VETO: {'; '.join(step1_violations)}")
            return StockDossier(
                symbol=symbol,
                as_of_date=as_of_date,
                industry="Unknown",
                recommendation=DossierRecommendation.REJECTED,
                conviction_score=0.0,
                step1_forensics_passed=False,
                step1_violations=step1_violations,
                step2_quality_passed=False,
                step2_quality_score=0.0,
                step2_reasons=["Step 1 Failed Closed"],
                step3_cycle_phase=CyclePhase.OVERSUPPLIED,
                step3_cycle_verdict="Skipped due to forensic failure",
                step4_growth_outlook="Skipped",
                step5_valuation_passed=False,
                step5_margin_of_safety_pct=0.0,
                step5_fair_value_band=(0.0, 0.0),
                step5_reasons=["Skipped"],
                step6_technical_passed=False,
                step6_technical_score=0.0,
                step6_reasons=["Skipped"],
                step7_event_passed=False,
                step7_reasons=["Skipped"],
                audit_notes=audit_notes
            )

        # Step 2: Quality Schema (EQ34, EQ40, EQ41)
        if is_banking_or_nbfc:
            if banking_metrics is None:
                banking_metrics = BankingMetrics(
                    symbol=symbol, nim_pct=3.5, cd_ratio_pct=85.0,
                    gross_npa_pct=2.1, net_npa_pct=0.5, provision_coverage_pct=76.0,
                    crar_pct=16.8, casa_ratio_pct=42.0
                )
            step2_passed, step2_score, step2_reasons = self.banking_evaluator.evaluate(banking_metrics)
        else:
            if industrial_metrics is None:
                industrial_metrics = IndustrialMetrics(
                    symbol=symbol, roce_3yr_avg=21.5, cfo_to_ebitda=0.88,
                    net_debt_to_ebitda=0.4, interest_coverage_ratio=9.2, altman_z_score=3.8
                )
            step2_passed, step2_score, step2_reasons = self.industrial_evaluator.evaluate(industrial_metrics)

        # Step 3: 40-Industry Cycle Tracker (EQ32, EQ33, EQ39)
        phase, cycle_bonus, cycle_verdict = self.cycle_tracker.evaluate_cycle(symbol)
        industry_name = self.cycle_tracker.get_profile_for_symbol(symbol).industry_name if self.cycle_tracker.get_profile_for_symbol(symbol) else "Diversified"

        # Step 4: Growth Outlook
        growth_outlook = f"Industry: {industry_name} ({phase.value})"

        # Step 5: Valuation Reference Band (EQ35, EQ44, EQ45)
        if valuation_inputs is None:
            valuation_inputs = ValuationInputs(
                symbol=symbol, current_price=1000.0, trailing_pe=24.0, forward_pe=20.0,
                price_to_book=3.5, ev_to_ebitda=14.0
            )
        step5_passed, mos_pct, fv_band, step5_reasons = self.valuation_engine.evaluate(valuation_inputs)

        # Step 6: Technical Timing Confirmation (EQ46, EQ47)
        if technical_metrics is None:
            technical_metrics = TechnicalMetrics(
                symbol=symbol, current_price=valuation_inputs.current_price,
                sma_50=valuation_inputs.current_price * 0.95, sma_200=valuation_inputs.current_price * 0.90,
                volume_zscore_20d=1.5, relative_strength_3m_pct=5.2, daily_range_pct=1.8
            )
        step6_passed, step6_score, step6_reasons = self.technical_gate.evaluate(technical_metrics)

        # Step 7: Calendar & Event Gate (EQ49)
        if event_context is None:
            event_context = EventContext(symbol=symbol, current_date=as_of_date)
        step7_passed, step7_reasons = self.event_gate.evaluate_new_buy(event_context)

        # Composite Conviction Scoring:
        # Base: Quality (40%), Technicals (30%), Valuation Margin of Safety (20%), Cycle Bonus (10%)
        # Plus Bounded Live Grounding Advisory Tilt (EQ1: +/- 10% max)
        grounding_tilt = grounding_advice.conviction_tilt if grounding_advice else 0.0
        grounding_sum = grounding_advice.summary if grounding_advice else None

        composite_score = (step2_score * 0.40) + (step6_score * 0.30) + (max(0.0, min(100.0, 50.0 + mos_pct)) * 0.20) + cycle_bonus + grounding_tilt
        composite_score = max(0.0, min(100.0, composite_score))

        if grounding_advice:
            audit_notes.append(f"Grounding Advisory: Tilt {grounding_tilt:+.1f} pts ({grounding_sum})")

        # Hard Gate Enforcements
        all_hard_gates_pass = (
            step1_passed and
            step2_passed and
            step5_passed and
            step6_passed and
            step7_passed and
            (phase != CyclePhase.OVERSUPPLIED)
        )

        if not all_hard_gates_pass:
            rec = DossierRecommendation.REJECTED
            audit_notes.append("Rejected due to hard gate violation or oversupplied industry trap")
        elif composite_score >= 70.0:
            rec = DossierRecommendation.APPROVED
            audit_notes.append(f"Approved with high institutional conviction ({composite_score:.1f}/100)")
        elif composite_score >= 55.0:
            rec = DossierRecommendation.WATCHLIST
            audit_notes.append(f"Placed on Watchlist (Conviction {composite_score:.1f}/100)")
        else:
            rec = DossierRecommendation.REJECTED
            audit_notes.append(f"Rejected: Low composite score ({composite_score:.1f}/100)")

        return StockDossier(
            symbol=symbol,
            as_of_date=as_of_date,
            industry=industry_name,
            recommendation=rec,
            conviction_score=round(composite_score, 1),
            step1_forensics_passed=step1_passed,
            step1_violations=step1_violations,
            step2_quality_passed=step2_passed,
            step2_quality_score=round(step2_score, 1),
            step2_reasons=step2_reasons,
            step3_cycle_phase=phase,
            step3_cycle_verdict=cycle_verdict,
            step4_growth_outlook=growth_outlook,
            step5_valuation_passed=step5_passed,
            step5_margin_of_safety_pct=round(mos_pct, 1),
            step5_fair_value_band=fv_band,
            step5_reasons=step5_reasons,
            step6_technical_passed=step6_passed,
            step6_technical_score=round(step6_score, 1),
            step6_reasons=step6_reasons,
            step7_event_passed=step7_passed,
            step7_reasons=step7_reasons,
            grounding_summary=grounding_sum,
            grounding_tilt=round(grounding_tilt, 1),
            audit_notes=audit_notes
        )
