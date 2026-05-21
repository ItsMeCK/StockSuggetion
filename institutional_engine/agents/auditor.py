import logging
from typing import Dict, Any, List
from institutional_engine.db.bitemporal_store import BitemporalStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class FundamentalAuditor:
    """
    Fundamental Auditor Agent: The "Math Specialist".
    Performs rigorous end-to-end 12-quarter mathematical auditing on P&L, BS, and Cash Flow.
    """
    def audit_symbol(self, symbol: str) -> Dict[str, Any]:
        logging.info(f"📊 Fundamental Auditor: Starting mathematical audit for {symbol}...")
        quarters = BitemporalStore.fetch_12_quarters(symbol)
        
        if len(quarters) < 4:
            logging.warning(f"Insufficient historical quarters ({len(quarters)}) to perform audit.")
            return {"grade": 0, "status": "FAIL_INSUFFICIENT_DATA"}
            
        # 1. EBITDA CAGR Calculation (comparing first FY average to last FY average)
        first_four_ebitda = sum(q["ebitda"] for q in quarters[:4]) / 4
        last_four_ebitda = sum(q["ebitda"] for q in quarters[-4:]) / 4
        
        ebitda_growth_3y = 0.0
        if first_four_ebitda > 0:
            ebitda_growth_3y = ((last_four_ebitda / first_four_ebitda) ** (1/3) - 1) * 100

        # 2. Debt-to-Equity Analysis (using latest quarter)
        latest = quarters[-1]
        debt = latest["debt"]
        equity = latest["equity"]
        latest_de = debt / equity if equity > 0 else 999.0
        
        # Check if D/E is falling or low
        initial_de = quarters[0]["debt"] / quarters[0]["equity"] if quarters[0]["equity"] > 0 else 999.0
        de_status = "STABLE/FALLING" if latest_de <= initial_de else "RISING"

        # 3. ROIC Evaluation
        latest_roic = latest["roic"]

        # 4. Operating Cash Flow Quality Check (Operating Cash Flow / Net Profit)
        # We check the average over the 12 quarters
        avg_ocf = sum(q["operating_cash_flow"] for q in quarters)
        avg_np = sum(q["net_profit"] for q in quarters)
        ocf_quality_ratio = avg_ocf / avg_np if avg_np > 0 else 0.0
        ocf_valid = ocf_quality_ratio >= 1.0

        # 5. CapEx Efficiency (Asset Turnover proxy)
        total_capex = sum(q["capex"] for q in quarters)
        total_revenue = sum(q["revenue"] for q in quarters)
        
        # 6. Scoring Ledger (Max 100 points)
        score = 0
        points_breakdown = {}
        
        # EBITDA hurdle (>20% CAGR gets 30 points, >10% gets 15 points)
        if ebitda_growth_3y >= 20.0:
            score += 30
            points_breakdown["ebitda_cagr"] = 30
        elif ebitda_growth_3y >= 10.0:
            score += 15
            points_breakdown["ebitda_cagr"] = 15
        else:
            points_breakdown["ebitda_cagr"] = 0
            
        # Debt-to-Equity Hurdle (<0.5 gets 25 points, <1.0 gets 10 points)
        if latest_de <= 0.5:
            score += 25
            points_breakdown["debt_to_equity"] = 25
        elif latest_de <= 1.0:
            score += 10
            points_breakdown["debt_to_equity"] = 10
        else:
            points_breakdown["debt_to_equity"] = 0
            
        # ROIC Hurdle (>15% gets 25 points, >10% gets 10 points)
        if latest_roic >= 15.0:
            score += 25
            points_breakdown["roic"] = 25
        elif latest_roic >= 10.0:
            score += 10
            points_breakdown["roic"] = 10
        else:
            points_breakdown["roic"] = 0
            
        # Cash Flow Hurdle (OCF > Net Profit gets 20 points)
        if ocf_valid:
            score += 20
            points_breakdown["cash_flow_quality"] = 20
        else:
            points_breakdown["cash_flow_quality"] = 0

        status = "ELITE" if score >= 80 else "STANDARD" if score >= 50 else "FAILED"
        
        audit_results = {
            "symbol": symbol,
            "grade": score,
            "status": status,
            "metrics": {
                "ebitda_cagr_3y": round(ebitda_growth_3y, 2),
                "latest_de": round(latest_de, 3),
                "de_trend": de_status,
                "roic": round(latest_roic, 2),
                "ocf_to_np_ratio": round(ocf_quality_ratio, 2),
                "total_capex": round(total_capex, 2),
                "total_revenue": round(total_revenue, 2)
            },
            "breakdown": points_breakdown
        }
        
        logging.info(f"   Scorecard Generated: {symbol} scored {score}/100. Status: {status}")
        return audit_results
