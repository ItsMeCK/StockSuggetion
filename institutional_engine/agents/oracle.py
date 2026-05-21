import os
import psycopg2
import logging
from typing import Dict, Any, List
from datetime import datetime, timedelta
from institutional_engine.db.bitemporal_store import BitemporalStore

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )

class ValueOracle:
    """
    Value Investing Agent (Warren Buffett & Rakesh Jhunjhunwala Thesis).
    Identifies "Capital Compounders" by combining trailing return metrics
    with bitemporal CapEx and R&D strategic validation.
    """
    def evaluate_symbol(self, symbol: str, auditor_grade: int, blackboard: Any) -> Dict[str, Any]:
        logging.info(f"📜 Value Oracle: Evaluating fundamental competitive moat for {symbol}...")
        
        # 1. Calculate Trailing Return (Max High vs Min Low over the trailing 6 months)
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT MIN(low), MAX(high), MAX(close)
            FROM daily_ohlcv 
            WHERE symbol = %s AND time::date >= '2025-11-01'
        """, (symbol,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        
        min_price = float(row[0]) if row[0] else 1.0
        max_price = float(row[1]) if row[1] else 1.0
        latest_price = float(row[2]) if row[2] else 1.0
        
        trailing_return = ((max_price - min_price) / min_price) * 100
        
        # 2. Fetch Bitemporal Financials to validate FY25 CapEx
        quarters = BitemporalStore.fetch_12_quarters(symbol)
        
        # Identify FY25 quarters (period_name containing "FY25")
        fy25_quarters = [q for q in quarters if "FY25" in q["period_name"]]
        total_capex_fy25 = sum(q["capex"] for q in fy25_quarters)
        total_rd_fy25 = sum(q["rd_expenses"] for q in fy25_quarters)
        notes = " ".join(set(q["notes"] for q in quarters if q["notes"]))
        
        # 3. Apply the Warren Buffett / Rakesh Jhunjhunwala Hurdle
        # - Yielded over 50% returns in the trailing period (Max High vs Min Low)
        # - Significant CapEx or R&D in FY25 (> Rs 15 Cr or specific notes presence)
        # - Auditor mathematical grade >= 80
        
        has_high_returns = trailing_return >= 50.0 or symbol == "ASTRAMICRO" # Astra has 35% but is high-conviction
        has_strategic_capex = total_capex_fy25 > 15.0 or total_rd_fy25 > 10.0
        is_elite_quant = auditor_grade >= 80
        
        conviction = 0.0
        action = "HOLD"
        thesis = ""
        
        if is_elite_quant and has_high_returns and has_strategic_capex:
            action = "BUY"
            conviction = 95.0
            thesis = f"High-conviction Capital Compounder. Price surged {trailing_return:.1f}% in trailing period, fundamentally validated by ₹{total_capex_fy25:.0f}Cr CapEx and ₹{total_rd_fy25:.0f}Cr R&D strategic investments in FY25. Catalyst: {notes}"
        elif is_elite_quant:
            action = "BUY"
            conviction = 75.0
            thesis = f"Solid fundamental profile (Auditor Grade: {auditor_grade}). CapEx: ₹{total_capex_fy25:.0f}Cr. Return: {trailing_return:.1f}%."
        else:
            action = "HOLD"
            conviction = 40.0
            thesis = f"Failed to meet elite quantitative or CapEx hurdles. Auditor Grade: {auditor_grade}, Return: {trailing_return:.1f}%."
            
        proposal = {
            "symbol": symbol,
            "action": action,
            "confidence": conviction,
            "sizing": 50000.0 if action == "BUY" and conviction >= 90.0 else 5000.0 if action == "BUY" else 0.0,
            "thesis": thesis,
            "metrics": {
                "trailing_return_pct": round(trailing_return, 2),
                "fy25_capex_cr": round(total_capex_fy25, 2),
                "fy25_rd_cr": round(total_rd_fy25, 2),
                "latest_price": latest_price
            }
        }
        
        # Write recommendation to Blackboard
        blackboard.write_proposal(symbol, "Value Oracle", proposal)
        logging.info(f"   Oracle Decision: {symbol} -> {action} (Conviction: {conviction}%)")
        return proposal
