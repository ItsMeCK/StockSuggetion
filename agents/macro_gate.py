import logging
from typing import Dict, Any

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

import os
import psycopg2
from typing import Dict, Any

class MacroDataFetcher:
    """
    Fetches real macro data from TimescaleDB.
    """
    @staticmethod
    def fetch_fii_dii_flow() -> Dict[str, float]:
        # Currently, FII/DII data is not ingested in the current pipeline.
        # We return a 'Neutral' flow to prevent fallback-induced rejections.
        # TODO: Add NSE FII/DII ingestion to pipeline.ingestion.py
        logging.warning("FII/DII data not found in DB. Falling back to NEUTRAL flow (0.0).")
        return {"fii_net": 0.0, "dii_net": 0.0}

    @staticmethod
    def fetch_market_health(target_date: str = None) -> Dict[str, Any]:
        """
        Fetches Nifty 50 momentum and overall market breadth (Advancing vs Declining).
        """
        nifty_return = 0.0
        advancing = 0
        declining = 0
        
        try:
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                dbname=os.getenv("POSTGRES_DB", "market_data")
            )
            cur = conn.cursor()
            
            # 1. Nifty Momentum
            nifty_query = """
                WITH prev AS (
                    SELECT close FROM daily_ohlcv WHERE symbol = 'NIFTY 50' AND time < %s ORDER BY time DESC LIMIT 1
                )
                SELECT (close - (SELECT close FROM prev)) / (SELECT close FROM prev) * 100 
                FROM daily_ohlcv WHERE symbol = 'NIFTY 50' AND time::date = %s
            """
            cur.execute(nifty_query, (target_date, target_date))
            row = cur.fetchone()
            nifty_return = float(row[0]) if row else 0.0
            
            # 2. Market Breadth (NSE 500 equivalent)
            breadth_query = """
                WITH daily_returns AS (
                    SELECT symbol, 
                           (close - LAG(close) OVER (PARTITION BY symbol ORDER BY time)) / LAG(close) OVER (PARTITION BY symbol ORDER BY time) as ret
                    FROM daily_ohlcv 
                    WHERE time <= %s
                )
                SELECT 
                    COUNT(*) FILTER (WHERE ret > 0) as adv,
                    COUNT(*) FILTER (WHERE ret < 0) as dec
                FROM daily_returns
                WHERE symbol IN (SELECT DISTINCT symbol FROM daily_ohlcv)
            """
            # This is a bit slow for every day, so we optimize to only the specific target day
            # Simplified for speed:
            breadth_query_fast = """
                SELECT 
                    COUNT(*) FILTER (WHERE close > open) as adv,
                    COUNT(*) FILTER (WHERE close < open) as dec
                FROM daily_ohlcv 
                WHERE time::date = %s
            """
            cur.execute(breadth_query_fast, (target_date,))
            b_row = cur.fetchone()
            if b_row:
                advancing, declining = b_row
            
            cur.close()
            conn.close()
        except Exception as e:
            logging.error(f"Macro Data fetch error: {e}")
            
        return {
            "nifty_return": nifty_return,
            "advancing": advancing,
            "declining": declining,
            "adr": advancing / max(1, declining)
        }

def evaluate_market_regime(fii_dii: Dict[str, float], health: Dict[str, Any]) -> str:
    """
    Enhanced Regime Evaluation with Nifty Momentum and Breadth.
    """
    nifty_ret = health.get("nifty_return", 0.0)
    adr = health.get("adr", 1.0)
    
    # --- RULE 1: THE NIFTY KILL-SWITCH ---
    if nifty_ret < -0.8:
        return "CAPITULATION" # Force halt on deep red days
        
    # --- RULE 2: BREADTH CHECK (STRICT ALPHA) ---
    if adr < 0.9: # High-conviction breadth only
        return "TUG_OF_WAR"
        
    return "EXPANSION"

def run_macro_regime_gate(state: SovereignState) -> SovereignState:
    """
    The LangGraph Node implementation for the Macro Regime Gate.
    This acts as the ultimate 'Kill-Switch'.
    """
    logging.info("Executing Macro Regime Gate...")
    
    # Get target date from state (for historical simulation support)
    target_date = state.get("target_date")
    if not target_date:
        # Fallback to current date for live runs
        from datetime import datetime
        target_date = datetime.now().strftime('%Y-%m-%d')

    # 1. Fetch Market Health
    market_health = MacroDataFetcher.fetch_market_health(target_date)
    logging.info(f"Nifty Return: {market_health['nifty_return']:.2f}% | ADR: {market_health['adr']:.2f}")
    
    # 2. Evaluate Regime
    regime = evaluate_market_regime({}, market_health)
    logging.info(f"Identified Market Regime: {regime}")
    
    # 3. Return Delta State Update
    if regime in ["CAPITULATION", "TUG_OF_WAR"]:
        logging.warning(f"🚨 MARKET KILL-SWITCH TRIGGERED ({target_date}). Regime: {regime}. Nifty Return: {market_health['nifty_return']:.2f}%. Halting Longs.")
        return {
            "macro_regime": regime, 
            "candidates": [], 
            "error_log": [f"Halted due to {regime} on {target_date}"]
        }
    
    return {
        "macro_regime": regime,
        "nifty_momentum": market_health['nifty_return'],
        "market_breadth": market_health['adr']
    }

if __name__ == "__main__":
    # Test the standalone node execution
    mock_state = SovereignState(
        macro_regime="", 
        candidates=[], 
        heuristic_flags={}, 
        experience_warnings={}, 
        vision_validations={}, 
        approved_allocations={}, 
        execution_telemetry={}, 
        error_log=[]
    )
    result = run_macro_regime_gate(mock_state)
    print(f"Resulting Delta State: {result}")
