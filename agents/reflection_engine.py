import logging
import json
import os
import psycopg2
from typing import Dict, Any, List
from core.state import SovereignState

class PostMortemReflectionEngine:
    """
    The 'Truth Seeker'. 
    Categorizes failures into Technical Traps, News Shocks, or Macro Flushes.
    """
    def __init__(self):
        self.db_params = {
            "host": os.getenv('DB_HOST', 'localhost'),
            "port": os.getenv('TIMESCALE_PORT', '5432'),
            "user": os.getenv('TIMESCALE_USER', 'quant'),
            "password": os.getenv('TIMESCALE_PASSWORD', 'quantpassword'),
            "database": os.getenv('TIMESCALE_DB', 'market_data')
        }

    def _get_connection(self):
        return psycopg2.connect(**self.db_params)

    def perform_attribution(self, symbol: str, date: str, macro_regime: str) -> str:
        """Determines the ROOT CAUSE of a trade failure."""
        logging.info(f"PERFORMING ATTRIBUTION AUDIT FOR {symbol} on {date}...")
        
        # 1. Check for Macro Flush (Nifty Performance on that day)
        # Placeholder: In production, check daily_ohlcv for 'NIFTY' index
        if macro_regime == "BEARISH":
            return "REGIME_FLUSH"

        # 2. Check for News Shock (Fundamental Audit)
        # We check the fundamental_reports stored in the state (simulated here)
        # If news contains 'fraud', 'resignation', 'fine', it is a NEWS_SHOCK
        
        # 3. Default to Technical Trap
        # If the market was okay and there was no news, it was our pattern that failed.
        return "SYSTEM_TRAP"

    def embed_experience(self, symbol: str, date: str, attribution: str, pattern: str):
        """Saves the failure to the experience database with the correct tag."""
        try:
            conn = self._get_connection()
            cur = conn.cursor()
            
            # Save to pattern_embeddings with the specific outcome/attribution
            # Fetch 60 day embedding for technical traps
            if attribution == "SYSTEM_TRAP":
                query = "SELECT close FROM daily_ohlcv WHERE symbol = %s AND time <= %s ORDER BY time DESC LIMIT 60"
                cur.execute(query, (symbol, date))
                prices = [float(r[0]) for r in cur.fetchall()[::-1]]
                if len(prices) >= 60:
                    base = prices[0]
                    emb = [(p - base) / base for p in prices]
                    
                    cur.execute(
                        "INSERT INTO pattern_embeddings (symbol, time, pattern_type, outcome, embedding) VALUES (%s, %s, %s, %s, %s)",
                        (symbol, date, pattern, "LOSS", emb)
                    )
            
            # Log the detailed experience
            cur.execute(
                "INSERT INTO experience_memory (ticker, pattern, macro_regime, notes) VALUES (%s, %s, %s, %s)",
                (symbol, pattern, attribution, f"Failed on {date}. Attribution: {attribution}")
            )
            
            conn.commit()
            cur.close()
            conn.close()
            logging.info(f"SUCCESS: Logged {attribution} memory for {symbol}.")
        except Exception as e:
            logging.error(f"REFLECTION ERROR: {e}")

def run_reflection_engine(state: SovereignState) -> Dict[str, Any]:
    logging.info("INITIATING SOVEREIGN ATTRIBUTION AUDIT...")
    
    engine = PostMortemReflectionEngine()
    target_date = state.get("target_date")
    macro_regime = state.get("macro_regime", "NEUTRAL")
    
    # We audit the trades that hit stop-losses (simulated from approved_allocations that failed)
    # In a real run, this would query the live Zerodha Ledger
    approved = state.get("approved_allocations", {})
    
    for symbol in approved:
        # For the historical audit, we determine the outcome manually
        from scratch.learning_audit import get_outcome
        outcome = get_outcome(symbol, target_date)
        
        if outcome == "LOSS":
            attribution = engine.perform_attribution(symbol, target_date, macro_regime)
            pattern = state.get("heuristic_flags", {}).get(symbol, {}).get("identified_pattern", "unknown")
            engine.embed_experience(symbol, target_date, attribution, pattern)
            
    return {}
