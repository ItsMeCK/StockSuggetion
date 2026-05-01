import logging
import json
from typing import Dict, Any, List

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class PostMortemReflectionEngine:
    """
    The Alpha Loop.
    Mines hit stop-losses from the Zerodha order book, constructs a high-dimensional 
    diagnostic context, and inserts it into the pgvector Experience DB to train the Meta-Gate.
    """
    def __init__(self):
        # In production: connect to KiteConnect and the pgvector database
        pass

    def check_hit_stops(self) -> List[Dict[str, Any]]:
        import os, psycopg2
        logging.info("Scanning Ledger for hit GTT Stop-Losses...")
        hit_stops = []
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                user=os.getenv('POSTGRES_USER', 'quant'),
                password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
                database=os.getenv('POSTGRES_DB', 'market_data')
            )
            cur = conn.cursor()
            query = "SELECT ticker, price, market_time, notes FROM trade_events WHERE status = 'STOP_HIT'"
            cur.execute(query)
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            for row in rows:
                hit_stops.append({
                    "symbol": row[0],
                    "pattern": "system_flagged",
                    "loss_pct": 0.0,
                    "hit_time": str(row[2]),
                    "notes": row[3]
                })
        except Exception as e:
            logging.error(f"Reflection Engine ledger query error: {e}")
            
        return hit_stops

    def build_autopsy_vector(self, failed_trade: Dict[str, Any], current_regime: str, vix: float) -> str:
        symbol = failed_trade["symbol"]
        pattern = failed_trade["pattern"]
        
        autopsy_text = (
            f"Trade Failure: {symbol}. Pattern: {pattern}. "
            f"Macro Regime at Entry: {current_regime}. "
            f"VIX at Failure: {vix:.2f}. "
            f"Likely Cause: Stop loss breached during broader market volatility."
        )
        return autopsy_text

    def embed_to_pgvector(self, autopsy_text: str, symbol: str, pattern: str, regime: str):
        import os, psycopg2
        logging.info(f"Inserting Autopsy into pgvector Experience DB for {symbol}...")
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=5433,
                user='agent',
                password='agentpassword',
                database='sovereign_state'
            )
            cur = conn.cursor()
            query = """
                INSERT INTO experience_memory (ticker, pattern, macro_regime, notes)
                VALUES (%s, %s, %s, %s)
            """
            cur.execute(query, (symbol, pattern, regime, autopsy_text))
            conn.commit()
            cur.close()
            conn.close()
            logging.info(f"Successfully logged failure memory cluster for {symbol}.")
        except Exception as e:
            logging.error(f"Failed to persist experience memory: {e}")

def run_reflection_engine(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Reflection Engine.
    Executes post-market.
    """
    logging.info("Initializing Post-Mortem Reflection Engine...")
    
    regime = state.get("macro_regime", "NEUTRAL")
    vix = state.get("india_vix", 15.0)
    
    reflection_module = PostMortemReflectionEngine()
    hit_stops = reflection_module.check_hit_stops()
    
    for trade in hit_stops:
        autopsy = reflection_module.build_autopsy_vector(trade, regime, vix)
        reflection_module.embed_to_pgvector(autopsy, trade["symbol"], trade["pattern"], regime)
        
    return {} # No direct state delta needed; mutates external pgvector state

if __name__ == "__main__":
    mock_state = SovereignState(macro_regime="TUG_OF_WAR")
    run_reflection_engine(mock_state)
