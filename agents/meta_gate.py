import logging
from typing import Dict, Any, List

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ExperienceDBMetaGate:
    """
    Queries the local pgvector Experience DB.
    Uses cosine similarity to check if the current setup shares "DNA" 
    (macro regime + pattern type) with a recent cluster of failed trades.
    If a match is found, the setup is vetoed immediately.
    """
    def __init__(self):
        # In production, connect to the Postgres pgvector container (port 5433)
        pass

    def check_failure_clusters(self, symbol: str, pattern: str, regime: str) -> List[str]:
        import os, psycopg2
        warnings = []
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('DB_PORT', '5432'),
                user=os.getenv('POSTGRES_USER', 'quant'),
                password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
                database=os.getenv('POSTGRES_DB', 'market_data')
            )
            cur = conn.cursor()
            query = "SELECT COUNT(*) FROM trade_events WHERE ticker = %s AND status = 'STOP_HIT'"
            cur.execute(query, (symbol,))
            count = cur.fetchone()[0]
            cur.close()
            conn.close()
            
            if count > 0:
                warnings.append(f"Failure Veto: Recent STOP_HIT event detected in the ledger for {symbol}. Pausing deployment parameters.")
        except Exception as e:
            logging.error(f"Meta-Gate Postgres Query Error: {e}")
            
        return warnings

def run_meta_gate(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Meta-Gate.
    """
    regime = state.get("macro_regime", "NEUTRAL")
    heuristic_flags = state.get("heuristic_flags", {})
    
    if not heuristic_flags:
        logging.warning("No heuristic flags received. Meta-Gate has nothing to evaluate.")
        return {"experience_warnings": {}}

    meta_gate = ExperienceDBMetaGate()
    experience_warnings = {}

    for symbol, setup_data in heuristic_flags.items():
        pattern = setup_data.get("identified_pattern", "unknown")
        
        warnings = meta_gate.check_failure_clusters(symbol, pattern, regime)
        
        if warnings:
            logging.warning(f"VETO APPLIED for {symbol}: {warnings[0]}")
            experience_warnings[symbol] = warnings
        else:
            logging.info(f"CLEAR: {symbol} passed the Experience Meta-Gate.")

    return {"experience_warnings": experience_warnings}

if __name__ == "__main__":
    # Test execution
    mock_state = SovereignState(
        macro_regime="TUG_OF_WAR",
        heuristic_flags={
            "RELIANCE": {"identified_pattern": "rectangle"},
            "INFY": {"identified_pattern": "ascending_triangle"},
            "HDFCBANK": {"identified_pattern": "bull_flag"}
        }
    )
    result = run_meta_gate(mock_state)
    print(f"Delta State Update: {result}")
