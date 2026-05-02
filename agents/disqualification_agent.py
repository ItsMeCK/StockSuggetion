import logging
from typing import Dict, Any, List
from core.state import SovereignState
from core.vector_store import SovereignVectorStore

class DisqualificationAgent:
    """
    The 'Pessimist' auditor. 
    Specializes in identifying Bull Traps, False Breakouts, and 'Similar Past Failures'.
    """
    def __init__(self):
        self.vector_store = SovereignVectorStore()

    def audit_candidate(self, symbol: str, pattern: str, price_data: List[float]) -> Dict[str, Any]:
        # Normalize price data to create a simple embedding
        if not price_data:
            return {"veto": True, "reason": "No price data for audit"}
            
        base_price = price_data[0]
        embedding = [(p - base_price) / base_price for p in price_data]
        if len(embedding) < 60:
            embedding = [0.0] * (60 - len(embedding)) + embedding
        else:
            embedding = embedding[:60]

        # 1. VECTOR MEMORY CHECK (The 'Trap Mirror')
        # SKIP SEARCH DURING HISTORICAL MEGA-BUILD TO PREVENT DB CRASHES
        import os
        if os.getenv('IS_MEGA_BUILD') == 'true':
            logging.info("SKIPPING VECTOR SEARCH DURING MEGA-BUILD...")
            similar_traps = []
        else:
            similar_traps = self.vector_store.find_similar_traps(embedding, threshold=0.85)
        
        if similar_traps:
            trap = similar_traps[0]
            return {
                "veto": True, 
                "reason": f"VECTOR_TRAP: Pattern is {trap['similarity']*100:.1f}% similar to historical failure in {trap['symbol']} on {trap['time']}."
            }

        # 2. HEURISTIC DISQUALIFICATION (Master Rules v2.3)
        # Placeholder for specific logic like 'Immediate rejection below breakout line'
        
        return {"veto": False, "reason": "None"}

def run_disqualification_agent(state: SovereignState) -> Dict[str, Any]:
    candidates = state.get("candidates", [])
    disqualifications = {}
    agent = DisqualificationAgent()
    
    import os, psycopg2
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('TIMESCALE_PORT', '5432'),
        user=os.getenv('TIMESCALE_USER', 'quant'),
        password=os.getenv('TIMESCALE_PASSWORD', 'quantpassword'),
        database=os.getenv('TIMESCALE_DB', 'market_data')
    )
    cur = conn.cursor()
    
    target_date = state.get("target_date")
    
    for symbol in candidates:
        # Fetch 60 days of data for the embedding
        query = "SELECT close FROM daily_ohlcv WHERE symbol = %s AND time <= %s ORDER BY time DESC LIMIT 60"
        cur.execute(query, (symbol, target_date if target_date else '2099-01-01'))
        rows = cur.fetchall()
        
        if rows:
            prices = [float(r[0]) for r in rows[::-1]] # Oldest to newest
            pattern = state.get("heuristic_flags", {}).get(symbol, {}).get("identified_pattern", "unknown")
            res = agent.audit_candidate(symbol, pattern, prices)
            disqualifications[symbol] = res
            
            if res["veto"]:
                logging.warning(f"SOVEREIGN VETO: {symbol} rejected by Disqualification Agent: {res['reason']}")
    
    cur.close()
    conn.close()
    
    return {"disqualifications": disqualifications}
