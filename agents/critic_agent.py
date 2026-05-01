import logging
from typing import Dict, Any
from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CriticAgent:
    """
    Performs strict adversarial evaluations across proposed setups.
    """
    def __init__(self):
        pass
        
    def evaluate_thesis(self, symbol: str, thesis: str, macro_regime: str) -> Dict[str, Any]:
        import os, psycopg2
        logging.info(f"Critic Agent: Evaluating deployment parameters for {symbol} under regime {macro_regime}...")
        
        veto_required = False
        reason = "None"
        
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('TIMESCALE_PORT', '5432'),
                user=os.getenv('TIMESCALE_USER', 'quant'),
                password=os.getenv('TIMESCALE_PASSWORD', 'quantpassword'),
                database=os.getenv('TIMESCALE_DB', 'market_data')
            )
            cur = conn.cursor()
            query = """
                SELECT close, volume 
                FROM daily_ohlcv 
                WHERE symbol = %s 
                ORDER BY time DESC 
                LIMIT 5
            """
            cur.execute(query, (symbol,))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if len(rows) >= 5:
                # Veto if price is rising but volume is falling (bearish divergence)
                prices = [float(r[0]) for r in rows[::-1]] # Oldest to newest
                volumes = [float(r[1]) for r in rows[::-1]]
                
                price_rising = prices[-1] > prices[0]
                volume_falling = volumes[-1] < volumes[0]
                
                if price_rising and volume_falling:
                    veto_required = True
                    reason = f"Hidden Distribution detected: Price rose over 5 days while volume fell ({volumes[0]:.0f} -> {volumes[-1]:.0f})."
        except Exception as e:
            logging.error(f"Critic Agent calculation error for {symbol}: {e}")
            
        return {
            "veto": veto_required,
            "critique": reason
        }

def run_critic_agent(state: SovereignState) -> Dict[str, Any]:
    candidates = state.get("candidates", [])
    heuristic_flags = state.get("heuristic_flags", {})
    macro = state.get("macro_regime", "UNKNOWN")
    
    critic = CriticAgent()
    critic_results = {}
    
    for symbol in candidates:
        thesis = heuristic_flags.get(symbol, {}).get("pattern", "None")
        evaluation = critic.evaluate_thesis(symbol, thesis, macro)
        critic_results[symbol] = evaluation
        
    return {"critic_results": critic_results, "debate_count": 1}
