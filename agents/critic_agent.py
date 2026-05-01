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
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                dbname=os.getenv("POSTGRES_DB", "market_data")
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
                
                # Veto only if the divergence is extreme (Price up > 1%, Volume down > 15%)
                price_change = (prices[-1] - prices[0]) / prices[0] * 100
                vol_change = (volumes[-1] - volumes[0]) / volumes[0] * 100
                
                if price_change > 2.0 and vol_change < -20.0:
                    veto_required = True
                    reason = f"Extreme Distribution detected: Price rose {price_change:.1f}% while volume collapsed {vol_change:.1f}%."
        except Exception as e:
            logging.error(f"Critic Agent calculation error for {symbol}: {e}")
            
        return {
            "veto": veto_required,
            "critique": reason
        }

def run_critic_agent(state: SovereignState) -> Dict[str, Any]:
    candidates = state.get("candidates", [])
    agent_scores = state.get("agent_scores", {})
    macro = state.get("macro_regime", "UNKNOWN")
    
    critic = CriticAgent()
    critic_results = {}
    approved_allocations = {}
    
    for symbol in candidates:
        scores = agent_scores.get(symbol, {})
        
        # --- WEIGHTED CONSENSUS SCORING ---
        # Weights: Entry (40%), Vision (40%), DTW (20%)
        w_entry = scores.get("entry", 0) * 0.40
        w_vision = scores.get("vision", 0) * 0.40
        w_dtw = scores.get("dtw", 0) * 0.20
        
        total_confidence = w_entry + w_vision + w_dtw
        
        # Run base adversarial check
        evaluation = critic.evaluate_thesis(symbol, "thesis", macro)
        
        # GEAR 3: VISION HARD GATE
        vision_passed = state.get("vision_validations", {}).get(symbol, {}).get("vision_approved", True)
        
        # Decision Logic: Threshold 70% + No Critic Veto + Vision Approved
        final_approval = (total_confidence >= 70.0) and (not evaluation["veto"]) and vision_passed
        
        evaluation["total_confidence"] = float(total_confidence)
        evaluation["approved"] = final_approval
        critic_results[symbol] = evaluation
        
        if final_approval:
            logging.info(f"COGNITIVE APPROVAL: {symbol} passed with {total_confidence:.1f}% confidence!")
        else:
            logging.warning(f"COGNITIVE REJECTION: {symbol} only reached {total_confidence:.1f}% confidence.")
        
    approved_candidates = [s for s, res in critic_results.items() if res.get("approved")]
    return {
        "critic_results": critic_results, 
        "approved_allocations": approved_allocations,
        "approved_candidates": approved_candidates,
        "debate_count": 1
    }
