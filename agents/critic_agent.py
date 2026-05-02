import logging
from typing import Dict, Any
from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CriticAgent:
    """
    Performs strict adversarial evaluations across proposed setups.
    """
    def __init__(self):
        self.rules = self._load_context_rules()

    def _load_context_rules(self) -> Dict[str, Any]:
        import json
        from pathlib import Path
        core_dir = Path(__file__).parent.parent / "core"
        rules_path = core_dir / "context_rules.json"
        try:
            with open(rules_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
        
    def evaluate_thesis(self, symbol: str, thesis: str, macro_regime: str, target_date: str = None) -> Dict[str, Any]:
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
            
            if target_date:
                query = """
                    SELECT close, volume 
                    FROM daily_ohlcv 
                    WHERE symbol = %s AND time <= %s
                    ORDER BY time DESC 
                    LIMIT 5
                """
                cur.execute(query, (symbol, target_date))
            else:
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
                
                # --- RESTORED: Distribution Veto Re-enabled ---
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
    agent_scores = state.get("agent_scores", {})
    macro = state.get("macro_regime", "UNKNOWN")
    
    critic = CriticAgent()
    critic_results = {}
    approved_allocations = {}
    
    # --- SOVEREIGN RESILIENCE PROTOCOL (Dynamic Alpha) ---
    regime_map = {"BULLISH": "BULL_MARKET", "BEARISH": "BEAR_MARKET", "NEUTRAL": "CHOPPY_SIDEWAYS"}
    active_regime = regime_map.get(macro, "CHOPPY_SIDEWAYS")
    rules = critic.rules.get("pring_pattern_geometries", {})
    
    # --- REGIME-BASED HURDLES (130-Point System) ---
    if macro == "BEARISH":
        COGNITIVE_THRESHOLD = 110.0 # Extreme selectivity
    elif macro == "NEUTRAL":
        COGNITIVE_THRESHOLD = 95.0  # High selectivity
    else:
        COGNITIVE_THRESHOLD = 85.0  # Bullish selectivity
        
    for symbol in candidates:
        scores = agent_scores.get(symbol, {})
        vision_data = state.get("vision_validations", {}).get(symbol, {})
        pattern = vision_data.get("identified_pattern", "unknown")
        
        # Calculate Relative Strength (RS) Alpha
        rs_score = scores.get("sector", 50.0) / 50.0 # 1.0 is parity
        
        pattern_rule = rules.get(pattern, {})
        priority = pattern_rule.get("institutional_priority", 5)
        
        # --- BASE WEIGHTED COGNITIVE CONSENSUS (Max 100) ---
        w_entry = scores.get("entry", 0) * 0.40
        w_vision = scores.get("vision", 0) * 0.40
        w_dtw = scores.get("dtw", 0) * 0.20
        total_confidence = w_entry + w_vision + w_dtw
        
        # --- ELITE BONUSES (The "Sovereign" Push) ---
        # 1. Resilience Bonus
        if macro == "BEARISH" and rs_score > 1.4:
            logging.info(f"ELITE RESILIENCE: {symbol} leading in bad market. +20 pts")
            total_confidence += 20.0
            
        # 2. Pattern-Regime Alignment
        target_regime = pattern_rule.get("regime_fit", "UNKNOWN")
        if target_regime == active_regime:
            logging.info(f"REGIME ALIGNMENT: {pattern} fits {active_regime}. +15 pts")
            total_confidence += 15.0

        # 3. Priority Multiplier
        if priority >= 9:
            logging.info(f"INSTITUTIONAL PRIORITY: {symbol} ({pattern}). +10 pts")
            total_confidence += 10.0
            
        # --- ADVERSARIAL CRITIQUE (The Veto) ---
        target_date = state.get("target_date")
        evaluation = critic.evaluate_thesis(symbol, "thesis", macro, target_date)
        if evaluation["veto"]:
            logging.warning(f"CRITIC VETO: {symbol} - {evaluation['critique']}")
            total_confidence -= 30.0 # Heavy penalty for distribution
            
        final_approval = (total_confidence >= COGNITIVE_THRESHOLD)
        evaluation["total_confidence"] = float(total_confidence)
        evaluation["approved"] = final_approval
        evaluation["rs_alpha"] = rs_score
        critic_results[symbol] = evaluation
        
        if final_approval:
            logging.info(f"SOVEREIGN ELITE APPROVAL: {symbol} ({total_confidence:.1f}/{COGNITIVE_THRESHOLD})")
        else:
            logging.info(f"SOVEREIGN REJECTION: {symbol} ({total_confidence:.1f}/{COGNITIVE_THRESHOLD})")
        
    return {
        "critic_results": critic_results, 
        "agent_scores": agent_scores,
        "debate_count": 1
    }
