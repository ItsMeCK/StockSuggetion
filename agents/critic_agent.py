import logging
from typing import Dict, Any
from midnight_sovereign.core.state import SovereignState

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
                
                # --- RESTORED: Distribution Veto Re-enabled to match 61% baseline ---
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
    
    # --- NEURAL STRATEGY ORCHESTRATOR (Dynamic Playbook) ---
    # Map engine regime to playbook regime
    regime_map = {
        "BULLISH": "BULL_MARKET",
        "BEARISH": "BEAR_MARKET",
        "NEUTRAL": "CHOPPY_SIDEWAYS"
    }
    active_regime = regime_map.get(macro, "CHOPPY_SIDEWAYS")
    
    rules = critic.rules.get("pring_pattern_geometries", {})
    
    for symbol in candidates:
        scores = agent_scores.get(symbol, {})
        heuristic_data = state.get("heuristic_flags", {}).get(symbol, {})
        pattern = heuristic_data.get("identified_pattern", "unknown")
        
        # Load the specific institutional rule for this pattern
        pattern_rule = rules.get(pattern, {})
        
        # --- WEIGHTED COGNITIVE CONSENSUS SCORING ---
        w_entry = scores.get("entry", 0) * 0.40
        w_vision = scores.get("vision", 0) * 0.40
        w_dtw = scores.get("dtw", 0) * 0.20
        
        total_confidence = w_entry + w_vision + w_dtw
        
        # 1. Regime Fit Check (Thematic Alignment)
        target_regime = pattern_rule.get("regime_fit", "UNKNOWN")
        theme = pattern_rule.get("thematic_category", "UNKNOWN")
        
        if target_regime == active_regime:
            logging.info(f"REGIME MATCH: {symbol} setup is optimized for {active_regime} ({theme}). Boosting +5%")
            total_confidence += 5.0
        elif target_regime != "UNKNOWN" and target_regime != active_regime:
            logging.warning(f"REGIME MISMATCH: {symbol} ({pattern}) is a {target_regime} setup in a {active_regime} market. Penalizing -10%")
            total_confidence -= 10.0
            
        # 2. Institutional Priority Boost
        priority = pattern_rule.get("institutional_priority", 5)
        if priority >= 8:
            logging.info(f"ELITE PRIORITY: {symbol} is a Tier 1 Institutional setup. Boosting +3%")
            total_confidence += 3.0
            
        # 3. Run base adversarial check (Veto penalty)
        evaluation = critic.evaluate_thesis(symbol, "thesis", macro)
        if evaluation["veto"]:
            total_confidence -= 20.0
            
        # Final Decision Logic (Restored 70% Baseline)
        COGNITIVE_THRESHOLD = 70.0
        final_approval = (total_confidence >= COGNITIVE_THRESHOLD)
        
        evaluation["total_confidence"] = float(total_confidence)
        evaluation["approved"] = final_approval
        critic_results[symbol] = evaluation
        
        if final_approval:
            logging.info(f"NEURAL APPROVAL: {symbol} passed {macro} regime with {total_confidence:.1f}% confidence!")
        else:
            logging.warning(f"NEURAL REJECTION: {symbol} failed {macro} regime ({total_confidence:.1f}/{COGNITIVE_THRESHOLD})")
        
    return {
        "critic_results": critic_results, 
        "agent_scores": agent_scores,
        "debate_count": 1
    }
