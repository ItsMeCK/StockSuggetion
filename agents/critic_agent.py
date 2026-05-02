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
        rules_path = core_dir / "context_rules_2.json"
        try:
            with open(rules_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception:
            return {}
        
    def evaluate_thesis(self, symbol: str, thesis: str, macro_regime: str, target_date: str = None) -> Dict[str, Any]:
        import os, psycopg2
        logging.info(f"Critic Agent: Performing Master Audit for {symbol}...")
        
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
            
            # --- RIGOR: Distribution Check (5-day volume profiling) ---
            query = "SELECT close, volume FROM daily_ohlcv WHERE symbol = %s AND time <= %s ORDER BY time DESC LIMIT 5"
            cur.execute(query, (symbol, target_date if target_date else '2099-01-01'))
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if len(rows) >= 5:
                prices = [float(r[0]) for r in rows[::-1]]
                volumes = [float(r[1]) for r in rows[::-1]]
                
                # Check for 'Hidden Distribution' (Price UP, Volume DOWN)
                if prices[-1] > prices[0] and volumes[-1] < volumes[0]:
                    veto_required = True
                    reason = "VSA_TRAP: Hidden Distribution (Price rising on thinning volume)."
        except Exception as e:
            logging.error(f"Critic Agent Error: {e}")
            
        return {"veto": veto_required, "critique": reason}

def run_critic_agent(state: SovereignState) -> Dict[str, Any]:
    candidates = state.get("candidates", [])
    agent_scores = state.get("agent_scores", {})
    macro = state.get("macro_regime", "UNKNOWN")
    vision_validations = state.get("vision_validations", {})
    
    critic = CriticAgent()
    critic_results = {}
    
    regime_map = {"BULLISH": "BULL_MARKET", "BEARISH": "BEAR_MARKET", "NEUTRAL": "CHOPPY_SIDEWAYS"}
    active_regime = regime_map.get(macro, "CHOPPY_SIDEWAYS")
    all_rules = critic.rules.get("pring_pattern_geometries", {})
    
    for symbol in candidates:
        scores = agent_scores.get(symbol, {})
        vision_res = vision_validations.get(symbol, {})
        pattern = vision_res.get("identified_pattern", "unknown")
        dq_flag = vision_res.get("disqualification_flag", "None")
        
        # 1. FETCH MASTER WEIGHTS (v2.3)
        rule = all_rules.get(pattern, {})
        priority = rule.get("institutional_priority", 5)
        risk_weight = rule.get("risk_weight", 5)
        suitable_regimes = rule.get("suitable_regime", [])
        if isinstance(suitable_regimes, str): suitable_regimes = [suitable_regimes]
        
        # 2. BASE WEIGHTED SCORE (v2.3 Additive Stack)
        w_entry = scores.get("entry", 0) * 0.60
        w_vision = scores.get("vision", 0) * 0.60
        w_dtw = scores.get("dtw", 0) * 0.30
        base_score = w_entry + w_vision + w_dtw
        
        # 3. MASTER JSON RIGOR ADJUSTMENTS (The 'Sovereign Multipliers')
        # A. Priority Boost (High Institutional Priority = Massive Alpha)
        priority_boost = (priority - 5) * 10.0
        # B. Risk Penalty (High Risk = Massive Veto)
        risk_penalty = (risk_weight - 5) * 8.0
        # C. Regime Friction
        regime_penalty = 0
        if suitable_regimes and active_regime not in suitable_regimes:
            regime_penalty = 30.0
            
        final_score = base_score + priority_boost - risk_penalty - regime_penalty
        
        # 4. THE MASTER VETO (Hard Rejections)
        veto_reason = "None"
        if dq_flag != "None":
            final_score -= 50.0
            veto_reason = f"HARD VETO: {dq_flag} detected by Auditor."
        
        # Distribution Audit
        target_date = state.get("target_date")
        evaluation = critic.evaluate_thesis(symbol, "thesis", macro, target_date)
        if evaluation["veto"]:
            final_score -= 25.0
            veto_reason = evaluation["critique"]
            
        # FINAL SOVEREIGN CRITERIA (Sovereign v2.4 Elite Rigor: 130+ Hurdle)
        is_elite = final_score >= 130
        is_momentum = scores.get("entry", 0) >= 90 and final_score >= 120
        is_accumulator = (scores.get("vision", 0) >= 95) and (final_score >= 115)
        
        # Restore the 'Sovereign Pure' Gate
        final_approval = (is_elite or is_momentum or is_accumulator) and (final_score >= 110)
        
        evaluation["total_confidence"] = float(final_score)
        evaluation["is_elite"] = is_elite
        evaluation["is_momentum"] = is_momentum
        evaluation["is_accumulator"] = is_accumulator
        evaluation["approved"] = final_approval
        evaluation["veto_reason"] = veto_reason
        
        if final_approval:
            critic_results[symbol] = evaluation
            logging.info(f"HYBRID ELITE APPROVED: {symbol} (Elite: {is_elite}, Mom: {is_momentum}, Acc: {is_accumulator})")
        else:
            logging.warning(f"CRITIC VETO: {symbol} - Failed Hybrid Elite criteria. (Conf: {final_score:.1f})")
        
    return {
        "critic_results": critic_results, 
        "agent_scores": agent_scores,
        "debate_count": 1
    }
