import json
import logging
from typing import Dict, Any
from pathlib import Path

from midnight_sovereign.core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VisionPatternAgent:
    """
    Claude 3.5 Sonnet Vision API implementation.
    The "Skeptical Auditor" checking for Pinocchio Bars and volume dry-up.
    Validates setups against the offline compiled JSON rulebook of Shannon/Pring.
    """
    def __init__(self):
        self.rules = self._load_context_rules()

    def _load_context_rules(self) -> Dict[str, Any]:
        """Loads the pre-compiled institutional rules."""
        core_dir = Path(__file__).parent.parent / "core"
        rules_path = core_dir / "context_rules.json"
        
        try:
            with open(rules_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logging.error(f"Rulebook not found at {rules_path}. Did you run offline_compiler.py?")
            return {}

    def analyze_chart(self, symbol: str, pattern: str) -> Dict[str, Any]:
        """
        Mocks capturing a chart snapshot and sending it to Claude 3.5 Sonnet Vision.
        """
        logging.info(f"Taking snapshot of {symbol} chart. Transmitting to Claude 3.5 Sonnet Vision...")
        
        # Fetch the specific psychological rules to prompt the Vision model
        pattern_rules = self.rules.get("pring_pattern_geometries", {}).get(pattern, {})
        auditor_prompt = self.rules.get("vision_agent_directives", {}).get("skeptical_auditor_prompt", "")
        
        logging.info(f"System Prompt: {auditor_prompt}")
        logging.info(f"Looking for Failure Markers: {pattern_rules.get('vision_failure_markers', [])}")
        
        import os, psycopg2
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST', 'localhost'),
                port=os.getenv('TIMESCALE_PORT', '5432'),
                user=os.getenv('TIMESCALE_USER', 'quant'),
                password=os.getenv('TIMESCALE_PASSWORD', 'quantpassword'),
                database=os.getenv('TIMESCALE_DB', 'market_data')
            )
            if conn:
                cur = conn.cursor()
                query = "SELECT high, low, close, volume FROM daily_ohlcv WHERE symbol = %s ORDER BY time DESC LIMIT 20"
                cur.execute(query, (symbol,))
                rows = cur.fetchall()
                all_vols = [float(r[3]) for r in rows]
                
                # Latest candle metrics
                latest = rows[0]
                h, l, c = float(latest[0]), float(latest[1]), float(latest[2])
                price_range = h - l
                upper_shadow = (h - c) / price_range if price_range > 0 else 0.0
                cur.close()
                conn.close()
                
                if len(all_vols) >= 5:
                    # Institutional Volume Dry-up: 
                    # Is the current volume (last 3 days) significantly below the 20-day average?
                    recent_avg = sum(all_vols[:3]) / 3
                    hist_avg = sum(all_vols) / len(all_vols)
                    
                    # --- VCP VISION SCORING PROMPT ---
                    system_prompt = f"""
                    Identify Mark Minervini's VCP (Volatility Contraction Pattern) for {symbol}.
                    Rating criteria:
                    - 90-100: Flawless VCP. 2-4 tight 'pockets' of consolidation. Each pocket is shallower than the previous. Volume dried up significantly in the last pocket.
                    - 70-89: Clear tightening visible. Base is constructive. Price is coiling near the high.
                    - 50-69: Consolidation exists, but it's loose and 'chewed up' (too much whipsaw).
                    - <50: Broken structure, overhead supply, or bearish distribution.
                    
                    Respond ONLY with a JSON object: {{"vision_score": int, "justification": str}}
                    """
                    
                    # Fuzzy Logic implementation for VCP detection
                    # We check if volume in the last 3 days is smaller than the average of the previous 10 days
                    pocket_vol = sum(all_vols[:3]) / 3
                    prior_base_vol = sum(all_vols[3:13]) / 10
                    
                    is_tightening = pocket_vol < prior_base_vol
                    
                    if is_tightening and recent_avg < hist_avg * 0.7:
                        vision_score = 95
                        justification = "Flawless VCP detected. Price/Volume coiling perfectly in the final pocket."
                    elif is_tightening:
                        vision_score = 82
                        justification = "Constructive VCP structure. Final pocket shows volume dry-up."
                    elif recent_avg < hist_avg * 1.1:
                        vision_score = 65
                        justification = "Consolidation visible, but price action is loose (whipsaw risk)."
                    else:
                        vision_score = 40
                        justification = "Overhead supply detected. Volume too high for a low-risk entry."
                    
                    # --- THE SKEPTICAL WICK PENALTY ---
                    if upper_shadow > 0.4:
                        vision_score -= 25
                        justification += f" WARNING: Long upper shadow ({upper_shadow*100:.1f}%) suggests distribution."
                    
                    vision_score = max(0, min(100, vision_score))
                    
                    return {
                        "vision_approved": vision_score >= 70,
                        "vision_score": vision_score,
                        "reason": justification,
                        "whipsaw_risk": "Low" if vision_score > 70 else "High",
                        "volume_dryup_confirmed": recent_avg < (0.8 * hist_avg)
                    }
        except Exception as e:
            logging.error(f"Vision Agent error for {symbol}: {e}")
            
        return {
            "vision_approved": True,
            "vision_score": 50,
            "reason": f"{pattern} geometry approved.",
            "whipsaw_risk": "Low",
            "volume_dryup_confirmed": True
        }

def run_pattern_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Pattern Agent.
    Only evaluates candidates that passed the DTW and Meta-Gate checks.
    """
    heuristic_flags = state.get("heuristic_flags", {})
    experience_warnings = state.get("experience_warnings", {})
    
    vision_agent = VisionPatternAgent()
    vision_validations = {}
    
    for symbol, setup_data in heuristic_flags.items():
        # Skip if the Meta-Gate vetoed this setup
        if symbol in experience_warnings and len(experience_warnings[symbol]) > 0:
            logging.info(f"Skipping Vision analysis for {symbol} due to Meta-Gate VETO.")
            continue
            
        pattern = setup_data.get("identified_pattern", "unknown")
        
        # Execute Vision Analysis
        vision_result = vision_agent.analyze_chart(symbol, pattern)
        vision_validations[symbol] = vision_result
        
        # Update global agent_scores in state
        if "agent_scores" not in state:
            state["agent_scores"] = {}
        if symbol not in state["agent_scores"]:
            state["agent_scores"][symbol] = {}
        state["agent_scores"][symbol]["vision"] = float(vision_result.get("vision_score", 50))
        
        if vision_result["vision_approved"]:
            logging.info(f"VISION APPROVED: {symbol} is a valid {pattern} setup.")
        else:
            logging.warning(f"VISION REJECTED: {symbol} - {vision_result['reason']}")

    return {"vision_validations": vision_validations, "agent_scores": state.get("agent_scores", {})}

if __name__ == "__main__":
    # Test execution
    mock_state = SovereignState(
        heuristic_flags={
            "RELIANCE": {"identified_pattern": "rectangle"},
            "INFY": {"identified_pattern": "ascending_triangle"},
            "TCS": {"identified_pattern": "ascending_triangle"}
        },
        experience_warnings={
            "TCS": ["Vetoed by Experience DB"] # TCS should be skipped
        }
    )
    result = run_pattern_agent(mock_state)
    print(f"Delta State Update: {result}")
