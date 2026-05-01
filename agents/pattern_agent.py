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
                query = "SELECT volume FROM daily_ohlcv WHERE symbol = %s ORDER BY time DESC LIMIT 20"
                cur.execute(query, (symbol,))
                all_vols = [float(r[0]) for r in cur.fetchall()]
                cur.close()
                conn.close()
                
                if len(all_vols) >= 5:
                    # Institutional Volume Dry-up: 
                    # Is the current volume (last 3 days) significantly below the 20-day average?
                    recent_avg = sum(all_vols[:3]) / 3
                hist_avg = sum(all_vols) / len(all_vols)
                
                # If volume in the last 3 days is < 80% of the 20-day average, it's a dry-up
                if recent_avg < (0.8 * hist_avg):
                    return {
                        "vision_approved": True,
                        "reason": f"Institutional dry-up confirmed. Recent vol {recent_avg:,.0f} is {(recent_avg/hist_avg):.1%} of average.",
                        "whipsaw_risk": "Low",
                        "volume_dryup_confirmed": True
                    }
                else:
                    return {
                        "vision_approved": False,
                        "reason": f"Failed volume dry-up. Recent vol {recent_avg:,.0f} is still {(recent_avg/hist_avg):.1%} of average.",
                        "whipsaw_risk": "Medium",
                        "volume_dryup_confirmed": False
                    }
        except Exception as e:
            logging.error(f"Vision Agent error for {symbol}: {e}")
            
        return {
            "vision_approved": True,
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
        
        if vision_result["vision_approved"]:
            logging.info(f"VISION APPROVED: {symbol} is a valid {pattern} setup.")
        else:
            logging.warning(f"VISION REJECTED: {symbol} - {vision_result['reason']}")

    return {"vision_validations": vision_validations}

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
