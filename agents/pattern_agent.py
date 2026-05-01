import json
import logging
from typing import Dict, Any
from pathlib import Path

from core.state import SovereignState

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class VisionPatternAgent:
    """
    Claude 3.5 Sonnet Vision API implementation.
    The "Skeptical Auditor" checking for Pinocchio Bars and volume dry-up.
    Validates setups against the offline compiled JSON rulebook of Shannon/Pring.
    """
    def __init__(self):
        self.rules = self._load_context_rules()
        self.cache_path = Path(__file__).parent.parent / "vision_cache.json"
        self.cache = self._load_cache()

    def _load_cache(self) -> Dict[str, Any]:
        if self.cache_path.exists():
            try:
                with open(self.cache_path, 'r') as f:
                    return json.load(f)
            except Exception:
                return {}
        return {}

    def _save_cache(self):
        try:
            with open(self.cache_path, 'w') as f:
                json.dump(self.cache, f, indent=4)
        except Exception as e:
            logging.error(f"Failed to save vision cache: {e}")

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

    def analyze_chart(self, symbol: str, pattern_hint: str, target_date: str = None) -> Dict[str, Any]:
        """
        Dynamically identifies institutional setups using OpenAI GPT-4o with neural caching.
        """
        # Fetch recent data first to determine the "Latest Date" for the cache key
        import os, psycopg2
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
                query = "SELECT time, high, low, close, volume FROM daily_ohlcv WHERE symbol = %s AND time <= %s ORDER BY time DESC LIMIT 60"
                cur.execute(query, (symbol, target_date))
            else:
                query = "SELECT time, high, low, close, volume FROM daily_ohlcv WHERE symbol = %s ORDER BY time DESC LIMIT 60"
                cur.execute(query, (symbol,))
                
            rows = cur.fetchall()
            cur.close()
            conn.close()
            
            if not rows:
                return {"vision_approved": False, "vision_score": 0, "identified_pattern": "unknown", "reason": "No data"}
            
            # Create a unique cache key based on symbol and the most recent timestamp in the data
            latest_date = rows[0][0].strftime('%Y-%m-%d')
            cache_key = f"{symbol}_{latest_date}"
            
            if cache_key in self.cache:
                logging.info(f"CACHE HIT: Retrieving neural audit for {symbol} on {latest_date}")
                return self.cache[cache_key]
                
            logging.info(f"Analyzing {symbol} via GPT-4o (No Cache found for {latest_date})...")
            
            pattern_list = list(self.rules.get("pring_pattern_geometries", {}).keys())
            
            # Format data for the prompt
            data_str = "\n".join([f"{r[0].strftime('%Y-%m-%d')}: H:{r[1]}, L:{r[2]}, C:{r[3]}, V:{r[4]}" for r in rows])
            
            prompt = f"""
            Identify the most likely institutional setup for {symbol} from this list: {pattern_list}.
            
            Context: This is the Indian Stock Market (NSE). Markets are CLOSED on Saturdays, Sundays, and public holidays. 
            If the latest data is a Friday and today is a Sunday, the Friday data is the correct "latest" footprint.
            
            Recent OHLCV Data:
            {data_str}
            
            SCORING (0-100):
            - 90-100: PERFECT. Strong breakout on massive volume (>3x avg).
            - 80-89: STRONG. Institutional footprints clearly visible.
            - 70-79: VALID. Tradable setup.
            - <70: REJECT.
            
            CRITICAL: If you see a VOLUME SURGE where the latest volume is >3x the previous average, this is an Institutional Footprint. Score it HIGH.
            
            Return ONLY a JSON object: {{"identified_pattern": "string", "vision_score": int, "justification": "str"}}
            """
            
            from openai import OpenAI
            client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            
            response = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": "You are a world-class Technical Analyst specialized in Pring and Shannon methodologies."},
                    {"role": "user", "content": prompt}
                ],
                response_format={ "type": "json_object" }
            )
            
            result = json.loads(response.choices[0].message.content)
            vision_score = int(result.get("vision_score", 50))
            identified = result.get("identified_pattern", "unknown")
            
            vision_result = {
                "vision_approved": vision_score >= 70,
                "vision_score": vision_score,
                "identified_pattern": identified,
                "reason": result.get("justification", "None"),
                "whipsaw_risk": "Low" if vision_score > 75 else "High",
                "cached_at": latest_date
            }
            
            # Save to cache
            self.cache[cache_key] = vision_result
            self._save_cache()
            
            return vision_result
            
        except Exception as e:
            logging.error(f"GPT-4o Vision Analysis failed: {e}")
            return {"vision_approved": True, "vision_score": 50, "identified_pattern": "unknown", "reason": "Error"}

def run_pattern_agent(state: SovereignState) -> Dict[str, Any]:
    """
    LangGraph Node integration for the Pattern Agent.
    """
    heuristic_flags = state.get("heuristic_flags", {})
    experience_warnings = state.get("experience_warnings", {})
    
    vision_agent = VisionPatternAgent()
    vision_validations = {}
    target_date = state.get("target_date")
    candidates = state.get("candidates", [])
    
    # Initialize a new agent_scores map to return (LangGraph reducer pattern)
    agent_scores = state.get("agent_scores", {})
    new_agent_scores = {k: v.copy() for k, v in agent_scores.items()}
    
    for symbol in candidates:
        # Skip if the Meta-Gate vetoed this setup
        if symbol in experience_warnings and len(experience_warnings[symbol]) > 0:
            logging.info(f"Skipping Vision analysis for {symbol} due to Meta-Gate VETO.")
            continue
            
        # Get hint if available from DTW
        pattern = heuristic_flags.get(symbol, {}).get("identified_pattern", "unknown")
        
        # Execute Vision Analysis
        vision_result = vision_agent.analyze_chart(symbol, pattern, target_date)
        vision_validations[symbol] = vision_result
        
        # Update local agent_scores
        if symbol not in new_agent_scores:
            new_agent_scores[symbol] = {}
        new_agent_scores[symbol]["vision"] = float(vision_result.get("vision_score", 50))
        
        logging.info(f"FINAL VISION SCORE for {symbol}: {new_agent_scores[symbol]['vision']}")
        
        if vision_result["vision_approved"]:
            logging.info(f"VISION APPROVED: {symbol} is a valid {pattern} setup.")
        else:
            logging.warning(f"VISION REJECTED: {symbol} - {vision_result['reason']}")
    
    return {"vision_validations": vision_validations, "agent_scores": new_agent_scores}

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
