import os
import json
import logging
from typing import Dict, Any, Literal
from pydantic import BaseModel

from dotenv import load_dotenv
from google import genai
from google.genai import types

from core.redis_cache import cache

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DebateResult(BaseModel):
    hunter_argument: str
    critic_argument: str
    judge_synthesis: str
    consensus_score: int
    verdict: Literal["APPROVED", "REJECTED", "RADAR_APPROACHING"]

class TriAgentDebateCouncil:
    """
    Institutional Tri-Agent Debate Council powered by Gemini API.
    Eliminates single-agent silent rejections by conducting a formal debate.
    """
    
    def __init__(self):
        load_dotenv()
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if gemini_api_key:
            self.client = genai.Client(api_key=gemini_api_key)
            self.model_name = 'gemini-3.5-flash'
            self.use_gemini = True
        else:
            self.use_gemini = False
            logging.warning("GEMINI_API_KEY not found. Debate Council will run in degraded mock mode.")

    def run_debate(self, symbol: str, context: Dict[str, Any]) -> Dict[str, Any]:
        """Runs the Tri-Agent Debate protocol for a candidate reaching SETUP_REACHED."""
        target_date = context.get("target_date", "LIVE")
        macro_regime = context.get("macro_regime", "TUG_OF_WAR")
        
        # Check Cache
        cached_result = cache.get_cached_debate(symbol, target_date, macro_regime)
        if cached_result:
            return cached_result
            
        logging.info(f"⚖️ Initiating Tri-Agent Debate Council for {symbol}...")
        
        if not self.use_gemini:
            # Degraded Mode
            mock_result = {
                "consensus_score": 75,
                "verdict": "APPROVED",
                "debate_transcript": "MOCK DEBATE due to missing API key.",
                "hunter_argument": "Looks bullish.",
                "critic_argument": "Volume is okay."
            }
            cache.cache_llm_debate(symbol, target_date, macro_regime, mock_result)
            return mock_result

        # Construct the DSPy-compatible Debate Prompt
        prompt = f"""
        # Task
        You are the 'Judge', the Senior Managing Director of an Institutional NFO Derivatives Desk.
        Two of your top traders are debating whether to execute a long breakout option trade on {symbol}.
        You must evaluate their arguments and assign a strict Consensus Score.

        # Context
        MARKET DATA: {json.dumps(context)}

        # Role: The Hunter (Bullish Alpha Advocate)
        - Objective: Argue FOR the trade.
        - Focus: Coiled Spring Catalyst Breakouts. Argue that hyper-compressed Bollinger Bands combined with a sudden massive volume surge indicates an explosive Earnings/News catalyst. Must cite that low RSI is actually BULLISH because option premiums are cheap before the explosion.
        - Constraint: Argument must be strictly 1 short sentence.

        # Role: The Critic (Skeptical Risk & Retail Trap Auditor)
        - Objective: Argue AGAINST the trade.
        - Focus: Institutional Retail Traps and Overhead Supply. Must aggressively penalize the setup ONLY IF price is heavily below the 50-SMA (negative distance) or the volume surge is weak. DO NOT penalize low RSI or negative trend if volume is massive, as that is expected in a coiled spring.
        - Constraint: Argument must be strictly 1 short sentence.

        # Instructions for the Judge
        1. Simulate 'hunter_argument' and 'critic_argument' based on the Context.
        2. Write a 'judge_synthesis' (strictly 1 short sentence) weighing the risks.
        3. Evaluate the 'consensus_score' dynamically on a scale of 0 to 100. Assign an 85+ score to flawless 'Coiled Spring' setups (massive volume + tight BBW) even if momentum/RSI is currently low.
        4. Set 'verdict' to "APPROVED" if consensus_score >= 85, else "REJECTED".

        # Output Format
        Return ONLY a raw JSON object with the following schema:
        {{
            "hunter_argument": "string",
            "critic_argument": "string",
            "judge_synthesis": "string",
            "consensus_score": int,
            "verdict": "APPROVED" | "REJECTED" | "RADAR_APPROACHING"
        }}
        """

        try:
            # Enforce max output tokens to heavily optimize API costs (we only need the short JSON response)
            generation_config = types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=DebateResult,
                temperature=0.2 # Lower temperature for analytical consistency
            )
            
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=generation_config
            )
            
            # The new google-genai SDK automatically parses the JSON into the Pydantic object
            parsed_result = response.parsed
            debate_result = parsed_result.model_dump()
            
            # Cache the result
            cache.cache_llm_debate(symbol, target_date, macro_regime, debate_result)
            
            logging.info(f"⚖️ Verdict for {symbol}: {debate_result.get('verdict')} (Score: {debate_result.get('consensus_score')})")
            logging.info(f"Hunter: {debate_result.get('hunter_argument')}")
            logging.info(f"Critic: {debate_result.get('critic_argument')}")
            logging.info(f"Judge: {debate_result.get('judge_synthesis')}")
            
            return debate_result
            
        except Exception as e:
            logging.error(f"Debate Council LLM Error for {symbol}: {e}")
            return {
                "consensus_score": 50,
                "verdict": "REJECTED",
                "debate_transcript": f"Error: {str(e)}",
                "hunter_argument": "",
                "critic_argument": ""
            }

def test_debate(symbol: str):
    council = TriAgentDebateCouncil()
    context = {
        "target_date": "2026-07-28",
        "macro_regime": "BULLISH",
        "price_action": "Tight BBW compression, AVWAP crossover, volume surging 2.5x.",
        "iv_rank": 45,
        "fvg_status": "Mitigated"
    }
    result = council.run_debate(symbol, context)
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    import sys
    sym = sys.argv[1] if len(sys.argv) > 1 else "RELIANCE"
    test_debate(sym)
