import os
import json
import logging
from typing import List, Dict, Any
from pydantic import BaseModel
from dotenv import load_dotenv
from google import genai
from google.genai import types

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Disable excessive HTTP request logs from google API
logging.getLogger("google.api_core.bidi").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

class RankedTrade(BaseModel):
    symbol: str
    catalyst_summary: str
    conviction_score: int

class RankingResponse(BaseModel):
    top_trades: List[RankedTrade]

class LLMRankingAgent:
    """
    Fundamental LLM Ranking Agent powered by Gemini with Google Search Grounding.
    Takes a list of mathematically valid setups and selects the top picks based on real-world catalysts.
    """
    def __init__(self):
        load_dotenv()
        gemini_api_key = os.getenv("GEMINI_API_KEY")
        if gemini_api_key:
            self.client = genai.Client(api_key=gemini_api_key)
            self.model_name = 'gemini-3.5-flash'
        else:
            raise ValueError("GEMINI_API_KEY not found in .env")

    def rank_trades(self, symbols: List[str], max_picks: int = 2) -> List[Dict[str, Any]]:
        logging.info(f"🔍 Initiating LLM Fundamental Analysis on {len(symbols)} candidate symbols...")
        
        prompt = f"""
        You are an elite Institutional Fundamental Equity Analyst specializing in the Indian Stock Market (NSE). 
        I have mathematically screened {len(symbols)} Indian stocks that are currently exhibiting extreme "Coiled Spring" technical setups (massive institutional volume spikes paired with tight volatility compression). 
        
        The mathematical filter is passive. Your job is active. I need you to find the REAL WORLD CATALYST causing these volume spikes.
        
        The candidate stocks are:
        {', '.join(symbols)}
        
        Your Instructions:
        1. Use Google Search to investigate recent news, earnings reports (Q1 FY25), management commentary, or corporate announcements for each of these companies within the last 7-14 days.
        2. Rank the stocks based strictly on the strength of their fundamental catalyst. 
           - Tier 1: Blockbuster Earnings Beat or Massive Upgrades
           - Tier 2: Major Government Contract Wins / M&A Activity
           - Tier 3: Generic Sector Tailwinds
           - Tier 4: No clear news (Reject these)
        3. Select ONLY the TOP {max_picks} stocks with the absolute strongest, most explosive fundamental catalysts.
        4. Assign a conviction score (0-100) and provide a concise 1-sentence catalyst summary for each of the top picks.
        """
        
        try:
            config = types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=RankingResponse,
                temperature=0.1,
                tools=[{"google_search": {}}] # Enable Google Search Grounding for live fundamental news
            )
            
            logging.info("🧠 Sending request to Gemini-2.5-Pro (with Web Search enabled)...")
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=config
            )
            
            parsed_result = response.parsed
            if parsed_result is None:
                import json
                text = response.text.strip()
                if text.startswith('```json'):
                    text = text[7:-3]
                data = json.loads(text)
                ranked = data.get("top_trades", [])
            else:
                ranked = [trade.model_dump() for trade in parsed_result.top_trades]
            
            logging.info(f"✅ LLM successfully ranked the top {len(ranked)} catalysts.")
            for r in ranked:
                logging.info(f"🏆 {r['symbol']} (Score: {r['conviction_score']}) -> {r['catalyst_summary']}")
                
            return ranked
            
        except Exception as e:
            logging.error(f"LLM Ranking Agent Error: {e}")
            return []

if __name__ == "__main__":
    agent = LLMRankingAgent()
    # Test with a subset of Friday's trades (mixing winners like REDINGTON/ASHOKLEY with losers like GAIL/ZEEL)
    test_symbols = ["REDINGTON", "ASHOKLEY", "INDGN", "TBOTEK", "ABCAPITAL", "GAIL", "ZEEL", "MGL"]
    results = agent.rank_trades(test_symbols)
    print(json.dumps(results, indent=2))
