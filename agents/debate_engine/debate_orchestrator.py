import os
import time
from datetime import datetime
from dotenv import load_dotenv
from google import genai
from google.genai import types

from agents.debate_engine.intelligence_scout import IntelligenceScout
from core.db_manager import init_db, add_anticipatory_stock

class DebateOrchestrator:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY is not set.")
        self.client = genai.Client(api_key=self.api_key)
        self.model = "gemini-3.5-flash"
        
        # Tools for Google Search Grounding
        self.config = types.GenerateContentConfig(
            temperature=0.3,
            tools=[{"google_search": {}}]
        )

    def run_debate(self, symbol):
        print(f"\n==============================================")
        print(f"⚖️ INITIATING DEBATE: {symbol}")
        print(f"==============================================")
        
        # 1. Bull Thesis
        bull_prompt = f"""
        You are the 'Bull Agent' for the Indian Stock Market. 
        Your goal is to build the strongest possible BULLISH thesis for NSE:{symbol} right now.
        Use Google Search to find any upcoming earnings (this week), major business updates, analyst upgrades, or positive macroeconomic tailwinds.
        Keep your argument concise and data-driven (max 3-4 sentences). 
        Do not mention that you are an AI. Just provide the thesis.
        """
        print(f"🐂 Bull Agent is gathering data for {symbol}...")
        try:
            bull_resp = self.client.models.generate_content(
                model=self.model, contents=bull_prompt, config=self.config
            )
            bull_thesis = bull_resp.text.strip()
        except Exception as e:
            print(f"Bull Agent error: {e}")
            return None
            
        print(f"🐂 BULL THESIS:\n{bull_thesis}\n")
        time.sleep(2) # Avoid rate limits
        
        # 2. Bear Thesis
        bear_prompt = f"""
        You are the 'Bear Agent' (Risk Manager) for the Indian Stock Market. 
        Your goal is to completely destroy the following BULLISH thesis for NSE:{symbol}.
        
        BULLISH THESIS:
        {bull_thesis}
        
        Use Google Search to find risks: Is the news fake/unverified? Is there a massive IV crush risk? Did they historically miss earnings? Is the sector struggling?
        Keep your argument concise, adversarial, and data-driven (max 3-4 sentences).
        """
        print(f"🐻 Bear Agent is cross-examining {symbol}...")
        try:
            bear_resp = self.client.models.generate_content(
                model=self.model, contents=bear_prompt, config=self.config
            )
            bear_thesis = bear_resp.text.strip()
        except Exception as e:
            print(f"Bear Agent error: {e}")
            return None
            
        print(f"🐻 BEAR THESIS:\n{bear_thesis}\n")
        time.sleep(2)
        
        # 3. The Judge
        judge_prompt = f"""
        You are the 'Judge Agent'. Review the debate for NSE:{symbol} and decide if this stock has a massive, highly-credible impending catalyst that warrants an anticipatory trade before the market reacts.
        
        BULL THESIS: {bull_thesis}
        BEAR THESIS: {bear_thesis}
        
        You must return a raw JSON object with exactly two keys:
        - "reasoning": A 1-sentence explanation of who won the debate.
        - "score": An integer from 0 to 100 (where >90 means absolute conviction it will gap up/explode).
        
        Do not include markdown or any other text.
        """
        judge_config = types.GenerateContentConfig(
            temperature=0.1,
            response_mime_type="application/json"
        )
        print(f"⚖️ The Judge is deliberating on {symbol}...")
        try:
            judge_resp = self.client.models.generate_content(
                model=self.model, contents=judge_prompt, config=judge_config
            )
            raw_text = judge_resp.text.strip()
            import json
            # Extract JSON block ignoring conversational padding or markdown
            start_idx = raw_text.find('{')
            end_idx = raw_text.rfind('}')
            if start_idx != -1 and end_idx != -1:
                raw_text = raw_text[start_idx:end_idx+1]
            
            result = json.loads(raw_text)
            score = result.get("score", 0)
            print(f"⚖️ VERDICT SCORE: {score}/100")
            print(f"⚖️ REASONING: {result.get('reasoning')}")
            
            return {
                "bull_thesis": bull_thesis,
                "bear_thesis": bear_thesis,
                "score": score
            }
        except Exception as e:
            print(f"Judge Agent error: {e}")
            return None

def run_daily_debate():
    import json
    init_db() # Ensure table exists
    
    in_path = "data/daily_scout_candidates.json"
    if not os.path.exists(in_path):
        print(f"[{datetime.now()}] ⚠️ No candidate file found at {in_path}. Did the Intelligence Scout run?")
        return
        
    with open(in_path, "r") as f:
        symbols = json.load(f)
        
    print(f"[{datetime.now()}] 📥 Loaded {len(symbols)} candidates from {in_path} for debate.")
    
    if not symbols:
        print("No candidates found by Intelligence Scout.")
        return
        
    orchestrator = DebateOrchestrator()
    
    for sym in symbols:
        result = orchestrator.run_debate(sym)
        if result:
            score = result["score"]
            if score >= 90:
                print(f"✅ {sym} passed the debate with high conviction! Adding to Anticipatory Watchlist.")
                add_anticipatory_stock(sym, result["bull_thesis"], result["bear_thesis"], score)
            else:
                print(f"❌ {sym} failed the debate (Score {score} < 90). Rejected.")
        
        time.sleep(3) # Rate limit

if __name__ == "__main__":
    run_daily_debate()
