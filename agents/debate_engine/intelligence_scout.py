import os
import json
from datetime import datetime
from dotenv import load_dotenv
from google import genai
from google.genai import types

class IntelligenceScout:
    def __init__(self):
        load_dotenv()
        self.api_key = os.getenv("GEMINI_API_KEY")
        if not self.api_key:
            raise ValueError("GEMINI_API_KEY is not set.")
        self.client = genai.Client(api_key=self.api_key)
        self.model = "gemini-3.5-flash"
        
    def scout_market_catalysts(self):
        """
        Uses Google Web Grounding to perform a macro-level scan of the Indian stock market.
        Hunts for upcoming weekend earnings, massive business updates, and major analyst upgrades.
        """
        print(f"[{datetime.now()}] 🦅 Intelligence Scout initiating Macro Market Scan...")
        
        prompt = """
        You are an elite Institutional Intelligence Scout for the Indian Stock Market (NSE).
        Your job is to identify EXACTLY 5 FNO (Futures & Options) stocks that have MASSIVE, high-conviction catalysts impending in the next 1-3 days.
        
        Using Google Web Search, look for the following across top financial news sites (Moneycontrol, ET, Bloomberg, CNBC TV18):
        1. Companies reporting Q1/Q2/Q3 earnings THIS WEEKEND or TOMORROW where analysts expect a massive beat.
        2. Companies that just released explosive 'Provisional Business Updates' (e.g. loan growth up 20%, order book doubled).
        3. Companies that just received major target price upgrades from tier-1 brokerages (Bernstein, Morgan Stanley, ICICI Sec, etc.).
        
        CRITICAL RULES:
        - Only select stocks that are traded in the NSE FNO (Derivatives) segment (e.g. large/mid caps).
        - Filter out fake news or unverified Twitter rumors. Only trust tier-1 financial journalism.
        - The catalyst MUST be highly bullish.
        
        Return the result as a raw JSON array of strings containing only the NSE trading symbols.
        Example output: ["PAYTM", "POWERINDIA", "HINDALCO", "GRASIM", "NAUKRI"]
        Do not include markdown blocks or any other text.
        """
        
        config = types.GenerateContentConfig(
            temperature=0.2,
            tools=[{"google_search": {}}]
        )
        
        try:
            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config=config
            )
            
            raw_text = response.text.strip()
            
            # Web Grounding often appends citation metadata that breaks json.loads
            # Extract only the content between the first [ and last ]
            start_idx = raw_text.find('[')
            end_idx = raw_text.rfind(']')
            if start_idx != -1 and end_idx != -1 and end_idx > start_idx:
                raw_text = raw_text[start_idx:end_idx+1]
            else:
                print(f"❌ Failed to find JSON array in response: {raw_text}")
                return []

                
            symbols = json.loads(raw_text)
            print(f"✅ Intelligence Scout identified {len(symbols)} high-conviction catalysts: {symbols}")
            return symbols
            
        except Exception as e:
            print(f"❌ Intelligence Scout failed to query Gemini: {e}")
            return []

if __name__ == "__main__":
    scout = IntelligenceScout()
    symbols = scout.scout_market_catalysts()
    
    if symbols:
        os.makedirs("data", exist_ok=True)
        out_path = "data/daily_scout_candidates.json"
        with open(out_path, "w") as f:
            json.dump(symbols, f)
        print(f"[{datetime.now()}] 💾 Saved {len(symbols)} candidates to {out_path} for the Debate Engine.")
    else:
        print(f"[{datetime.now()}] ⚠️ No candidates found. Saved nothing.")
