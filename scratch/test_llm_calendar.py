import os
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

from agents.sector_macro_agent import SectorMacroAgent

logging.basicConfig(level=logging.INFO)

def test_llm():
    agent = SectorMacroAgent()
    target_date = "2026-05-26"
    
    # Run the headlines fetching logic exactly like in SectorMacroAgent
    general_query = '("board meeting to consider" OR "results date" OR "earnings board meeting" OR "dividend") stock India'
    general_headlines = agent.fetch_sector_headlines(general_query, target_date, lookback_days=14, max_results=80)
    
    watchlist_symbols = ["CUMMINSIND", "WIPRO", "VBL", "INDIGO", "COALINDIA", "SAIL", "VEDL", "RELIANCE", "HDFCBANK", "SBIN", "TITAGARH", "JWL", "AXISBANK", "EXIDEIND", "CGPOWER", "POWERINDIA", "DALBHARAT", "SUZLON"]
    
    import concurrent.futures
    def fetch_for_symbol(symbol):
        friendly_names = {
            "CUMMINSIND": '("CUMMINSIND" OR "Cummins India")',
            "WIPRO": '("WIPRO" OR "Wipro")',
            "VBL": '("VBL" OR "Varun Beverages")',
            "INDIGO": '("INDIGO" OR "InterGlobe Aviation" OR "IndiGo")',
            "COALINDIA": '("COALINDIA" OR "Coal India")',
            "SAIL": '("SAIL" OR "Steel Authority")',
            "VEDL": '("VEDL" OR "Vedanta")',
            "RELIANCE": '("RELIANCE" OR "Reliance")',
            "HDFCBANK": '("HDFCBANK" OR "HDFC Bank")',
            "SBIN": '("SBIN" OR "State Bank of India" OR "SBI")',
            "TITAGARH": '("TITAGARH" OR "Titagarh Rail")',
            "JWL": '("JWL" OR "Jupiter Wagons")',
            "AXISBANK": '("AXISBANK" OR "Axis Bank")',
            "EXIDEIND": '("EXIDEIND" OR "Exide Industries")',
            "CGPOWER": '("CGPOWER" OR "CG Power")',
            "POWERINDIA": '("POWERINDIA" OR "Hitachi Energy")',
            "DALBHARAT": '("DALBHARAT" OR "Dalmia Bharat")',
            "SUZLON": '("SUZLON" OR "Suzlon Energy")'
        }
        term = friendly_names.get(symbol, f'"{symbol}"')
        query = f'{term} AND ("board meeting" OR "results" OR "earnings" OR "dividend" OR "conference call")'
        return agent.fetch_sector_headlines(query, target_date, lookback_days=14, max_results=30)
        
    targeted_headlines = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(fetch_for_symbol, sym) for sym in watchlist_symbols]
        for future in concurrent.futures.as_completed(futures):
            targeted_headlines.extend(future.result())
            
    headlines = list(set(general_headlines + targeted_headlines))
    
    print(f"\nTotal headlines: {len(headlines)}")
    print("Does CUMMINSIND or Cummins exist in headlines?")
    cummins_headlines = [h for h in headlines if "cummins" in h.lower()]
    print(f"Count: {len(cummins_headlines)}")
    for h in cummins_headlines:
        print(f" -> {h}")
        
    # Let's call the LLM and see the response!
    headlines_text = "\n".join([f"- {h}" for h in headlines])
    prompt = f"""
    Analyze the following Indian stock market news headlines to identify companies that have officially scheduled 
    an upcoming Board Meeting (typically to consider financial results, earnings, or dividends, or scheduled results conference call).
    
    Extract the uppercase NSE stock symbol for each identified company.
    
    --- UNIVERSE OF SYMBOLS ---
    {", ".join(agent.symbols[:150])} ... [Additional standard NSE symbols include: TITAGARH, JWL, ARE&M, EMMVEE, HFCL, ADANIPOWER, WIPRO, VBL, ONGC, HINDCOPPER, CUMMINSIND, EXIDEIND, CGPOWER]
    
    --- NEWS HEADLINES ---
    {headlines_text}
    
    Return a JSON object:
    - "meetings": A list of objects, each containing:
      - "company": The name of the company
      - "symbol": The uppercase NSE symbol from the universe (e.g. "CUMMINSIND")
      - "event_date": The scheduled date of the board meeting or results conference call if mentioned, or "UNKNOWN"
      - "catalyst_type": "EARNINGS" or "DIVIDEND" or "CORPORATE_ACTION"
    """
    
    response = agent.client.chat.completions.create(
        model=agent.model,
        messages=[{"role": "user", "content": prompt}],
        response_format={"type": "json_object"}
    )
    import json
    result = json.loads(response.choices[0].message.content)
    print("\n--- LLM RESULT ---")
    print(json.dumps(result, indent=2))

if __name__ == "__main__":
    test_llm()
