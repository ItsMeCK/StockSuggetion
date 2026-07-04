import os
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()

from agents.sector_macro_agent import SectorMacroAgent

logging.basicConfig(level=logging.INFO)

def debug_calendar():
    agent = SectorMacroAgent()
    target_date = "2026-05-26"
    
    # 1. Run general query
    general_query = '("board meeting to consider" OR "results date" OR "earnings board meeting" OR "dividend") stock India'
    general_headlines = agent.fetch_sector_headlines(general_query, target_date, lookback_days=14, max_results=80)
    
    # 2. Run targeted query
    watchlist_symbols = ["CUMMINSIND", "WIPRO", "VBL", "INDIGO", "COALINDIA", "SAIL", "VEDL", "RELIANCE", "HDFCBANK", "SBIN", "TITAGARH", "JWL", "AXISBANK", "EXIDEIND", "CGPOWER", "POWERINDIA", "DALBHARAT", "SUZLON"]
    targeted_query = f'({" OR ".join([f'"{s}"' for s in watchlist_symbols])}) AND ("board meeting" OR "results" OR "earnings" OR "dividend" OR "conference call")'
    targeted_headlines = agent.fetch_sector_headlines(targeted_query, target_date, lookback_days=14, max_results=80)
    
    print("\n--- GENERAL HEADLINES ---")
    for h in general_headlines:
        if "cummins" in h.lower() or "cmi" in h.lower():
            print(f" MATCH: {h}")
        else:
            print(f" - {h}")
            
    print("\n--- TARGETED HEADLINES ---")
    for h in targeted_headlines:
        if "cummins" in h.lower() or "cmi" in h.lower():
            print(f" MATCH: {h}")
        else:
            print(f" - {h}")

if __name__ == "__main__":
    debug_calendar()
