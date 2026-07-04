import os
import logging
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
load_dotenv()

from agents.sector_macro_agent import SectorMacroAgent

logging.basicConfig(level=logging.INFO)

def check_headlines():
    agent = SectorMacroAgent()
    target_date = "2026-05-26"
    q = '("CUMMINSIND" OR "Cummins India") AND ("board meeting" OR "results" OR "earnings" OR "dividend" OR "conference call")'
    print(f"Query: {q}")
    headlines = agent.fetch_sector_headlines(q, target_date, lookback_days=14, max_results=50)
    for h in headlines:
        print(f" - {h}")

if __name__ == "__main__":
    check_headlines()
