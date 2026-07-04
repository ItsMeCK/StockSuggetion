import os
import logging
from dotenv import load_dotenv

load_dotenv()

from pipeline.catalyst_screener import EventCatalystScreener

logging.basicConfig(level=logging.INFO)

def test_corporate_calendar():
    print("Initializing EventCatalystScreener for Corporate Calendar Test...")
    screener = EventCatalystScreener()
    
    print("\nRunning screener for Tuesday, May 26, 2026 (target_date='2026-05-26')...")
    res = screener.run(target_date="2026-05-26")
    
    print("\n=== TEST RESULTS ===")
    print("Injected Catalyst Stocks:", res)
    print("Verification:")
    if "CUMMINSIND" in res:
        print(" -> SUCCESS: CUMMINSIND board meeting catalyst successfully detected and stock injected!")
    else:
        print(" -> FAILURE: CUMMINSIND board meeting catalyst NOT detected. Check log and API responses above.")

if __name__ == "__main__":
    test_corporate_calendar()
