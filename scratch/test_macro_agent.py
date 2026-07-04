import os
import logging
from dotenv import load_dotenv

load_dotenv()

from pipeline.catalyst_screener import EventCatalystScreener

logging.basicConfig(level=logging.INFO)

def test_screener():
    print("Initializing EventCatalystScreener...")
    screener = EventCatalystScreener()
    
    print("\nRunning screener for Friday, May 22, 2026 (target_date='2026-05-22')...")
    res = screener.run(target_date="2026-05-22")
    
    print("\n=== TEST RESULTS ===")
    print("Injected Catalyst Stocks:", res)
    print("Verification:")
    if "TITAGARH" in res or "JWL" in res:
        print(" -> SUCCESS: Railway wagon catalysts successfully detected and stocks injected!")
    else:
        print(" -> WARNING: Railway wagon catalysts NOT detected. Check log and API responses above.")

if __name__ == "__main__":
    test_screener()
