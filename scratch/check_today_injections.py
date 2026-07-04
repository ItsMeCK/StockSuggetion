import os
import logging
from dotenv import load_dotenv

load_dotenv()

from pipeline.catalyst_screener import EventCatalystScreener

logging.basicConfig(level=logging.INFO)

def check_today():
    print("Running screener for Wednesday, May 27, 2026...")
    screener = EventCatalystScreener()
    res = screener.run(target_date="2026-05-27")
    print("\n=== WEDNESDAY'S INJECTED SYMBOLS ===")
    print(res)

if __name__ == "__main__":
    check_today()
