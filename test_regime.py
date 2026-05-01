import os
import polars as pl
from midnight_sovereign.pipeline.screener import SovereignScreener
from dotenv import load_dotenv

load_dotenv()

def test_regime():
    screener = SovereignScreener()
    date = "2026-02-10"
    regime = screener.calculate_market_regime(date)
    print(f"Date: {date} | Result: {regime}")

if __name__ == "__main__":
    test_regime()
