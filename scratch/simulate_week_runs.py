import os
import logging
from dotenv import load_dotenv

load_dotenv()
os.environ["TRADING_MODE"] = "HISTORICAL"

logging.basicConfig(level=logging.WARNING) # Mute verbose logging

from run_historical import run_historical_engine

def simulate_week():
    dates = ["2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04"]
    results = []
    
    for d in dates:
        print(f"\n==========================================")
        print(f"RUNNING HISTORICAL SIMULATION FOR {d}...")
        print(f"==========================================")
        try:
            res = run_historical_engine(d)
            results.append(res)
        except Exception as e:
            print(f"Failed to run simulation for {d}: {e}")
            
    print("\n\n=== WEEKLY 3 PM RUNS SUMMARY ===")
    print("| Date | Macro Regime | Screened Candidates | Approved Allocations (F&O / Equity) |")
    print("|------|--------------|---------------------|-------------------------------------|")
    for r in results:
        candidates_str = ", ".join(r["candidates"]) if r["candidates"] else "None"
        approved_str = ", ".join(r["approved"]) if r["approved"] else "None"
        print(f"| {r['date']} | {r['macro_regime']} | {candidates_str} | {approved_str} |")

if __name__ == "__main__":
    simulate_week()
