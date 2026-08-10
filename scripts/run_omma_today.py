import os
import sys

sys.path.append(os.getcwd())

from agents.option_momentum_mesh_agent import run_option_momentum_mesh_agent

def main():
    state = {
        "target_date": "2026-07-28",
        "approved_allocations": {},
        "candidates": [],
        "bearish_divergence_candidates": []
    }
    
    print("--- RUNNING OMMA FOR 2026-07-08 ---")
    res = run_option_momentum_mesh_agent(state)
    allocs = res.get("approved_allocations", {})
    print(f"Allocations triggered: {len(allocs)}")
    for sym, details in allocs.items():
        print(f"  - {sym}: Route: {details['route']}, Entry: {details['entry']}, Conviction: {details['conviction_score']}, Features: {details['signal_features']}")

if __name__ == "__main__":
    main()
