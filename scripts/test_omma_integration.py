import os
import sys

sys.path.append(os.getcwd())

from core.state import SovereignState
from agents.option_momentum_mesh_agent import run_option_momentum_mesh_agent

def main():
    # 1. Test for July 2nd, 2026 (expected to catch ADANIENSOL and NAUKRI CE breakouts)
    state_july2 = {
        "target_date": "2026-07-02",
        "approved_allocations": {},
        "candidates": [],
        "bearish_divergence_candidates": []
    }
    
    print("--- RUNNING OMMA FOR 2026-07-02 ---")
    res_july2 = run_option_momentum_mesh_agent(state_july2)
    allocs = res_july2.get("approved_allocations", {})
    print(f"Allocations triggered: {len(allocs)}")
    for sym, details in allocs.items():
        print(f"  - {sym}: Route: {details['route']}, Entry Close: {details['entry']}, Conviction: {details['conviction_score']}")

    # 2. Test for July 6th, 2026 (expected to catch BANKINDIA PE breakdown)
    state_july6 = {
        "target_date": "2026-07-06",
        "approved_allocations": {},
        "candidates": [],
        "bearish_divergence_candidates": []
    }
    
    print("\n--- RUNNING OMMA FOR 2026-07-06 ---")
    res_july6 = run_option_momentum_mesh_agent(state_july6)
    allocs_j6 = res_july6.get("approved_allocations", {})
    print(f"Allocations triggered: {len(allocs_j6)}")
    for sym, details in allocs_j6.items():
        print(f"  - {sym}: Route: {details['route']}, Entry Close: {details['entry']}, Conviction: {details['conviction_score']}")

if __name__ == "__main__":
    main()
