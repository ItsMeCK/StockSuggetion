import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Add workspace root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

# Set TRADING_MODE to HISTORICAL to bypass freshness check
os.environ["TRADING_MODE"] = "HISTORICAL"

from core.state import SovereignState
from graph.builder import build_sovereign_graph

def test_flow():
    # 1. Compile the graph
    app = build_sovereign_graph({})
    
    # 2. Build initial state
    initial_state = SovereignState(
        target_date="2026-05-20",  # Yesterday's date
        pulse=3,
        macro_regime="BULLISH",
        candidates=[],
        incubator=[],
        flagged_momentum_candidates=["APOLLOHOSP", "HONAUT"],  # Test symbols
        breakouts=[],
        base_scores={
            "APOLLOHOSP": 65.0,
            "HONAUT": 60.0
        },
        heuristic_flags={},
        experience_warnings={},
        vision_validations={},
        news_catalysts={},
        approved_allocations={},
        execution_telemetry={},
        error_log=[],
        debate_count=0
    )
    
    print("--- INITIATING COGNITIVE PIPELINE TEST FOR STEALTH CATALYSTS ---")
    final_state = app.invoke(initial_state)
    
    print("\n==========================================")
    print("INTEGRATION TEST RESULTS")
    print("==========================================")
    
    print(f"Candidates list after Momentum agent: {final_state.get('candidates')}")
    print("\nDetected News Catalysts:")
    for ticker, cat in final_state.get("news_catalysts", {}).items():
        print(f" - {ticker}: {cat}")
        
    print("\nCritic Results:")
    for ticker, res in final_state.get("critic_results", {}).items():
        print(f" - {ticker}: approved={res.get('approved')}, final_score={res.get('total_confidence')}, veto={res.get('veto')}, reason={res.get('veto_reason')}")
        
    print("\nApproved Allocations:")
    for ticker, alloc in final_state.get("approved_allocations", {}).items():
        print(f" - {ticker}: {alloc}")

if __name__ == "__main__":
    test_flow()
