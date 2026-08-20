import asyncio
import sys
sys.path.append(".")
from agents.intraday_debate.intraday_orchestrator import IntradayDebateOrchestrator

def run_test():
    orchestrator = IntradayDebateOrchestrator()
    
    # Mocking a massively bullish scenario for a stock
    debate_candidates = [
        {
            "symbol": "HAL",
            "context": "Candle: GREEN, Wick: 0.5%, VWAP: ABOVE_VWAP, Vol Surge: 5.0x. Massive 50,000 Crore defense contract from the Indian Government announced 5 minutes ago for new Tejas fighter jets."
        }
    ]
    
    results = orchestrator.evaluate_all_sync(debate_candidates)
    
    print("\n--- TEST ORCHESTRATOR ---")
    for cand, res in zip(debate_candidates, results):
        print(f"Symbol: {cand['symbol']}")
        print(f"Score: {res.conviction_score}")
        print(f"Verdict: {res.arbiter_verdict}")
        print(f"Bull: {res.bull_thesis}")
        print(f"Bear: {res.bear_thesis}")

if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv()
    run_test()
