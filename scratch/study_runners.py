import json
import os
from collections import Counter

def study_runners():
    if not os.path.exists("backtest_results.json"):
        print("Results file not found.")
        return
        
    with open("backtest_results.json", "r") as f:
        results = json.load(f)
        
    runners = []
    for day in results:
        agent_scores = day.get("agent_scores", {})
        for trade in day["trades"]:
            if trade["pnl"] >= 20.0:
                symbol = trade["symbol"]
                scores = agent_scores.get(symbol, {})
                w_v = scores.get("vision", 0) * 0.4
                w_e = scores.get("entry", 0) * 0.4
                w_d = scores.get("dtw", 0) * 0.2
                total = w_v + w_e + w_d
                
                runners.append({
                    "symbol": symbol,
                    "pnl": trade["pnl"],
                    "total_score": total,
                    "scores": scores,
                    "regime": day["market_regime"]
                })
                
    if not runners:
        print("No 20%+ runners found in this dataset.")
        return
        
    print(f"Total 20%+ Runners Found: {len(runners)}")
    
    rejected_runners = [r for r in runners if r["total_score"] < 85]
    print(f"Runners rejected by 85-hurdle: {len(rejected_runners)}")
    
    print("\n--- DNA of REJECTED RUNNERS (< 85 Score) ---")
    
    # 1. Check average component scores
    avg_vision = sum(r["scores"].get("vision", 0) for r in rejected_runners) / len(rejected_runners)
    avg_entry = sum(r["scores"].get("entry", 0) for r in rejected_runners) / len(rejected_runners)
    avg_dtw = sum(r["scores"].get("dtw", 0) for r in rejected_runners) / len(rejected_runners)
    
    print(f"Avg Vision: {avg_vision:.1f}")
    print(f"Avg Entry: {avg_entry:.1f}")
    print(f"Avg DTW: {avg_dtw:.1f}")
    
    # 2. Check Regimes
    regimes = Counter(r["regime"] for r in rejected_runners)
    print(f"Regime Distribution: {dict(regimes)}")

    # 3. Check for specific "Anchor" scores
    high_dtw = [r for r in rejected_runners if r["scores"].get("dtw", 0) >= 85]
    print(f"Runners with High DTW (>=85): {len(high_dtw)}")

if __name__ == "__main__":
    study_runners()
