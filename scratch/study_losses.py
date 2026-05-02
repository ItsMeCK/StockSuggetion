import json
import os
from collections import Counter

def study_losses():
    if not os.path.exists("backtest_results.json"):
        print("Results file not found.")
        return
        
    with open("backtest_results.json", "r") as f:
        results = json.load(f)
        
    loss_trades = []
    for day in results:
        date = day["date"]
        regime = day["market_regime"]
        agent_scores = day.get("agent_scores", {})
        
        for trade in day["trades"]:
            if trade["outcome"] == "LOSS":
                symbol = trade["symbol"]
                scores = agent_scores.get(symbol, {})
                
                # Re-calculate total confidence
                w_v = scores.get("vision", 0) * 0.4
                w_e = scores.get("entry", 0) * 0.4
                w_d = scores.get("dtw", 0) * 0.2
                total = w_v + w_e + w_d
                
                loss_trades.append({
                    "date": date,
                    "symbol": symbol,
                    "regime": regime,
                    "total": total,
                })
                
    if not loss_trades:
        print("No loss trades found to study.")
        return
        
    print(f"Total Loss Trades: {len(loss_trades)}")
    
    saved_by_80 = [t for t in loss_trades if t["total"] < 80]
    saved_by_85 = [t for t in loss_trades if t["total"] < 85]
    saved_by_110 = [t for t in loss_trades if t["total"] < 110]
    
    print("\n--- HYPOTHETICAL ANALYSIS ---")
    print(f"Losses saved if hurdle was 80: {len(saved_by_80)} ({len(saved_by_80)/len(loss_trades)*100:.1f}%)")
    print(f"Losses saved if hurdle was 85: {len(saved_by_85)} ({len(saved_by_85)/len(loss_trades)*100:.1f}%)")
    print(f"Losses saved if hurdle was 110 (BEARISH ELITE): {len(saved_by_110)} ({len(saved_by_110)/len(loss_trades)*100:.1f}%)")

if __name__ == "__main__":
    study_losses()
