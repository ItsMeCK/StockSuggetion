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
                
                # Re-calculate total confidence if needed, or just take it from scores
                w_v = scores.get("vision", 0) * 0.4
                w_e = scores.get("entry", 0) * 0.4
                w_d = scores.get("dtw", 0) * 0.2
                total = w_v + w_e + w_d
                
                loss_trades.append({
                    "date": date,
                    "symbol": symbol,
                    "regime": regime,
                    "vision": scores.get("vision"),
                    "entry": scores.get("entry"),
                    "dtw": scores.get("dtw"),
                    "total": total,
                    "pnl": trade["pnl"]
                })
                
    if not loss_trades:
        print("No loss trades found to study.")
        return
        
    print(f"Total Loss Trades: {len(loss_trades)}")
    
    # Analysis 1: Regime Distribution
    regime_counts = Counter([t["regime"] for t in loss_trades])
    print("\nRegime Distribution of Losses:")
    for r, count in regime_counts.items():
        print(f"{r}: {count} ({count/len(loss_trades)*100:.1f}%)")
        
    # Analysis 2: Average Scores
    avg_vision = sum(t["vision"] for t in loss_trades) / len(loss_trades)
    avg_entry = sum(t["entry"] for t in loss_trades) / len(loss_trades)
    avg_total = sum(t["total"] for t in loss_trades) / len(loss_trades)
    
    print("\nAverage Scores of Loss Trades:")
    print(f"Avg Vision: {avg_vision:.2f}")
    print(f"Avg Entry: {avg_entry:.2f}")
    print(f"Avg Total: {avg_total:.2f}")
    
    # Analysis 3: Identifying "Weak" approved trades
    weak_approved = [t for t in loss_trades if t["total"] < 80]
    print(f"\nLosses with Total Score < 80: {len(weak_approved)} ({len(weak_approved)/len(loss_trades)*100:.1f}%)")
    
    # Analysis 4: Top Loser Symbols
    symbol_counts = Counter([t["symbol"] for t in loss_trades])
    print("\nRepeat Loser Symbols:")
    for s, count in symbol_counts.most_common(10):
        print(f"{s}: {count}")

if __name__ == "__main__":
    study_losses()
