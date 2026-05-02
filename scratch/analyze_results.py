import json
import os

def analyze():
    if not os.path.exists("backtest_results.json"):
        print("Results file not found.")
        return
        
    with open("backtest_results.json", "r") as f:
        results = json.load(f)
        
    all_trades = [t for r in results for t in r["trades"]]
    if not all_trades:
        print("No trades found.")
        return
        
    profits = [t for t in all_trades if t["outcome"] == "PROFIT"]
    losses = [t for t in all_trades if t["outcome"] == "LOSS"]
    open_trades = [t for t in all_trades if t["outcome"] == "OPEN"]
    
    print(f"Total Trades: {len(all_trades)}")
    print(f"Closed - Profit: {len(profits)}")
    print(f"Closed - Loss: {len(losses)}")
    print(f"Open: {len(open_trades)}")
    
    if open_trades:
        open_pnls = [t["pnl"] for t in open_trades]
        pos_open = [p for p in open_pnls if p > 0]
        neg_open = [p for p in open_pnls if p <= 0]
        
        print(f"\nOpen Trades Analysis:")
        print(f"Positive PnL: {len(pos_open)} ({len(pos_open)/len(open_trades)*100:.1f}%)")
        print(f"Negative PnL: {len(neg_open)} ({len(neg_open)/len(open_trades)*100:.1f}%)")
        print(f"Average PnL: {sum(open_pnls)/len(open_trades):.2f}%")
        if pos_open:
            print(f"Avg Positive: {sum(pos_open)/len(pos_open):.2f}%")
        if neg_open:
            print(f"Avg Negative: {sum(neg_open)/len(neg_open):.2f}%")

    total_profitable = len(profits) + len([p for p in open_trades if p["pnl"] > 0])
    print(f"\nHypothetical Win Rate (Profits + Positive Open): {total_profitable/len(all_trades)*100:.1f}%")

if __name__ == "__main__":
    analyze()
