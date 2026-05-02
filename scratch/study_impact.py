import json
import os

def study_hurdle_impact():
    if not os.path.exists("backtest_results.json"):
        print("Results file not found.")
        return
        
    with open("backtest_results.json", "r") as f:
        results = json.load(f)
        
    all_trades = []
    total_days = 0
    trades_with_scores = 0
    trades_without_scores = 0
    
    for day in results:
        total_days += 1
        agent_scores = day.get("agent_scores", {})
        for trade in day.get("trades", []):
            symbol = trade["symbol"]
            scores = agent_scores.get(symbol)
            
            if not scores:
                trades_without_scores += 1
                continue
                
            trades_with_scores += 1
            w_v = scores.get("vision", 0) * 0.4
            w_e = scores.get("entry", 0) * 0.4
            w_d = scores.get("dtw", 0) * 0.2
            total = w_v + w_e + w_d
            
            all_trades.append({
                "symbol": symbol,
                "outcome": trade["outcome"],
                "pnl": trade["pnl"],
                "total": total
            })
            
    print(f"Summary:")
    print(f"- Total Days: {total_days}")
    print(f"- Trades with scores: {trades_with_scores}")
    print(f"- Trades without scores: {trades_without_scores}")
    
    if not all_trades:
        print("No valid trades to study.")
        return
        
    for hurdle in [80, 85, 110]:
        approved = [t for t in all_trades if t["total"] >= hurdle]
        rejected = [t for t in all_trades if t["total"] < hurdle]
        
        lost_losses = [t for t in rejected if t["outcome"] == "LOSS"]
        lost_profits = [t for t in rejected if (t["outcome"] == "PROFIT" or (t["outcome"] == "OPEN" and t["pnl"] > 0))]
        
        print(f"\n--- IMPACT OF {hurdle} HURDLE ---")
        print(f"Total Trades Allowed: {len(approved)} ({(len(approved)/len(all_trades))*100:.1f}%)")
        print(f"Losses Avoided: {len(lost_losses)}")
        print(f"Winners Lost: {len(lost_profits)}")
        
        if len(approved) > 0:
            remaining_wins = [t for t in approved if (t["outcome"] == "PROFIT" or (t["outcome"] == "OPEN" and t["pnl"] > 0))]
            win_rate = (len(remaining_wins)) / len(approved) * 100
            print(f"New Hypothetical Win Rate: {win_rate:.1f}%")

if __name__ == "__main__":
    study_hurdle_impact()
