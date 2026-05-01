import json
import os

def analyze_failures():
    results_path = "backtest_results.json"
    if not os.path.exists(results_path):
        return

    with open(results_path, "r") as f:
        data = json.load(f)

    feb_trades = []
    march_trades = []
    
    for day in data:
        date = day["date"]
        for t in day.get("trades", []):
            trade_info = {
                "date": date,
                "symbol": t["symbol"],
                "outcome": t["outcome"],
                "pnl": t["pnl"]
            }
            if "-02-" in date:
                feb_trades.append(trade_info)
            elif "-03-" in date:
                march_trades.append(trade_info)

    def stats(trades, month_name):
        profits = [t for t in trades if t["outcome"] == "PROFIT"]
        losses = [t for t in trades if t["outcome"] == "LOSS"]
        total = len(trades)
        win_rate = (len(profits) / (len(profits) + len(losses)) * 100) if (len(profits) + len(losses)) > 0 else 0
        net_pnl = sum([t["pnl"] for t in trades])
        print(f"--- {month_name} Analysis ---")
        print(f"Total Trades: {total}")
        print(f"Win Rate: {win_rate:.1f}%")
        print(f"Net PnL: {net_pnl:.1f}%")
        print(f"Losses: {len(losses)}")
        if losses:
            print("First 5 Losses:")
            for l in losses[:5]:
                print(f"  {l['date']}: {l['symbol']}")

    stats(feb_trades, "February")
    stats(march_trades, "March")

if __name__ == "__main__":
    analyze_failures()
