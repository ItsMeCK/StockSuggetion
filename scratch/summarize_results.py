import json

def summarize():
    try:
        with open('backtest_results.json', 'r') as f:
            data = json.load(f)
    except Exception as e:
        print(f"Error loading results: {e}")
        return

    profits = 0
    losses = 0
    open_trades = 0
    total_trades = 0
    total_pnl = 0.0

    for day in data:
        for trade in day.get('trades', []):
            total_trades += 1
            if trade['outcome'] == 'PROFIT':
                profits += 1
            elif trade['outcome'] == 'LOSS':
                losses += 1
            elif trade['outcome'] == 'OPEN':
                open_trades += 1
            total_pnl += trade.get('pnl', 0.0)

    win_rate = (profits / (profits + losses) * 100) if (profits + losses) > 0 else 0
    avg_pnl = (total_pnl / total_trades) if total_trades > 0 else 0

    print(f"--- BACKTEST SUMMARY (FEB - APRIL) ---")
    print(f"Total Trading Days: {len(data)}")
    print(f"Total Trades: {total_trades}")
    print(f"Profits: {profits}")
    print(f"Losses: {losses}")
    print(f"Open: {open_trades}")
    print(f"Win Rate: {win_rate:.2f}%")
    print(f"Average PnL: {avg_pnl:.2f}%")
    print(f"Total PnL Sum: {total_pnl:.2f}%")

if __name__ == "__main__":
    summarize()
