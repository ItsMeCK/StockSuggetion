import math

def compute_stats():
    with open("docs/exact_weekly_pnl.md", "r") as f:
        lines = f.readlines()
        
    total_trades = 0
    wins = 0
    losses = 0
    total_pnl_rs = 0.0
    active = 0
    total_capital_deployed = 0.0
    
    for line in lines:
        if "Symbol" in line or ":---" in line or "NO_DATA" in line:
            continue
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) >= 6:
            entry_str = parts[3]
            status = parts[4]
            pnl_str = parts[5].replace('%', '').replace('+', '')
            try:
                entry_price = float(entry_str)
                pnl_pct = float(pnl_str)
                
                # Allocation Logic
                if entry_price > 5000:
                    shares = 1
                else:
                    shares = math.floor(5000 / entry_price)
                
                invested = shares * entry_price
                trade_pnl_rs = invested * (pnl_pct / 100)
                
                total_trades += 1
                
                if "ACTIVE" in status:
                    active += 1
                else:
                    total_pnl_rs += trade_pnl_rs
                    total_capital_deployed += invested
                    
                    if "SL (-5%)" in status or pnl_pct < 0:
                        losses += 1
                    else:
                        wins += 1
                    
            except ValueError:
                pass
                
    win_rate = (wins / (wins + losses)) * 100 if (wins + losses) > 0 else 0
    
    print(f"Total Valid Trades: {total_trades}")
    print(f"Wins: {wins}")
    print(f"Losses: {losses}")
    print(f"Active (Open): {active}")
    print(f"Win Rate (Closed Trades): {win_rate:.2f}%")
    print(f"Total Capital Deployed (Closed Trades): ₹{total_capital_deployed:,.2f}")
    print(f"Net P&L (Rupees): ₹{total_pnl_rs:,.2f}")

if __name__ == "__main__":
    compute_stats()
