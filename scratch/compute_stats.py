import re

def compute_stats():
    with open("docs/exact_weekly_pnl.md", "r") as f:
        lines = f.readlines()
        
    total_trades = 0
    wins = 0
    losses = 0
    total_pnl = 0.0
    active = 0
    
    for line in lines:
        if "Symbol" in line or ":---" in line or "NO_DATA" in line:
            continue
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) >= 6:
            status = parts[4]
            pnl_str = parts[5].replace('%', '').replace('+', '')
            try:
                pnl = float(pnl_str)
                total_trades += 1
                total_pnl += pnl
                
                if "SL (-5%)" in status:
                    losses += 1
                elif pnl > 0 and "ACTIVE" not in status:
                    wins += 1
                elif "ACTIVE" in status:
                    active += 1
                else:
                    losses += 1 # Time stop with negative PNL
                    
            except ValueError:
                pass
                
    win_rate = (wins / (wins + losses)) * 100 if (wins + losses) > 0 else 0
    
    print(f"Total Valid Trades: {total_trades}")
    print(f"Wins: {wins}")
    print(f"Losses: {losses}")
    print(f"Active (Open): {active}")
    print(f"Win Rate (Closed): {win_rate:.2f}%")
    print(f"Net P&L: {total_pnl:.2f}%")

if __name__ == "__main__":
    compute_stats()
