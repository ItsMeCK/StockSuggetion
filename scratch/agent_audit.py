import re

def analyze_agent_performance():
    with open("docs/exact_weekly_pnl.md", "r") as f:
        lines = f.readlines()
        
    stats = {
        "🚀 Momentum": {"trades": 0, "wins": 0, "losses": 0, "active": 0, "total_pnl": 0.0},
        "🛡️ Standard": {"trades": 0, "wins": 0, "losses": 0, "active": 0, "total_pnl": 0.0}
    }
    
    for line in lines:
        if "Symbol" in line or ":---" in line or "NO_DATA" in line:
            continue
        parts = [p.strip() for p in line.split("|") if p.strip()]
        if len(parts) >= 6:
            agent = "🚀 Momentum" if "Momentum" in parts[2] else "🛡️ Standard"
            status = parts[4]
            pnl_str = parts[5].replace('%', '').replace('+', '')
            
            try:
                pnl = float(pnl_str)
                stats[agent]["trades"] += 1
                
                if "ACTIVE" in status:
                    stats[agent]["active"] += 1
                else:
                    stats[agent]["total_pnl"] += pnl
                    if "SL (-5%)" in status or pnl < 0:
                        stats[agent]["losses"] += 1
                    else:
                        stats[agent]["wins"] += 1
            except ValueError:
                pass
                
    print("# 📊 AGENT PERFORMANCE BREAKDOWN\n")
    print("| Agent | Total Trades | Wins | Losses | Active | Win Rate | Avg P&L | Total P&L% |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    
    for agent, data in stats.items():
        total_closed = data["wins"] + data["losses"]
        win_rate = (data["wins"] / total_closed * 100) if total_closed > 0 else 0
        avg_pnl = (data["total_pnl"] / total_closed) if total_closed > 0 else 0
        
        print(f"| {agent} | {data['trades']} | {data['wins']} | {data['losses']} | {data['active']} | {win_rate:.2f}% | {avg_pnl:.2f}% | {data['total_pnl']:.2f}% |")

if __name__ == "__main__":
    analyze_agent_performance()
