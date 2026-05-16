import psycopg2
import os
import re
from datetime import datetime, timedelta

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )

def simulate_trade_titan_heavy(symbol, signal_date, is_momentum=False):
    """
    Titan-Heavy Simulation:
    - Titans: ₹50k, Trailing SL 5%, No TP.
    - Standard: ₹5k, 10% TP.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    query = f"""
        SELECT time, open, high, low, close 
        FROM daily_ohlcv 
        WHERE symbol = '{symbol}' AND time::date >= '{signal_date}'
        ORDER BY time ASC LIMIT 3
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    
    valid_rows = []
    signal_date_dt = datetime.strptime(signal_date, "%Y-%m-%d").date()
    
    for r in rows:
        ist_time = r[0] + timedelta(hours=5, minutes=30)
        if ist_time.date() > signal_date_dt:
            valid_rows.append(r)
            
    if not valid_rows:
        return {"entry_price": 0.0, "status": "NO_DATA", "pnl": 0.0}
        
    day_1 = valid_rows[0]
    entry_price = float(day_1[1])
    d1_low = float(day_1[3])
    d1_high = float(day_1[2])
    d1_close = float(day_1[4])
    
    peak_high = d1_high
    
    # Day 1 checks
    if (d1_low - entry_price) / entry_price <= -0.05:
        return {"entry_price": entry_price, "status": "🛑 SL (-5%)", "pnl": -5.0}
        
    if not is_momentum:
        if (d1_high - entry_price) / entry_price >= 0.10:
            return {"entry_price": entry_price, "status": "🎯 TP (+10%)", "pnl": 10.0}
    
    if len(valid_rows) < 2:
        pnl = ((d1_close - entry_price) / entry_price) * 100
        return {"entry_price": entry_price, "status": "🔍 ACTIVE", "pnl": round(pnl, 2)}
        
    day_2 = valid_rows[1]
    d2_low = float(day_2[3])
    d2_high = float(day_2[2])
    d2_close = float(day_2[4])
    
    peak_high = max(peak_high, d2_high)
    
    if is_momentum:
        if (d2_low - peak_high) / peak_high <= -0.05:
            exit_price = peak_high * 0.95
            pnl = ((exit_price - entry_price) / entry_price) * 100
            return {"entry_price": entry_price, "status": "🛡️ TRAIL SL", "pnl": round(pnl, 2)}
    else:
        if (d2_low - entry_price) / entry_price <= -0.05:
            return {"entry_price": entry_price, "status": "🛑 SL (-5%)", "pnl": -5.0}
            
    if not is_momentum:
        if (d2_high - entry_price) / entry_price >= 0.10:
            return {"entry_price": entry_price, "status": "🎯 TP (+10%)", "pnl": 10.0}
            
    pnl = ((d2_close - entry_price) / entry_price) * 100
    status = "🚀 RUNNER EXIT" if is_momentum else "⏳ TIME STOP"
    return {"entry_price": entry_price, "status": status, "pnl": round(pnl, 2)}

def run():
    log_file = "weekly_report_data_Pure_Intraday.log"
    with open(log_file, "r") as f:
        log_content = f.readlines()

    rescued = set()
    approvals_per_day = {} # date -> list of (sym, agent)
    current_date = ""
    
    for line in log_content:
        if "HISTORICAL SIMULATION FOR" in line:
            match = re.search(r"HISTORICAL SIMULATION FOR (\d{4}-\d{2}-\d{2})", line)
            if match:
                current_date = match.group(1)
                if current_date not in approvals_per_day:
                    approvals_per_day[current_date] = []
        
        rescue_match = re.search(r"RESCUED: (\w+) \(", line)
        if rescue_match:
            symbol = rescue_match.group(1)
            rescued.add((current_date, symbol))
            
        alloc_match = re.search(r"Approved Allocations: \[(.*?)\]", line)
        if alloc_match:
            symbols_str = alloc_match.group(1)
            if symbols_str:
                symbols = [s.strip(" '") for s in symbols_str.split(",")]
                for symbol in symbols:
                    if symbol:
                        agent = "🚀 Momentum" if (current_date, symbol) in rescued else "🛡️ Standard"
                        approvals_per_day[current_date].append((symbol, agent))

    print("# 🛡️ SOVEREIGN PURE: TITAN-HEAVY SIMULATION (MAY 11-15)")
    print("## Strategy: Titans @ ₹50k (Trailing SL) | Top 2 Standard @ ₹5k")
    print("\n| Date | Symbol | Agent | Sizing | Status | PnL% | PnL (₹) |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    
    total_deployed = 0
    total_profit = 0
    portfolio = set() # (symbol, expiry_date)
    
    for date in sorted(approvals_per_day.keys()):
        date_dt = datetime.strptime(date, "%Y-%m-%d")
        day_approvals = approvals_per_day[date]
        
        # Split into Momentum and Standard
        moms = [a for a in day_approvals if a[1] == "🚀 Momentum"]
        stds = [a for a in day_approvals if a[1] == "🛡️ Standard"]
        
        # APPLY TOP 2 CAP TO STANDARD ONLY
        selected_stds = stds[:2]
        
        # TOTAL ELITE LIST: All Momentum + Top 2 Standard
        elite_list = moms + selected_stds
        
        for sym, agent in elite_list:
            # Check if symbol is already active (crude check)
            # In a real sim, we'd check against actual exit dates, but here we assume 2 days
            if any(p[0] == sym and date_dt < p[1] for p in portfolio):
                continue
                
            is_mom = (agent == "🚀 Momentum")
            result = simulate_trade_titan_heavy(sym, date, is_momentum=is_mom)
            
            if result["status"] == "NO_DATA":
                continue
                
            allocation = 50000 if is_mom else 5000
            profit_rs = allocation * (result['pnl'] / 100)
            
            # Record in portfolio
            portfolio.add((sym, date_dt + timedelta(days=2)))
            
            pnl_str = f"+{result['pnl']}%" if result['pnl'] > 0 else f"{result['pnl']}%"
            profit_str = f"₹{profit_rs:,.0f}"
            
            print(f"| {date_dt.strftime('%b %d')} | **{sym}** | {agent} | ₹{allocation/1000:.0f}k | {result['status']} | {pnl_str} | {profit_str} |")
            
            if "ACTIVE" not in result["status"]:
                total_deployed += allocation
                total_profit += profit_rs

    print(f"\n### 💰 Sovereign Pure Metrics")
    print(f"- **Total Capital Deployed (Closed)**: ₹{total_deployed:,.0f}")
    print(f"- **Net Profit Realized**: **₹{total_profit:,.0f}**")
    print(f"- **Absolute ROI**: **{(total_profit/total_deployed*100):.2f}%**")

if __name__ == "__main__":
    run()
