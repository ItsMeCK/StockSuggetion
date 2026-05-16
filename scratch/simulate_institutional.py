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

def simulate_trade_institutional(symbol, signal_date, is_momentum=False):
    """
    Institutional Simulation:
    - Entry: Next day Open
    - Stop Loss: 5% (Intraday Low)
    - Momentum/Titan: No TP, 5% Trailing SL from Peak High.
    - Standard: 10% TP cap.
    - Time Stop: 2 Days.
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
    
    # Peak High Tracker for Trailing SL
    peak_high = d1_high
    
    # Day 1: Stop Loss check
    if (d1_low - entry_price) / entry_price <= -0.05:
        return {"entry_price": entry_price, "status": "🛑 SL (-5%)", "pnl": -5.0}
        
    # Day 1: Take Profit (Standard Only)
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
    
    # Day 2: Update Peak High
    peak_high = max(peak_high, d2_high)
    
    # Day 2: Trailing SL Check (for Momentum) or Standard SL
    if is_momentum:
        # Exit if drops 5% from peak high
        if (d2_low - peak_high) / peak_high <= -0.05:
            exit_price = peak_high * 0.95
            pnl = ((exit_price - entry_price) / entry_price) * 100
            return {"entry_price": entry_price, "status": "🛡️ TRAIL SL", "pnl": round(pnl, 2)}
    else:
        # Standard SL
        if (d2_low - entry_price) / entry_price <= -0.05:
            return {"entry_price": entry_price, "status": "🛑 SL (-5%)", "pnl": -5.0}
            
    # Day 2: Take Profit (Standard Only)
    if not is_momentum:
        if (d2_high - entry_price) / entry_price >= 0.10:
            return {"entry_price": entry_price, "status": "🎯 TP (+10%)", "pnl": 10.0}
            
    # Time Stop Exit (End of Day 2)
    pnl = ((d2_close - entry_price) / entry_price) * 100
    status = "⏳ TIME STOP" if not is_momentum else "🚀 RUNNER EXIT"
    return {"entry_price": entry_price, "status": status, "pnl": round(pnl, 2)}

def run():
    log_file = "weekly_report_data_Elite_Fixed.log"
    with open(log_file, "r") as f:
        log_content = f.readlines()

    rescued = set()
    approvals = []
    current_date = ""
    
    for line in log_content:
        if "HISTORICAL SIMULATION FOR" in line:
            match = re.search(r"HISTORICAL SIMULATION FOR (\d{4}-\d{2}-\d{2})", line)
            if match:
                current_date = match.group(1)
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
                        approvals.append((current_date, symbol, agent))

    print("# 🏦 INSTITUTIONAL SNIPER AUDIT (MAY 11-15)")
    print("## Rules: Titans @ ₹25k (No TP, Trailing SL) | Standard @ ₹5k")
    print("\n| Date | Symbol | Agent | Sizing | Status | PnL% | PnL (₹) |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    
    total_deployed = 0
    total_profit = 0
    active_trades = {} # symbol -> exit_date
    
    for date, sym, agent in approvals:
        # Simple virtual portfolio logic: only one trade per symbol at a time
        date_dt = datetime.strptime(date, "%Y-%m-%d")
        if sym in active_trades and date_dt < active_trades[sym]:
            continue
            
        is_mom = (agent == "🚀 Momentum")
        result = simulate_trade_institutional(sym, date, is_momentum=is_mom)
        
        if result["status"] == "NO_DATA":
            continue
            
        allocation = 25000 if is_mom else 5000
        profit_rs = allocation * (result['pnl'] / 100)
        
        # Track active trades to prevent double counting (using a 2-day assumption)
        active_trades[sym] = date_dt + timedelta(days=2)
        
        pnl_str = f"+{result['pnl']}%" if result['pnl'] > 0 else f"{result['pnl']}%"
        profit_str = f"₹{profit_rs:,.0f}"
        
        dt_str = date_dt.strftime("%b %d")
        print(f"| {dt_str} | **{sym}** | {agent} | ₹{allocation/1000:.0f}k | {result['status']} | {pnl_str} | {profit_str} |")
        
        if "ACTIVE" not in result["status"]:
            total_deployed += allocation
            total_profit += profit_rs

    print(f"\n### 💰 Final Institutional Metrics")
    print(f"- **Total Capital Deployed (Closed)**: ₹{total_deployed:,.0f}")
    print(f"- **Net Profit Realized**: **₹{total_profit:,.0f}**")
    print(f"- **Absolute ROI**: **{(total_profit/total_deployed*100):.2f}%**")

if __name__ == "__main__":
    run()
