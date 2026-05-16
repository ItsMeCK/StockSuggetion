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

def simulate_trade(symbol, signal_date):
    """
    Simulates a trade based on strict rules:
    - Entry: Next day Open
    - Stop Loss: 5% (Intraday Low)
    - Take Profit: 10% (Intraday High)
    - Time Stop: 2 Days (Close)
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get the next 2 trading days after the signal date
    query = f"""
        SELECT time, open, high, low, close 
        FROM daily_ohlcv 
        WHERE symbol = '{symbol}' AND time::date >= '{signal_date}'
        ORDER BY time ASC LIMIT 3
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    
    # Filter out the signal day itself (since its time::date might be the day before in UTC)
    # The true "next day" is the first row where the date is strictly after the signal_date logically,
    # but since time is stored as 18:30 UTC, time::date is usually signal_date - 1.
    # To be safe, we fetch 3 and filter in Python.
    valid_rows = []
    signal_date_dt = datetime.strptime(signal_date, "%Y-%m-%d").date()
    
    for r in rows:
        # UTC to IST conversion roughly: add 5.5 hours
        ist_time = r[0] + timedelta(hours=5, minutes=30)
        if ist_time.date() > signal_date_dt:
            valid_rows.append(r)
            
    if not valid_rows:
        return {"entry_price": 0.0, "status": "NO_DATA", "pnl": 0.0}
        
    day_1 = valid_rows[0]
    entry_price = float(day_1[1]) # Next day Open
    
    # Day 1 checks
    d1_low = float(day_1[3])
    d1_high = float(day_1[2])
    d1_close = float(day_1[4])
    
    if (d1_low - entry_price) / entry_price <= -0.05:
        return {"entry_price": entry_price, "status": "🛑 SL (-5%)", "pnl": -5.0}
        
    if (d1_high - entry_price) / entry_price >= 0.10:
        return {"entry_price": entry_price, "status": "🎯 TP (+10%)", "pnl": 10.0}
        
    if len(valid_rows) < 2:
        # Held after 1 day (or data missing for day 2)
        pnl = ((d1_close - entry_price) / entry_price) * 100
        return {"entry_price": entry_price, "status": "🔍 ACTIVE", "pnl": round(pnl, 2)}
        
    day_2 = valid_rows[1]
    d2_low = float(day_2[3])
    d2_high = float(day_2[2])
    d2_close = float(day_2[4])
    
    # Day 2 checks
    if (d2_low - entry_price) / entry_price <= -0.05:
        return {"entry_price": entry_price, "status": "🛑 SL (-5%)", "pnl": -5.0}
        
    if (d2_high - entry_price) / entry_price >= 0.10:
        return {"entry_price": entry_price, "status": "🎯 TP (+10%)", "pnl": 10.0}
        
    # Time Stop Exit
    pnl = ((d2_close - entry_price) / entry_price) * 100
    return {"entry_price": entry_price, "status": "⏳ TIME STOP (2 Days)", "pnl": round(pnl, 2)}

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

    print("| Signal Date | Symbol | Agent | Entry Price | Status | Total PnL% |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- |")
    
    for date, sym, agent in approvals:
        result = simulate_trade(sym, date)
        
        # Don't print empty data to keep it clean unless requested
        if result["status"] == "NO_DATA":
            continue
            
        pnl_str = f"+{result['pnl']}%" if result['pnl'] > 0 else f"{result['pnl']}%"
        dt = datetime.strptime(date, "%Y-%m-%d").strftime("%b %d")
        
        print(f"| {dt} | **{sym}** | {agent} | {result['entry_price']:.2f} | {result['status']} | {pnl_str} |")

if __name__ == "__main__":
    run()
