import os
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# Options Segment trades and their signal dates
TRADES = [
    # May 21
    ("DIXON", "2026-05-21"),
    ("GRASIM", "2026-05-21"),
    ("REDINGTON", "2026-05-21"),
    # May 25
    ("INDIGO", "2026-05-25"),
    # May 26
    ("ADANIPOWER", "2026-05-26"),
    ("ATGL", "2026-05-26"),
    # May 29
    ("ADANIPOWER", "2026-05-29"),
    ("ATGL", "2026-05-29"),
    ("CUMMINSIND", "2026-05-29"),
    ("IDFCFIRSTB", "2026-05-29"),
    # June 1
    ("ASTRAL", "2026-06-01"),
    ("AXISBANK", "2026-06-01"),
    ("BALKRISIND", "2026-06-01"),
    ("FEDERALBNK", "2026-06-01"),
    ("GAIL", "2026-06-01"),
    ("IDBI", "2026-06-01"),
    ("LT", "2026-06-01"),
    ("LTF", "2026-06-01"),
    ("SIEMENS", "2026-06-01"),
    ("TECHM", "2026-06-01"),
    ("TRENT", "2026-06-01"),
    ("UPL", "2026-06-01"),
    # June 2
    ("FEDERALBNK", "2026-06-02"),
    ("DIXON", "2026-06-02"),
    # June 3
    ("CUMMINSIND", "2026-06-03"),
    ("BALKRISIND", "2026-06-03"),
    ("GAIL", "2026-06-03"),
    ("INDIGO", "2026-06-03"),
    # June 4
    ("BALKRISIND", "2026-06-04")
]

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
    Simulates a trade:
    - Entry: Next day Open
    - Stop Loss: 5% (Intraday Low)
    - Take Profit: 10% (Intraday High)
    - Time Stop: 2 Days (Close)
    """
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Get the next 3 trading days after the signal date to ensure we have enough data
    query = f"""
        SELECT time, open, high, low, close 
        FROM daily_ohlcv 
        WHERE symbol = '{symbol}' AND time::date >= '{signal_date}'
        ORDER BY time ASC LIMIT 4
    """
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    
    valid_rows = []
    signal_date_dt = datetime.strptime(signal_date, "%Y-%m-%d").date()
    
    for r in rows:
        # UTC to IST conversion: add 5.5 hours
        ist_time = r[0] + timedelta(hours=5, minutes=30)
        if ist_time.date() > signal_date_dt:
            valid_rows.append(r)
            
    if not valid_rows:
        return {"entry_price": 0.0, "status": "PENDING_ENTRY", "pnl": 0.0, "entry_date": None, "exit_date": None}
        
    day_1 = valid_rows[0]
    entry_price = float(day_1[1])  # Next day Open
    entry_date = (day_1[0] + timedelta(hours=5, minutes=30)).date().strftime("%b %d")
    
    # Day 1 checks
    d1_low = float(day_1[3])
    d1_high = float(day_1[2])
    d1_close = float(day_1[4])
    day_1_date = (day_1[0] + timedelta(hours=5, minutes=30)).date().strftime("%b %d")
    
    # Check stop loss first (conservative)
    if (d1_low - entry_price) / entry_price <= -0.05:
        return {"entry_price": entry_price, "status": "🛑 SL (-5%)", "pnl": -5.0, "entry_date": entry_date, "exit_date": day_1_date}
        
    if (d1_high - entry_price) / entry_price >= 0.10:
        return {"entry_price": entry_price, "status": "🎯 TP (+10%)", "pnl": 10.0, "entry_date": entry_date, "exit_date": day_1_date}
        
    if len(valid_rows) < 2:
        pnl = ((d1_close - entry_price) / entry_price) * 100
        return {"entry_price": entry_price, "status": "🔍 ACTIVE (1 Day)", "pnl": round(pnl, 2), "entry_date": entry_date, "exit_date": day_1_date}
        
    day_2 = valid_rows[1]
    d2_low = float(day_2[3])
    d2_high = float(day_2[2])
    d2_close = float(day_2[4])
    day_2_date = (day_2[0] + timedelta(hours=5, minutes=30)).date().strftime("%b %d")
    
    # Day 2 checks
    if (d2_low - entry_price) / entry_price <= -0.05:
        return {"entry_price": entry_price, "status": "🛑 SL (-5%)", "pnl": -5.0, "entry_date": entry_date, "exit_date": day_2_date}
        
    if (d2_high - entry_price) / entry_price >= 0.10:
        return {"entry_price": entry_price, "status": "🎯 TP (+10%)", "pnl": 10.0, "entry_date": entry_date, "exit_date": day_2_date}
        
    # Time Stop Exit
    pnl = ((d2_close - entry_price) / entry_price) * 100
    return {"entry_price": entry_price, "status": "⏳ TIME STOP (2 Days)", "pnl": round(pnl, 2), "entry_date": entry_date, "exit_date": day_2_date}

def run():
    print("| Signal Date | Symbol | Entry Date | Entry Price | Exit Date | Status | P&L % |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    
    total_trades = 0
    wins = 0
    losses = 0
    active = 0
    pnl_sum = 0.0
    
    for symbol, signal_date in TRADES:
        res = simulate_trade(symbol, signal_date)
        sig_dt_str = datetime.strptime(signal_date, "%Y-%m-%d").strftime("%b %d")
        
        entry_pr_str = f"₹{res['entry_price']:.2f}" if res['entry_price'] > 0 else "-"
        pnl_val = res['pnl']
        pnl_str = f"+{pnl_val:.2f}%" if pnl_val > 0 else f"{pnl_val:.2f}%"
        if res['status'] == "PENDING_ENTRY":
            pnl_str = "-"
            
        exit_date_str = res['exit_date'] if res['exit_date'] else "-"
        entry_date_str = res['entry_date'] if res['entry_date'] else "-"
        
        print(f"| {sig_dt_str} | **{symbol}** | {entry_date_str} | {entry_pr_str} | {exit_date_str} | {res['status']} | {pnl_str} |")
        
        if res['status'] != "PENDING_ENTRY":
            total_trades += 1
            pnl_sum += pnl_val
            is_active = "ACTIVE" in res['status']
            if is_active:
                active += 1
            
            if pnl_val > 0:
                if is_active:
                    pass
                else:
                    wins += 1
            elif pnl_val < 0:
                if is_active:
                    pass
                else:
                    losses += 1
                
    win_rate = (wins / (total_trades - active) * 100) if (total_trades - active) > 0 else 0.0
    avg_pnl = (pnl_sum / total_trades) if total_trades > 0 else 0.0
    
    print("\n### Summary Statistics")
    print(f"- **Total Simulated Trades:** {total_trades}")
    print(f"- **Active Trades (Open):** {active}")
    print(f"- **Closed Trades:** {total_trades - active}")
    print(f"- **Wins (Closed):** {wins}")
    print(f"- **Losses (Closed):** {losses}")
    print(f"- **Win Rate (Closed Trades):** {win_rate:.2f}%")
    print(f"- **Average Return per Trade:** {avg_pnl:+.2f}%")
    print(f"- **Cumulative P&L:** {pnl_sum:+.2f}%")

if __name__ == "__main__":
    run()
