import os, sys, pandas as pd, psycopg2, logging
from datetime import datetime, timedelta

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from run_historical import run_historical_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_market_data(symbol, start_date, days=10):
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('TIMESCALE_PORT', '5432'),
        user=os.getenv('TIMESCALE_USER', 'quant'),
        password=os.getenv('TIMESCALE_PASSWORD', 'quantpassword'),
        database=os.getenv('TIMESCALE_DB', 'market_data')
    )
    cur = conn.cursor()
    query = "SELECT close FROM daily_ohlcv WHERE symbol = %s AND time > %s ORDER BY time ASC LIMIT %s"
    cur.execute(query, (symbol, start_date, days))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [float(r[0]) for r in rows]

def run_performance_audit():
    audit_dates = ['2026-04-02', '2026-04-09', '2026-04-16', '2026-04-23']
    results = []
    
    for date in audit_dates:
        logging.info(f"AUDITING PERFORMANCE FOR {date}...")
        output = run_historical_engine(date)
        approved = output.get("approved", [])
        
        for sym in approved:
            prices = get_market_data(sym, date, 7)
            if not prices: continue
            
            entry_price = prices[0]
            max_up = (max(prices) - entry_price) / entry_price * 100
            max_down = (min(prices) - entry_price) / entry_price * 100
            
            outcome = "WIN" if max_up >= 5 and max_down > -5 else ("LOSS" if max_down <= -5 else "NEUTRAL")
            results.append({"date": date, "symbol": sym, "max_up": max_up, "max_down": max_down, "outcome": outcome})
            
    df = pd.DataFrame(results)
    win_rate = (len(df[df['outcome'] == 'WIN']) / len(df)) * 100 if len(df) > 0 else 0
    loss_rate = (len(df[df['outcome'] == 'LOSS']) / len(df)) * 100 if len(df) > 0 else 0
    
    print("\n" + "="*50)
    print(f"SOVEREIGN V2.3 PERFORMANCE AUDIT (APRIL 2026)")
    print("="*50)
    print(f"Total Trades Sampled: {len(df)}")
    print(f"Win Rate (>5% Profit): {win_rate:.1f}%")
    print(f"Loss Rate (<5% Stop): {loss_rate:.1f}%")
    print(f"Neutral (Holding): {100 - win_rate - loss_rate:.1f}%")
    print("="*50)
    
    # Monday Freshness
    monday_output = run_historical_engine('2026-04-30')
    monday_list = set(monday_output.get("approved", []))
    prev_list = set(approved) # From the last audit date (April 23)
    
    fresh_signals = monday_list - prev_list
    print(f"\nFRESHLY RAISED SIGNALS FOR MONDAY (MAY 4):")
    print(list(fresh_signals))
    print("="*50)

if __name__ == "__main__":
    run_performance_audit()
