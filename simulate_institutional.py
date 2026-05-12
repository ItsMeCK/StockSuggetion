import json
import glob
import os
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def get_db_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )

def simulate():
    conn = get_db_connection()
    cur = conn.cursor()
    
    load_dotenv()
    kite = KiteConnect(api_key=os.getenv('KITE_API_KEY'))
    kite.set_access_token(os.getenv('KITE_ACCESS_TOKEN'))

    history_files = glob.glob("run_history/run_202605*.json")
    
    # Group by date and take the latest run per day
    daily_runs = {}
    for f in history_files:
        date = os.path.basename(f).split('_')[1] # YYYYMMDD
        if date not in daily_runs or f > daily_runs[date]:
            daily_runs[date] = f
    
    sorted_files = sorted(daily_runs.values())
    
    print("\n--- INSTITUTIONAL DYNAMIC METHODOLOGY SIMULATION ---")
    print(f"{'Date':<12} | {'Symbol':<12} | {'P&L %':<8} | {'Score':<6} | {'Regime':<12}")
    print("-" * 60)

    total_count = 0
    total_wins = 0
    total_pnl = 0.0

    for file_path in sorted_files:
        with open(file_path, "r") as f:
            data = json.load(f)
        
        regime = data.get('macro_regime', 'NEUTRAL')
        date_str = data['timestamp'][:10]
        approved = data.get("approved_allocations", {})
        entry_results = data.get("entry_trigger_results", {})
        
        # DYNAMIC THRESHOLD based on regime
        threshold = 135 if regime == 'TUG_OF_WAR' else 130
        
        symbols = [f"NSE:{s}" for s in approved.keys()]
        quotes = kite.quote(symbols)
        
        day_count = 0
        for ticker, details in approved.items():
            score = details.get('conviction_score', 0)
            
            # 1. APPLY DYNAMIC SCORE THRESHOLD
            if score < threshold: continue
            
            # 2. APPLY LIQUIDITY FILTER (Volume * Price > 20 Cr)
            # We'll approximate this using the signal day volume
            # Wait, the JSON doesn't have volume. I'll query DB.
            cur.execute("SELECT volume, close FROM daily_ohlcv WHERE symbol = %s AND time::date = %s::date", (ticker, date_str))
            vol_res = cur.fetchone()
            if vol_res:
                turnover = (vol_res[0] * float(vol_res[1])) / 1e7 # in Crores
                if turnover < 15: continue # 15 Cr liquidity gate
            
            # 3. Calculate Performance
            cur.execute("SELECT open FROM daily_ohlcv WHERE symbol = %s AND time > %s ORDER BY time ASC LIMIT 1", (ticker, date_str))
            res = cur.fetchone()
            buy_price = float(res[0]) if res else 0.0
            if buy_price == 0:
                buy_price = quotes.get(f"NSE:{ticker}", {}).get('ohlc', {}).get('open', 0.0)
            
            live_price = quotes.get(f"NSE:{ticker}", {}).get('last_price', 0.0)
            
            if buy_price > 0 and live_price > 0:
                pnl = ((live_price - buy_price) / buy_price) * 100
                
                print(f"{date_str:<12} | {ticker:<12} | {pnl:>7.2f}% | {score:<6.1f} | {regime:<12}")
                
                total_count += 1
                total_pnl += pnl
                if pnl > 0: total_wins += 1
                day_count += 1

    cur.close()
    conn.close()
    
    if total_count > 0:
        print("-" * 60)
        print(f"TOTAL TRADES: {total_count}")
        print(f"AVG P&L: {total_pnl/total_count:.2f}%")
        print(f"WIN RATE: {total_wins/total_count*100:.1f}%")

if __name__ == "__main__":
    simulate()
