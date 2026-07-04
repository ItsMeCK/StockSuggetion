import psycopg2
import os
import pandas as pd
import json

def analyze_missed_rally():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )
    
    # Get top gainers for today (May 25 -> DB date 2026-05-24)
    query_today = """
    SELECT symbol, close as close_today, open as open_today, volume as vol_today 
    FROM daily_ohlcv 
    WHERE DATE(time) = '2026-05-24'
    """
    df_today = pd.read_sql(query_today, conn)
    
    # Get Friday's close (May 22 -> DB date 2026-05-21)
    query_friday = """
    SELECT symbol, close as close_friday, volume as vol_friday
    FROM daily_ohlcv 
    WHERE DATE(time) = '2026-05-21'
    """
    df_friday = pd.read_sql(query_friday, conn)
    conn.close()
    
    if df_today.empty or df_friday.empty:
        print("Missing data for May 24 or May 21 in DB")
        return
        
    df = pd.merge(df_today, df_friday, on='symbol')
    df['pct_change'] = ((df['close_today'] - df['close_friday']) / df['close_friday']) * 100
    
    # Filter for reasonable volume/liquidity (Turnover > 10 Cr on Friday)
    df['turnover_friday'] = (df['close_friday'] * df['vol_friday']) / 10000000.0
    df = df[df['turnover_friday'] > 10]
    
    top_gainers = df.sort_values(by='pct_change', ascending=False).head(15)
    
    print("--- TOP 15 LIQUID STOCKS THAT BLASTED TODAY (MAY 25 / DB MAY 24) ---")
    print(top_gainers[['symbol', 'pct_change', 'turnover_friday', 'close_today', 'close_friday']].to_string(index=False))
    
    print("\n--- CHECKING FRIDAY'S ENGINE RUN (May 22, 15:20) ---")
    
    # Check if these were seen in Friday's run
    run_file = "run_history/run_20260522_152004.json"
    if os.path.exists(run_file):
        with open(run_file, 'r') as f:
            data = json.load(f)
            candidates = data.get('candidates', [])
            alloc = data.get('approved_allocations', {})
            
            top_symbols = top_gainers['symbol'].tolist()
            print(f"Friday's Initial Candidates from Screener: {len(candidates)}")
            print(f"Were any of today's top 15 gainers in Friday's initial candidate list?")
            for sym in top_symbols:
                if sym in candidates:
                    print(f" -> YES: {sym} was screened, but failed Cognitive/Critic Agent!")
                elif sym in alloc:
                    print(f" -> YES: {sym} was APPROVED! (Did we miss it?)")
                else:
                    print(f" -> NO: {sym} was completely rejected/not-screened by the Pring/Catalyst Screener on Friday.")
    else:
        print("Could not find Friday's run log.")

if __name__ == "__main__":
    analyze_missed_rally()
