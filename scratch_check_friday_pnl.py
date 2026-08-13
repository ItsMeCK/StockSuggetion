import os
import math
import psycopg2
import polars as pl
from dotenv import load_dotenv
from datetime import datetime, timezone

def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bs_price(S, K, T, r, sigma, opt_type):
    if T <= 0:
        return max(0.0, S - K) if opt_type == 'CE' else max(0.0, K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if opt_type == 'CE':
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)

def check_friday_pnl():
    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )
    
    query = """
        SELECT time, symbol, close 
        FROM daily_ohlcv 
        WHERE time >= '2026-07-30' AND time <= '2026-08-04'
        ORDER BY symbol, time
    """
    df = pl.read_database(query, conn)
    
    symbols = [
        "AARTIIND", "ABCAPITAL", "ABFRL", "ASHOKLEY", "AWL", "BAJAJFINSV", 
        "BAJFINANCE", "BLUEDART", "CONCORDBIO", "ESCORTS", "GAIL", "HYUNDAI", 
        "INDGN", "JPPOWER", "LTFOODS", "MGL", "NETWEB", "NIVABUPA", 
        "REDINGTON", "TBOTEK", "THELEELA", "TMCV", "ZEEL"
    ]
    
    r = 0.07
    sigma = 0.25
    DTE_entry = 14.0 / 365.0
    # Friday July 31 to Monday August 3 = 3 days elapsed
    days_elapsed = 3.0
    DTE_today = (14.0 - days_elapsed) / 365.0
    
    results = []
    
    for symbol in symbols:
        data = df.filter(pl.col("symbol") == symbol)
        
        entry_row = data.filter(pl.col("time").cast(pl.String).str.contains("2026-07-30 18:30:00"))
        today_row = data.filter(pl.col("time").cast(pl.String).str.contains("2026-08-02 18:30:00"))
        
        if len(entry_row) == 0 or len(today_row) == 0:
            continue
            
        S_entry = entry_row["close"][0]
        S_today = today_row["close"][0]
        
        opt_price_entry = bs_price(S_entry, S_entry, DTE_entry, r, sigma, 'CE')
        opt_price_today = bs_price(S_today, S_entry, DTE_today, r, sigma, 'CE')
        
        trade_pnl = (opt_price_today / opt_price_entry) - 1
        
        status = "OPEN"
        if trade_pnl <= -0.50:
            trade_pnl = -0.50
            status = "STOPPED_OUT"
            
        results.append({
            'symbol': symbol,
            'entry_price': S_entry,
            'today_price': S_today,
            'pnl': trade_pnl * 100,
            'status': status
        })
        
    print(f"=== P&L FOR FRIDAY'S 23 MISSED TRADES AS OF TODAY ===")
    for t in sorted(results, key=lambda x: x['pnl'], reverse=True):
        print(f"{t['symbol']:<12} | Friday Close: ₹{t['entry_price']:<8.2f} | Today Close: ₹{t['today_price']:<8.2f} | Option PnL: {t['pnl']:>7.2f}% | Status: {t['status']}")
        
    if results:
        avg_pnl = sum(t['pnl'] for t in results) / len(results)
        print(f"\nNet Portfolio PnL for these 23 trades: {avg_pnl:.2f}%")

if __name__ == "__main__":
    check_friday_pnl()
