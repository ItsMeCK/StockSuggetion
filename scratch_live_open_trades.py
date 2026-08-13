import os
import math
import psycopg2
import polars as pl
from dotenv import load_dotenv

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

def run_live_check():
    load_dotenv()
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )
    
    query = """
        SELECT time, symbol, close, volume 
        FROM daily_ohlcv 
        WHERE time >= '2026-03-01' AND time <= '2026-07-29'
        ORDER BY symbol, time
    """
    df = pl.read_database(query, conn)
    df = df.filter(~pl.col("symbol").str.contains(r"\d")).sort(["symbol", "time"])
    
    # Base Indicators
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])
    
    # Core Strategy Metrics
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
    ])

    # Find ALL ultra-tight setups in July 18 - 28
    setups = df.filter(
        (pl.col("bbw") < 0.15) & 
        (pl.col("vol_surge") > 4.0) & 
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("time").cast(pl.String).str.contains("2026-07-18") | 
         pl.col("time").cast(pl.String).str.contains("2026-07-19") |
         pl.col("time").cast(pl.String).str.contains("2026-07-20") |
         pl.col("time").cast(pl.String).str.contains("2026-07-21") |
         pl.col("time").cast(pl.String).str.contains("2026-07-22") |
         pl.col("time").cast(pl.String).str.contains("2026-07-23") |
         pl.col("time").cast(pl.String).str.contains("2026-07-24") |
         pl.col("time").cast(pl.String).str.contains("2026-07-25") |
         pl.col("time").cast(pl.String).str.contains("2026-07-26") |
         pl.col("time").cast(pl.String).str.contains("2026-07-27") |
         pl.col("time").cast(pl.String).str.contains("2026-07-28"))
    )
    
    r = 0.07
    sigma = 0.25
    HOLDING_PERIOD = 11.0 # Max holding period in days
    
    open_trades = []
    
    from datetime import datetime, timezone
    today = datetime(2026, 7, 29, tzinfo=timezone.utc)
    
    for row in setups.iter_rows(named=True):
        entry_date = row['time']
        symbol = row['symbol']
        S_entry = row['close']
        
        # Calculate days elapsed since entry
        days_elapsed = (today - entry_date).days
        
        if days_elapsed <= 0:
            # Triggered today, PnL is 0
            continue
            
        # Get today's close price (last row for this symbol)
        today_data = df.filter(pl.col("symbol") == symbol).sort("time").tail(1)
        
        if len(today_data) == 0:
            continue
            
        S_today = today_data["close"][0]
        
        # Calculate Option PnL
        DTE_entry = 14.0 / 365.0
        DTE_today = (14.0 - days_elapsed) / 365.0
        
        opt_price_entry = bs_price(S_entry, S_entry, DTE_entry, r, sigma, 'CE')
        opt_price_today = bs_price(S_today, S_entry, DTE_today, r, sigma, 'CE')
        
        if opt_price_entry > 0:
            trade_pnl = (opt_price_today / opt_price_entry) - 1
            status = "OPEN"
            
            # Check Stop Loss
            if trade_pnl <= -0.50: 
                trade_pnl = -0.50
                status = "STOPPED_OUT"
                
            open_trades.append({
                'symbol': symbol,
                'entry_date': str(entry_date.date()),
                'entry_price': S_entry,
                'today_price': S_today,
                'days_held': days_elapsed,
                'pnl': trade_pnl * 100,
                'status': status
            })

    print(f"=== OPEN TRADES (Triggered July 18 - 28) ===")
    for t in sorted(open_trades, key=lambda x: x['pnl'], reverse=True):
        print(f"{t['symbol']:<12} | Entry: {t['entry_date']} @ {t['entry_price']:.2f} | Today: {t['today_price']:.2f} | Held: {t['days_held']}d | PnL: {t['pnl']:>7.2f}% | Status: {t['status']}")
        
    if open_trades:
        avg_pnl = sum(t['pnl'] for t in open_trades) / len(open_trades)
        print(f"\nNet Portfolio PnL of Current Open Trades: {avg_pnl:.2f}%")

if __name__ == "__main__":
    run_live_check()
