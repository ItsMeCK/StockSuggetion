import os
import psycopg2
import polars as pl
from dotenv import load_dotenv

def get_missed_trades():
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
        WHERE time >= '2026-06-01' AND time <= '2026-08-04'
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

    # Find the setups triggered on FRIDAY (July 31, 2026 IST = July 30 UTC) using baseline rules
    friday_setups = df.filter(
        (pl.col("bbw") < 0.22) & 
        (pl.col("vol_surge") > 2.5) & 
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("time").cast(pl.String).str.contains("2026-07-30 18:30:00"))
    )
    
    # Find the setups triggered TODAY (August 3, 2026 IST = August 2 UTC) using baseline rules
    today_setups = df.filter(
        (pl.col("bbw") < 0.22) & 
        (pl.col("vol_surge") > 2.5) & 
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("time").cast(pl.String).str.contains("2026-08-02 18:30:00"))
    )
    
    print("=== MISSED TRADES ON FRIDAY (JULY 31) ===")
    if len(friday_setups) == 0:
        print("No ultra-tight setups on Friday.")
    else:
        for row in friday_setups.iter_rows(named=True):
            print(f"SYMBOL: {row['symbol']:<12} | Close: ₹{row['close']:<8.2f} | BBW: {row['bbw']:.3f} | Vol Surge: {row['vol_surge']:.2f}x")
            
    print("\n=== NEW TRADES TRIGGERED TODAY (AUGUST 3) ===")
    if len(today_setups) == 0:
        print("No ultra-tight setups today.")
    else:
        for row in today_setups.iter_rows(named=True):
            print(f"SYMBOL: {row['symbol']:<12} | Close: ₹{row['close']:<8.2f} | BBW: {row['bbw']:.3f} | Vol Surge: {row['vol_surge']:.2f}x")

if __name__ == "__main__":
    get_missed_trades()
