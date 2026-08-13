import os
import psycopg2
import polars as pl
from dotenv import load_dotenv

def run_tightening_scenarios():
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
    
    # NIFTY 50 MACRO TREND
    nifty = df.filter(pl.col("symbol") == "NIFTY 50").sort("time")
    nifty = nifty.with_columns([
        pl.col("close").rolling_mean(window_size=20).alias("nifty_sma_20")
    ])
    nifty = nifty.select(["time", "close", "nifty_sma_20"]).rename({"close": "nifty_close"})
    
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
    
    df = df.join(nifty, on="time", how="left")

    dates = [
        {"name": "FRIDAY (JULY 31)", "time_str": "2026-07-30 18:30:00"},
        {"name": "TODAY (AUGUST 3)", "time_str": "2026-08-02 18:30:00"}
    ]
    
    scenarios = [
        {"name": "Baseline (bbw < 0.22, vol > 2.5)", "bbw": 0.22, "vol": 2.5, "macro": False},
        {"name": "Macro Filter (Baseline + Nifty > 20 SMA)", "bbw": 0.22, "vol": 2.5, "macro": True},
        {"name": "Intermediate Tight (bbw < 0.18, vol > 3.0)", "bbw": 0.18, "vol": 3.0, "macro": False},
        {"name": "Ultra-Tight (bbw < 0.15, vol > 4.0)", "bbw": 0.15, "vol": 4.0, "macro": False}
    ]
    
    for d in dates:
        print(f"\n======================================")
        print(f"       {d['name']}")
        print(f"======================================")
        
        day_df = df.filter(pl.col("time").cast(pl.String).str.contains(d['time_str']))
        
        for s in scenarios:
            cond = (
                (pl.col("bbw") < s['bbw']) & 
                (pl.col("vol_surge") > s['vol']) & 
                (pl.col("close") > pl.col("sma_20"))
            )
            
            if s['macro']:
                cond = cond & (pl.col("nifty_close") > pl.col("nifty_sma_20"))
                
            res = day_df.filter(cond)
            
            symbols = [row['symbol'] for row in res.iter_rows(named=True)]
            print(f"\n[Scenario: {s['name']}]")
            print(f"Total Trades: {len(symbols)}")
            if symbols:
                print(f"Symbols: {', '.join(symbols)}")

if __name__ == "__main__":
    run_tightening_scenarios()
