import os
import polars as pl
from datetime import datetime
import pytz

def run_aug12_math():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print("Parquet file missing!")
        return

    df = pl.read_parquet(parquet_path)
    ist_tz = pytz.timezone('Asia/Kolkata')
    
    df = df.sort(["symbol", "time"])
    
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])
    
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
    ])
    
    hours_to_check = [9, 10, 11, 12, 13, 14] 
    
    for h in hours_to_check:
        print(f"\n{'='*50}")
        print(f"Simulating Run at {h+1}:15 IST (Checking {h}:15 IST Candle)")
        
        target_ist = ist_tz.localize(datetime(2026, 8, 12, h, 15, 0))
        target_utc = target_ist.astimezone(pytz.utc)
        
        historical_view = df.filter(pl.col("time") <= target_utc)
        if historical_view.height == 0:
            continue
            
        latest_time = historical_view.select(pl.col("time").max())[0, 0]
        
        if latest_time != target_utc:
            print(f"Latest candle found is {latest_time}, expected {target_utc}. Skipping.")
            continue
            
        current_hour_df = historical_view.filter(pl.col("time") == latest_time)
        
        breakouts = current_hour_df.filter(
            (pl.col("bbw") < 0.22) & 
            (pl.col("vol_surge") > 2.5) & 
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < (pl.col("sma_20") * 1.03)) &
            (pl.col("close") > pl.col("open"))
        ).sort("vol_surge", descending=True).head(4)
        
        valid_symbols = [row['symbol'] for row in breakouts.iter_rows(named=True)]
        
        if valid_symbols:
            print(f"🎯 Math Engine generated Breakouts: {valid_symbols}")
            for row in breakouts.iter_rows(named=True):
                print(f"   -> {row['symbol']}: Vol Surge = {row['vol_surge']:.2f}x, BBW = {row['bbw']:.3f}")
        else:
            print("No valid breakouts found.")

if __name__ == "__main__":
    run_aug12_math()
