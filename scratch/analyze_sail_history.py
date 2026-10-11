import polars as pl
from datetime import datetime
import os

def analyze():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print("No dataset")
        return
        
    df = pl.read_parquet(parquet_path)
    sail = df.filter(pl.col("symbol") == "SAIL").sort("time")
    
    if sail.height == 0:
        print("No SAIL data")
        return
        
    # Calculate exactly what live_hourly_job.py calculates
    sail = sail.with_columns([
        pl.col("close").rolling_mean(window_size=20).alias("sma_20"),
        pl.col("volume").rolling_mean(window_size=20).alias("vol_avg_20"),
        pl.col("time").dt.date().alias("date"),
        (pl.col("close") * pl.col("volume")).alias("pv")
    ])
    
    sail = sail.with_columns([
        pl.col("volume").cum_sum().over("date").alias("cum_vol"),
        pl.col("pv").cum_sum().over("date").alias("cum_pv")
    ])
    
    sail = sail.with_columns([
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap")
    ])
    
    sail = sail.with_columns([
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (((pl.col("close") - pl.col("vwap")) / pl.col("vwap")) * 100).alias("vwap_ext_pct"),
        (((pl.col("close") - pl.col("open").first().over("date")) / pl.col("open").first().over("date")) * 100).alias("intraday_ext_pct"),
        (((pl.col("close") - pl.col("open")) / pl.col("open")) * 100).alias("candle_pct")
    ])
    
    # Filter for last Thursday, Friday, Monday
    target_dates = [datetime(2026, 8, 20).date(), datetime(2026, 8, 21).date(), datetime(2026, 8, 24).date(), datetime(2026, 8, 25).date()]
    
    analysis = sail.filter(pl.col("date").is_in(target_dates))
    
    print(f"{'Time':<22} | {'Open':>6} | {'Close':>6} | {'Candle%':>7} | {'VolSurge':>8} | {'VWAP Ext%':>9} | {'IntradayExt%':>12}")
    print("-" * 90)
    for row in analysis.iter_rows(named=True):
        t = str(row['time'])
        o = row['open']
        c = row['close']
        c_pct = row['candle_pct']
        vs = row['vol_surge'] if row['vol_surge'] else 0
        ve = row['vwap_ext_pct'] if row['vwap_ext_pct'] else 0
        ie = row['intraday_ext_pct'] if row['intraday_ext_pct'] else 0
        
        # Highlight massive candles
        flag = " <--- HUGE SURGE" if c_pct > 1.5 else ""
        
        print(f"{t:<22} | {o:>6.2f} | {c:>6.2f} | {c_pct:>6.2f}% | {vs:>7.2f}x | {ve:>8.2f}% | {ie:>11.2f}%{flag}")

if __name__ == "__main__":
    analyze()
