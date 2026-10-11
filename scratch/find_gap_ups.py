import polars as pl
from datetime import datetime
import os

def analyze():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print("No dataset")
        return
        
    df = pl.read_parquet(parquet_path)
    
    # We want yesterday's close and today's open
    # Sort by symbol and time
    df = df.sort(["symbol", "time"])
    
    # Get the dates
    date_yesterday = datetime(2026, 8, 24).date()
    date_today = datetime(2026, 8, 25).date()
    
    # Extract the last close of yesterday for each symbol
    yest_df = df.filter(pl.col("time").dt.date() == date_yesterday)
    if yest_df.height == 0:
        print("No data for yesterday")
        return
    
    yest_close = yest_df.group_by("symbol").tail(1).select(["symbol", pl.col("close").alias("prev_close")])
    
    # Extract the first open of today for each symbol
    today_df = df.filter(pl.col("time").dt.date() == date_today)
    if today_df.height == 0:
        print("No data for today")
        return
        
    today_open = today_df.group_by("symbol").head(1).select(["symbol", pl.col("open").alias("today_open")])
    
    # Join and calculate gap
    merged = yest_close.join(today_open, on="symbol", how="inner")
    merged = merged.with_columns([
        (((pl.col("today_open") - pl.col("prev_close")) / pl.col("prev_close")) * 100).alias("gap_pct")
    ])
    
    # Sort by gap_pct descending
    top_gaps = merged.sort("gap_pct", descending=True).head(15)
    
    print("Top Gap Ups Today (Aug 25):")
    print(top_gaps)

if __name__ == "__main__":
    analyze()
