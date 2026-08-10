import polars as pl
from datetime import datetime
import pytz

def backtest_friday_anticipatory():
    # Load the intraday parquet file
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    # Calculate indicators (SMA, BBW, Vol Surge) just like the main engine
    df = df.sort(["symbol", "time"])
    
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20")
    ])
    
    df = df.with_columns([
        (pl.col("sma_20") + 2 * pl.col("std_20")).alias("upper_band"),
        (pl.col("sma_20") - 2 * pl.col("std_20")).alias("lower_band"),
    ])
    
    df = df.with_columns([
        ((pl.col("upper_band") - pl.col("lower_band")) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_sma_20")).alias("vol_surge")
    ])
    
    # Drop nulls
    df = df.drop_nulls()
    
    # Target Friday 3:15 PM IST (which is 2026-08-07 08:45:00 UTC)
    target_utc = datetime(2026, 8, 7, 8, 45, tzinfo=pytz.utc)
    friday_df = df.filter(pl.col("time") == target_utc)
    
    # Apply the "Silent Accumulation" Filter
    # 1. BBW < 0.25 (Still tight, hasn't exploded yet)
    # 2. Vol Surge > 1.2 but < 2.5 (Subtle accumulation, not a full breakout)
    # 3. Close > SMA 20 (Uptrending)
    # 4. Close > Open (Green candle into the close)
    anticipatory_candidates = friday_df.filter(
        (pl.col("bbw") < 0.25) & 
        (pl.col("vol_surge") > 1.2) & 
        (pl.col("vol_surge") <= 2.5) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") > pl.col("open"))
    ).sort("vol_surge", descending=True)
    
    print(f"Total symbols evaluated on Friday 3:15 PM: {len(friday_df)}")
    print(f"Silent Accumulation Candidates Found: {len(anticipatory_candidates)}")
    
    # Print the top 15 candidates
    pdf = anticipatory_candidates.head(15).to_pandas()
    print("\nTop 15 Anticipatory Candidates (Friday 3:15 PM):")
    print(pdf[["symbol", "close", "vol_surge", "bbw"]].to_string(index=False))
    
    # Explicitly check for PAYTM and POWERINDIA
    for sym in ["PAYTM", "POWERINDIA", "NAUKRI"]:
        sym_df = friday_df.filter(pl.col("symbol") == sym).to_pandas()
        if not sym_df.empty:
            print(f"\n{sym} Friday 3:15 PM Status:")
            print(sym_df[["symbol", "close", "sma_20", "vol_surge", "bbw"]].to_string(index=False))
        else:
            print(f"\n{sym} was not found in Friday's data.")

if __name__ == "__main__":
    backtest_friday_anticipatory()
