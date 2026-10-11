import polars as pl
from datetime import datetime
import os

def analyze():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        return
        
    df = pl.read_parquet(parquet_path)
    sail = df.filter(pl.col("symbol") == "SAIL").sort("time")
    
    # MAs
    sail = sail.with_columns([
        pl.col("close").rolling_mean(window_size=20).alias("sma_20"),
        pl.col("volume").rolling_mean(window_size=20).alias("vol_avg_20"),
        pl.col("time").dt.date().alias("date"),
        (pl.col("close") * pl.col("volume")).alias("pv")
    ])
    
    # IAF Metrics
    sail = sail.with_columns([
        pl.max_horizontal(
            (pl.col("high") - pl.col("low")),
            (pl.col("high") - pl.col("close").shift(1)).abs(),
            (pl.col("low") - pl.col("close").shift(1)).abs()
        ).alias("true_range")
    ])
    sail = sail.with_columns([
        pl.col("true_range").rolling_mean(window_size=14).alias("atr_14")
    ])
    
    # Z-Score
    sail = sail.with_columns([
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_expansion"),
        (pl.col("true_range") / pl.col("atr_14")).alias("range_expansion")
    ])
    sail = sail.with_columns([
        (pl.col("vol_expansion") * pl.col("range_expansion")).alias("breakout_energy")
    ])
    sail = sail.with_columns([
        pl.col("breakout_energy").rolling_mean(window_size=20).alias("energy_mean"),
        pl.col("breakout_energy").rolling_std(window_size=20).alias("energy_std")
    ])
    sail = sail.with_columns([
        ((pl.col("breakout_energy") - pl.col("energy_mean")) / pl.col("energy_std")).alias("energy_zscore")
    ])
    
    # TWVD
    sail = sail.with_columns([
        pl.when(pl.col("close") > pl.col("open")).then(1).otherwise(-1).alias("direction")
    ])
    sail = sail.with_columns([
        pl.when(pl.col("volume") > 1.5 * pl.col("vol_avg_20"))
        .then(pl.col("direction") * (pl.col("volume") / pl.col("vol_avg_20")))
        .otherwise(0)
        .alias("surge_magnitude")
    ])
    sail = sail.with_columns([
        pl.col("surge_magnitude").ewm_mean(alpha=0.2, ignore_nulls=True).alias("twvd")
    ])
    
    # Print Monday
    mon = sail.filter(pl.col("time").dt.date() == datetime(2026, 8, 24).date())
    print("SAIL 10:15 Data:")
    print(mon.select(["time", "twvd", "energy_zscore", "breakout_energy"]).head(1))

if __name__ == "__main__":
    analyze()
