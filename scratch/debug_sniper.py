import polars as pl

def analyze_sniper():
    parquet_path = "data/intraday_ohlcv.parquet"
    df = pl.read_parquet(parquet_path)
    
    df = df.sort(["symbol", "time"])
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20"),
        pl.col("close").shift(1).over("symbol").alias("prev_close"),
        pl.col("low").shift(1).over("symbol").alias("prev_low")
    ])
    
    latest_time = df.select(pl.col("time").max()).item()
    print(f"Analyzing Target Candle: {latest_time} UTC")
    
    current_hour_df = df.filter(pl.col("time") == latest_time).sort("symbol")
    print(f"Total Universe: {current_hour_df.height} stocks")
    
    current_hour_df = current_hour_df.with_columns([
        (pl.col("volume") / pl.col("vol_sma_20")).alias("vol_surge"),
        (
            pl.when(pl.col("high") != pl.col("low"))
            .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
            .otherwise(0.0)
        ).alias("close_to_high_ratio")
    ])
    
    # Condition 1: Structural Integrity (Green candle, above 20 SMA)
    c1 = current_hour_df.filter(
        (pl.col("close") > pl.col("open")) &
        (pl.col("close") > pl.col("sma_20"))
    )
    print(f"Passed Structural Integrity (Green & > SMA20): {c1.height}")
    
    # Condition 2: Pump & Dump Veto (Higher close and higher low)
    c2 = c1.filter(
        (pl.col("close") > pl.col("prev_close")) &
        (pl.col("low") > pl.col("prev_low"))
    )
    print(f"Passed Pump & Dump Veto (Higher Highs/Lows): {c2.height}")
    
    # Condition 3: No Supply (Close near HOD > 0.85)
    c3 = c2.filter(
        (pl.col("close_to_high_ratio") > 0.85)
    )
    print(f"Passed No Supply (Close near HOD > 85%): {c3.height}")
    
    # Condition 4: Slow Grind (Vol Surge > 1.2 and < 3.0)
    c4 = c3.filter(
        (pl.col("vol_surge") > 1.2) & (pl.col("vol_surge") < 3.0)
    )
    print(f"Passed Slow Grind (Volume TWAP): {c4.height}")
    
    if c3.height > 0:
        print("\nStocks that passed Condition 3 but failed Vol Surge:")
        for row in c3.iter_rows(named=True):
            print(f"- {row['symbol']}: vol_surge = {row['vol_surge']:.2f}")

if __name__ == "__main__":
    analyze_sniper()
