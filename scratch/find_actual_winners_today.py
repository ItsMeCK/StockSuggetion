import polars as pl

def find_actual_winners():
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    # Filter for today (2026-08-18)
    today_df = df.filter(
        pl.col("time").dt.date() == pl.date(2026, 8, 18)
    )
    
    # We want to see the performance from 10:15 AM to 15:15 PM
    # Let's find the closing price of the 09:15 candle (which is the 10:15 entry price)
    # The 09:15 candle has timestamp = 03:45:00 UTC
    entry_df = today_df.filter(
        pl.col("time").dt.hour() == 3,
        pl.col("time").dt.minute() == 45
    ).select(["symbol", "close"]).rename({"close": "entry_price"})
    
    # Let's find the max high achieved between 10:15 and 15:15
    # Timestamps > 03:45:00 UTC
    subsequent_df = today_df.filter(
        (pl.col("time").dt.hour() > 3) | 
        ((pl.col("time").dt.hour() == 3) & (pl.col("time").dt.minute() > 45))
    )
    
    max_high_df = subsequent_df.group_by("symbol").agg(
        pl.col("high").max().alias("max_high"),
        pl.col("close").last().alias("final_close")
    )
    
    # Join and calculate max possible % gain
    results = entry_df.join(max_high_df, on="symbol")
    results = results.with_columns(
        ((pl.col("max_high") - pl.col("entry_price")) / pl.col("entry_price") * 100).alias("max_gain_pct"),
        ((pl.col("final_close") - pl.col("entry_price")) / pl.col("entry_price") * 100).alias("final_gain_pct")
    )
    
    top_winners = results.filter(pl.col("max_gain_pct") > 3.0).sort("max_gain_pct", descending=True)
    
    print(f"Top Winners Today (Max Gain > 3% after 10:15 AM):")
    for row in top_winners.iter_rows(named=True):
        print(f"{row['symbol']}: Max Gain {row['max_gain_pct']:.2f}% | Final Gain {row['final_gain_pct']:.2f}%")

if __name__ == "__main__":
    find_actual_winners()
