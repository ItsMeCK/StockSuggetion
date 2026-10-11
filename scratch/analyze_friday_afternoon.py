import polars as pl
from datetime import datetime

def analyze_friday_pm():
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    # Filter for Friday, Aug 21
    friday = datetime(2026, 8, 21).date()
    if "date" not in df.columns:
        df = df.with_columns(pl.col("time").dt.date().alias("date"))
    df_friday = df.filter(pl.col("date") == friday)
    
    symbols = ["LTFOODS", "URBANCO", "HFCL"]
    
    for symbol in symbols:
        print(f"\n{'='*50}\nAnalyzing {symbol} on Friday Afternoon (Aug 21)\n{'='*50}")
        sym_df = df_friday.filter(pl.col("symbol") == symbol).sort("time")
        if sym_df.height == 0:
            print(f"No data for {symbol} on Friday.")
            continue
            
        # Get the 14:15 (08:45 UTC) and 15:15 (09:45 UTC) candles
        afternoon_df = sym_df.filter(
            (pl.col("time").dt.hour() == 8) & (pl.col("time").dt.minute() == 45) |
            (pl.col("time").dt.hour() == 9) & (pl.col("time").dt.minute() == 45)
        )
        
        for row in afternoon_df.iter_rows(named=True):
            print(f"Time: {row['time']}")
            print(f"  Close: {row['close']} | Open: {row['open']} | Vol: {row['volume']}")
            
        # Let's also calculate if they had volume surge or bbw coil
        # We need the full df to get moving averages
        full_sym = df.filter(pl.col("symbol") == symbol).sort("time")
        full_sym = full_sym.with_columns([
            pl.col("close").rolling_mean(window_size=20).alias("sma_20"),
            pl.col("close").rolling_std(window_size=20).alias("std_20"),
            pl.col("volume").rolling_mean(window_size=20).alias("vol_avg_20"),
        ])
        
        full_sym = full_sym.with_columns([
            ((pl.col("sma_20") + 2 * pl.col("std_20") - (pl.col("sma_20") - 2 * pl.col("std_20"))) / pl.col("sma_20")).alias("bbw"),
            (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge")
        ])
        
        friday_full = full_sym.filter(pl.col("date") == friday).filter(
            (pl.col("time").dt.hour() == 8) & (pl.col("time").dt.minute() == 45) |
            (pl.col("time").dt.hour() == 9) & (pl.col("time").dt.minute() == 45)
        )
        
        for row in friday_full.iter_rows(named=True):
            print(f"  [Metrics] BBW: {row['bbw']:.4f} | Vol Surge: {row['vol_surge']:.2f}x | SMA20: {row['sma_20']:.2f}")

if __name__ == "__main__":
    analyze_friday_pm()
