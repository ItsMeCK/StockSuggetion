import polars as pl
from datetime import datetime

def analyze_1015_candle():
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    today = datetime(2026, 8, 24).date()
    if "date" not in df.columns:
        df = df.with_columns(pl.col("time").dt.date().alias("date"))
    df_today = df.filter(pl.col("date") == today)
    
    symbols = ["URBANCO", "HFCL", "VMM", "LTFOODS"]
    
    for symbol in symbols:
        print(f"\nAnalyzing {symbol} 10:15-11:15 Candle")
        sym_df = df_today.filter(pl.col("symbol") == symbol).sort("time")
        
        # 10:15 IST is 04:45 UTC
        candle = sym_df.filter((pl.col("time").dt.hour() == 4) & (pl.col("time").dt.minute() == 45))
        if candle.height > 0:
            row = candle[0]
            print(f"Time: {row['time'][0]}")
            print(f"Open: {row['open'][0]}, High: {row['high'][0]}, Low: {row['low'][0]}, Close: {row['close'][0]}")
            # Calculate % move in this specific hour
            pct_move = ((row['close'][0] - row['open'][0]) / row['open'][0]) * 100
            print(f"Move in 2nd hour: {pct_move:.2f}%")
        else:
            print("No candle found.")

if __name__ == "__main__":
    analyze_1015_candle()
