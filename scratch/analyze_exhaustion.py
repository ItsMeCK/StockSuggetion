import polars as pl
from datetime import datetime, timezone
import pytz

parquet_path = "../data/intraday_ohlcv.parquet"
df = pl.read_parquet(parquet_path)
symbols = ["DLF", "LODHA", "PRESTIGE", "SUZLON"]
df = df.filter(pl.col("symbol").is_in(symbols))
df = df.sort(["symbol", "time"])

print("Data for today:")
for symbol in symbols:
    print(f"\n--- {symbol} ---")
    sym_df = df.filter(pl.col("symbol") == symbol).tail(10)
    for row in sym_df.iter_rows(named=True):
        print(f"Time: {row['time']}, Open: {row['open']}, High: {row['high']}, Low: {row['low']}, Close: {row['close']}, Vol: {row['volume']}")
