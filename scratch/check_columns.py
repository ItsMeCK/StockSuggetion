import polars as pl

df = pl.read_parquet("data/intraday_ohlcv.parquet")
print("Parquet Columns:", df.columns)

records = [{"time": "x", "symbol": "y", "open": 1, "high": 2, "low": 3, "close": 4, "volume": 5}]
new_df = pl.DataFrame(records)
print("New DF Columns:", new_df.columns)
