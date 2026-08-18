import polars as pl

df = pl.read_parquet("data/intraday_ohlcv.parquet")
print("Schema:", df.schema)
print("Row count:", len(df))
print("Symbols count:", df["symbol"].n_unique())
print("Unique symbols (first 20):", df["symbol"].unique().head(20).to_list())
print("Time range:", df["time"].min(), "to", df["time"].max())

# Check for our 8 symbols
target_symbols = ["ZYDUSLIFE", "OBEROIRLTY", "DRREDDY", "PRESTIGE", "NATIONALUM", "PATANJALI", "TRENT", "TATAPOWER"]
present = df.filter(pl.col("symbol").is_in(target_symbols))
print("Present target symbols:", present["symbol"].unique().to_list())
print("Target symbols rows count per symbol:")
for s in target_symbols:
    sdf = df.filter(pl.col("symbol") == s)
    if len(sdf) > 0:
        print(f"  {s}: {len(sdf)} rows, time min={sdf['time'].min()}, time max={sdf['time'].max()}")
    else:
        print(f"  {s}: 0 rows (NOT FOUND)")
