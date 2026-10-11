import polars as pl

df = pl.read_parquet("data/intraday_ohlcv.parquet")
df = df.sort(["symbol", "time"])
df = df.with_columns([
    pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20")
])

glaxo = df.filter(pl.col("symbol") == "GLAXO").tail(5)
print(glaxo.select(["time", "volume", "vol_sma_20"]))
