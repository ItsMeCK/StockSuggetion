import os
import psycopg2
import polars as pl
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()
kite = KiteConnect(api_key=os.getenv("KITE_API_KEY", "").strip("'\""))
kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN", "").strip("'\""))

# Fetch FNO list
instruments = kite.instruments("NFO")
fno_symbols = set([inst['name'] for inst in instruments if inst['instrument_type'] in ['CE', 'PE']])

print(f"Loaded {len(fno_symbols)} FNO symbols.")

conn = psycopg2.connect(
    host=os.getenv('DB_HOST', 'localhost'),
    port=os.getenv('DB_PORT', '5432'),
    user=os.getenv('POSTGRES_USER', 'quant'),
    password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
    dbname=os.getenv('POSTGRES_DB', 'market_data')
)

query = f"""
    SELECT time, symbol, close, volume 
    FROM daily_ohlcv 
    WHERE time >= '2026-04-01' AND time <= '2026-08-03'
    ORDER BY symbol, time
"""
df = pl.read_database(query, conn)

# Filter for only FNO symbols
df = df.filter(pl.col("symbol").is_in(list(fno_symbols)))

# Run math
df = df.with_columns([
    pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
    pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
    pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
    (pl.col("close") / pl.col("close").shift(5) - 1).over("symbol").alias("return_5d"),
    (pl.col("close") - pl.col("close").shift(1)).over("symbol").alias("price_diff")
])
df = df.with_columns([
    pl.when(pl.col("price_diff") > 0).then(pl.col("price_diff")).otherwise(0).rolling_mean(window_size=14).over("symbol").alias("gain_14"),
    pl.when(pl.col("price_diff") < 0).then(pl.col("price_diff").abs()).otherwise(0).rolling_mean(window_size=14).over("symbol").alias("loss_14")
])
df = df.with_columns([
    ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
    (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
    (100 - (100 / (1 + (pl.col("gain_14") / pl.col("loss_14"))))).alias("rsi_14"),
    ((pl.col("close") / pl.col("sma_50")) - 1).alias("dist_50sma"),
    (pl.col("std_20") / pl.col("close")).alias("hist_vol")
])
df = df.with_columns([
    pl.col("vol_surge").rolling_mean(window_size=3).over("symbol").alias("vol_surge_3d")
])

setups = df.filter(
    (pl.col("bbw") < 0.22) & 
    (pl.col("vol_surge") > 2.5) & 
    (pl.col("close") > pl.col("sma_20")) & 
    (pl.col("time") == pl.datetime(2026, 8, 3, time_zone="UTC"))
)

print(f"Found {len(setups)} FNO setups for 2026-08-03.")
for row in setups.iter_rows(named=True):
    print(f"FNO SETUP: {row['symbol']} | BBW: {row['bbw']:.2f} | Vol Surge: {row['vol_surge']:.2f}x")

conn.close()
