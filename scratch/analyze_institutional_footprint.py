import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST','localhost'), 
    port=os.getenv('DB_PORT','5432'), 
    user=os.getenv('POSTGRES_USER','quant'), 
    password=os.getenv('POSTGRES_PASSWORD','quantpassword'), 
    database=os.getenv('POSTGRES_DB','market_data')
)

query = """
SELECT symbol, time, open, high, low, close, volume, 
       close - open as candle_size,
       (close - open) / open * 100 as return_pct
FROM daily_ohlcv
WHERE symbol IN ('APOLLOHOSP', 'HONAUT', 'TATACOMM', 'TRITURBINE', 'GRASIM', 'AIIL', 'TIMKEN')
  AND time >= '2026-05-10'
ORDER BY symbol, time
"""
df = pd.read_sql(query, conn)
for symbol in df['symbol'].unique():
    print(f"\n--- {symbol} ---")
    print(df[df['symbol'] == symbol].tail(5))
