import os
import psycopg2
import polars as pl
import logging
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)

def analyze_axis():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    
    # Query daily OHLCV for AXISBANK for the last 15 days
    query = """
        SELECT time, open, high, low, close, volume 
        FROM daily_ohlcv 
        WHERE symbol = 'AXISBANK' 
        ORDER BY time DESC 
        LIMIT 20
    """
    
    df = pl.read_database(query, conn)
    conn.close()
    
    print("\n=== AXISBANK RECENT DAILY DATA ===")
    print(df)
    
    # Calculate some basic technicals
    closes = df["close"].to_list()[::-1] # reverse to chronological order
    if len(closes) >= 20:
        sma_20 = sum(closes) / 20
        print(f"\nSMA-20: {sma_20:.2f}")
        print(f"Current Close vs SMA-20: {closes[-1]:.2f} vs {sma_20:.2f}")
        
if __name__ == "__main__":
    analyze_axis()
