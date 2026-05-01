import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def check_data():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    cur.execute("SELECT MIN(time), MAX(time), COUNT(*) FROM daily_ohlcv")
    row = cur.fetchone()
    print(f"Market Data: Start={row[0]}, End={row[1]}, Total Rows={row[2]}")
    
    cur.execute("SELECT DISTINCT time::date FROM daily_ohlcv WHERE time >= '2026-02-01' AND time <= '2026-04-30' ORDER BY time ASC")
    dates = cur.fetchall()
    print(f"Trading days in Feb-April: {len(dates)}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_data()
