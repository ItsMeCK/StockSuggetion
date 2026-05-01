import psycopg2
import os

def check_symbols():
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port='5432',
        user='quant',
        password='quantpassword',
        database='market_data'
    )
    cur = conn.cursor()
    symbols = ['DATAPATTNS', 'MAZDOCK', 'CGPOWER']
    print(f"{'Symbol':<12} | {'Count':<8} | {'Start Date':<12} | {'End Date':<12}")
    print("-" * 55)
    for s in symbols:
        cur.execute("SELECT COUNT(*), MIN(time), MAX(time) FROM daily_ohlcv WHERE symbol = %s", (s,))
        row = cur.fetchone()
        count = row[0]
        start = row[1].strftime('%Y-%m-%d') if row[1] else "N/A"
        end = row[2].strftime('%Y-%m-%d') if row[2] else "N/A"
        print(f"{s:<12} | {count:<8} | {start:<12} | {end:<12}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    check_symbols()
