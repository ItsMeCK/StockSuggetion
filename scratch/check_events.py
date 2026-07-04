import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def print_trade_events():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        dbname=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    cur.execute("""
        SELECT trade_id, ticker, status, price, quantity, system_time, notes
        FROM trade_events
        ORDER BY system_time ASC
    """)
    rows = cur.fetchall()
    print(f"Total trade events: {len(rows)}")
    for r in rows:
        print(f"ID: {r[0]}, Ticker: {r[1]}, Status: {r[2]}, Price: {r[3]}, Qty: {r[4]}, Time: {r[5]}, Notes: {r[6][:100] if r[6] else ''}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    print_trade_events()
