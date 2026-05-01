import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def list_tables():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cur.fetchall()
    print("Tables in market_data:")
    for table in tables:
        print(f" - {table[0]}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    list_tables()
