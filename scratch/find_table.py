import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def find_table():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    cur.execute("SELECT schemaname, tablename FROM pg_catalog.pg_tables")
    tables = cur.fetchall()
    print("All tables in market_data:")
    for schema, table in tables:
        print(f" - {schema}.{table}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    find_table()
