import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def list_tables_sovereign():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port="5433",
        user="agent",
        password="agentpassword",
        database="sovereign_state"
    )
    cur = conn.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    tables = cur.fetchall()
    print("Tables in sovereign_state:")
    for table in tables:
        print(f" - {table[0]}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    list_tables_sovereign()
