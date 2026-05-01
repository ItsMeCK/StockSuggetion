import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def list_dbs():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database="postgres"
    )
    cur = conn.cursor()
    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
    dbs = cur.fetchall()
    print("Databases on 5432:")
    for db in dbs:
        print(f" - {db[0]}")
    cur.close()
    conn.close()

if __name__ == "__main__":
    list_dbs()
