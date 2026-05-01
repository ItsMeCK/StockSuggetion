import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def apply_extra_schemas():
    # 1. market_data (5432)
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "quant"),
            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
            database=os.getenv("POSTGRES_DB", "market_data")
        )
        cur = conn.cursor()
        with open("schema/bitemporal_ledger.sql", "r") as f:
            sql = f.read()
        
        cur.execute(sql)
        conn.commit()
        print("Applied bitemporal_ledger.sql to market_data")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to apply bitemporal_ledger.sql: {e}")

    # 2. sovereign_state (5433)
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port="5433",
            user="agent",
            password="agentpassword",
            database="sovereign_state"
        )
        cur = conn.cursor()
        with open("schema/experience_memory.sql", "r") as f:
            sql = f.read()
        
        cur.execute(sql)
        conn.commit()
        print("Applied experience_memory.sql to sovereign_state")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"Failed to apply experience_memory.sql: {e}")

if __name__ == "__main__":
    apply_extra_schemas()
