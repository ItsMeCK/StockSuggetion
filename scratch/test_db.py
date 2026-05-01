import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def test_conn():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "quant"),
            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
            database=os.getenv("POSTGRES_DB", "market_data")
        )
        print("Connected to market_data")
        conn.close()
    except Exception as e:
        print(f"Failed to connect to market_data: {e}")

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port="5433",
            user="agent",
            password="agentpassword",
            database="sovereign_state"
        )
        print("Connected to sovereign_state")
        conn.close()
    except Exception as e:
        print(f"Failed to connect to sovereign_state: {e}")

if __name__ == "__main__":
    test_conn()
