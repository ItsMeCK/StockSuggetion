import os
import psycopg2
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO)
load_dotenv()

def init_ledger():
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "quant"),
            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
            database=os.getenv("POSTGRES_DB", "market_data")
        )
        cur = conn.cursor()
        
        # Create decision_ledger table
        # Columns: id, time, symbol, mode, score, status, agent_opinions (JSONB)
        create_table_query = """
        CREATE TABLE IF NOT EXISTS decision_ledger (
            id SERIAL PRIMARY KEY,
            time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            symbol VARCHAR(20) NOT NULL,
            mode VARCHAR(20) NOT NULL,
            score INTEGER,
            status VARCHAR(20),
            agent_opinions JSONB,
            price_at_scan FLOAT
        );
        
        CREATE INDEX IF NOT EXISTS idx_ledger_symbol ON decision_ledger(symbol);
        CREATE INDEX IF NOT EXISTS idx_ledger_time ON decision_ledger(time);
        """
        
        cur.execute(create_table_query)
        conn.commit()
        logging.info("✅ decision_ledger table initialized successfully.")
        
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"❌ Failed to initialize decision_ledger: {e}")

if __name__ == "__main__":
    init_ledger()
