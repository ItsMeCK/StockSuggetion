import os
import logging
import psycopg2
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

def init_ledger_schema():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5432")
    user = os.getenv("POSTGRES_USER", "quant")
    password = os.getenv("POSTGRES_PASSWORD", "quantpassword")
    db_name = os.getenv("POSTGRES_DB", "market_data")

    try:
        conn = psycopg2.connect(
            host=host, port=port, user=user, password=password, dbname=db_name
        )
        cur = conn.cursor()
        
        # Check if type exists before creating
        cur.execute("SELECT 1 FROM pg_type WHERE typname = 'trade_status';")
        if not cur.fetchone():
            cur.execute("""
            CREATE TYPE trade_status AS ENUM (
                'SIGNALED', 
                'AMO_PLACED', 
                'ACTIVE', 
                'STOP_HIT', 
                'PROFIT_BOOKED', 
                'EXPIRED_UNFILLED'
            );
            """)
            logging.info("Created trade_status ENUM.")

        cur.execute("""
        CREATE TABLE IF NOT EXISTS trade_events (
            event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            trade_id UUID NOT NULL, 
            ticker VARCHAR(20) NOT NULL,
            status trade_status NOT NULL,
            price DECIMAL(10,2),
            quantity INT,
            market_time TIMESTAMP WITH TIME ZONE, 
            system_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
            order_id VARCHAR(50), 
            notes TEXT
        );
        """)
        conn.commit()
        logging.info("Successfully initialized trade_events ledger.")
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"Schema Initialization Error: {e}")

if __name__ == "__main__":
    init_ledger_schema()
