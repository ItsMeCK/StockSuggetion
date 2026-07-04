import os
import psycopg2
import logging
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

def close_all_trades():
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    
    cur = conn.cursor()
    
    # 1. Fetch all currently open/active trades
    query = """
        WITH latest_status AS (
            SELECT trade_id, ticker, status, price, quantity, order_id,
                   ROW_NUMBER() OVER(PARTITION BY trade_id ORDER BY system_time DESC) as rn
            FROM trade_events
        )
        SELECT trade_id, ticker, price, quantity, order_id
        FROM latest_status 
        WHERE rn = 1 AND status IN ('ACTIVE', 'AMO_PLACED');
    """
    cur.execute(query)
    open_trades = cur.fetchall()
    
    if not open_trades:
        print("No open or pending trades found in the ledger to close.")
        cur.close()
        conn.close()
        return
        
    print(f"Found {len(open_trades)} open/pending trades. Closing them now...")
    
    # 2. Insert closing events for all of them
    insert_query = """
        INSERT INTO trade_events (trade_id, ticker, status, price, quantity, order_id, notes)
        VALUES (%s, %s, 'PROFIT_BOOKED', %s, %s, %s, 'Closed manually by user request')
    """
    
    for trade_id, ticker, price, quantity, order_id in open_trades:
        cur.execute(insert_query, (trade_id, ticker, price, quantity, order_id))
        print(f" -> Closed trade for {ticker} (Trade ID: {trade_id})")
        
    conn.commit()
    cur.close()
    conn.close()
    print("All open/pending trades successfully closed in the ledger.")

if __name__ == "__main__":
    close_all_trades()
