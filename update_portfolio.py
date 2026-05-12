import os
import psycopg2
import json
import logging
from kiteconnect import KiteConnect
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    load_dotenv()
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )

def get_kite_client():
    load_dotenv()
    api_key = os.getenv('KITE_API_KEY')
    access_token = os.getenv('KITE_ACCESS_TOKEN')
    if api_key and access_token:
        try:
            kite = KiteConnect(api_key=api_key)
            kite.set_access_token(access_token)
            return kite
        except Exception as e:
            logging.error(f"Kite init error: {e}")
    return None

def update_portfolio():
    conn = get_db_connection()
    cur = conn.cursor()
    
    # 1. Transition SIGNALED -> ACTIVE (Simulated Automatic Entry)
    # Get all trades where the latest status is SIGNALED
    cur.execute("""
        WITH latest_status AS (
            SELECT trade_id, ticker, status, price, quantity, notes,
                   ROW_NUMBER() OVER(PARTITION BY trade_id ORDER BY system_time DESC) as rn
            FROM trade_events
        )
        SELECT trade_id, ticker, price, quantity, notes 
        FROM latest_status 
        WHERE rn = 1 AND status = 'SIGNALED'
    """)
    signaled_trades = cur.fetchall()
    
    if signaled_trades:
        kite = get_kite_client()
        tickers_to_fetch = [f"NSE:{row[1]}" for row in signaled_trades]
        quotes = {}
        if kite:
            try:
                quotes = kite.quote(tickers_to_fetch)
            except Exception as e:
                logging.error(f"Failed to fetch quotes for activation: {e}")

        for trade_id, ticker, suggested_price, quantity, notes in signaled_trades:
            kite_symbol = f"NSE:{ticker}"
            # Use the actual opening price from today's quote if available, 
            # otherwise fallback to suggested_price
            buy_price = suggested_price
            if kite_symbol in quotes:
                buy_price = quotes[kite_symbol].get('ohlc', {}).get('open', suggested_price)
                
            logging.info(f"Transitioning {ticker} to ACTIVE at Open Price: {buy_price}.")
            cur.execute("""
                INSERT INTO trade_events (trade_id, ticker, status, price, quantity, notes)
                VALUES (%s, %s, 'ACTIVE', %s, %s, %s)
            """, (trade_id, ticker, buy_price, quantity, notes))
    
    conn.commit()
    logging.info(f"Activated {len(signaled_trades)} trades.")

    # 2. Check ACTIVE trades against live quotes for Target/Stop-Loss
    cur.execute("""
        WITH latest_status AS (
            SELECT trade_id, ticker, status, price as entry_price, notes,
                   ROW_NUMBER() OVER(PARTITION BY trade_id ORDER BY system_time DESC) as rn
            FROM trade_events
        )
        SELECT trade_id, ticker, entry_price, notes 
        FROM latest_status 
        WHERE rn = 1 AND status = 'ACTIVE'
    """)
    active_trades = cur.fetchall()
    
    if not active_trades:
        logging.info("No active trades to monitor.")
        cur.close()
        conn.close()
        return

    kite = get_kite_client()
    if not kite:
        logging.error("Cannot fetch live quotes to check active trades. Kite client failed.")
        cur.close()
        conn.close()
        return
        
    tickers_to_fetch = [f"NSE:{row[1]}" for row in active_trades]
    try:
        quotes = kite.quote(tickers_to_fetch)
    except Exception as e:
        logging.error(f"Failed to fetch quotes from Kite: {e}")
        cur.close()
        conn.close()
        return

    closed_count = 0
    for trade_id, ticker, entry_price, notes in active_trades:
        kite_symbol = f"NSE:{ticker}"
        if kite_symbol not in quotes:
            continue
            
        live_price = quotes[kite_symbol].get('last_price', 0.0)
        
        try:
            notes_dict = json.loads(notes)
        except:
            notes_dict = {}
            
        target = notes_dict.get('target', float('inf'))
        stop_loss = notes_dict.get('stop_loss', 0.0)
        
        status_update = None
        if live_price >= target and target > 0:
            status_update = 'CLOSED_WIN'
            logging.info(f"{ticker} hit TARGET at {live_price}. (Entry: {entry_price})")
        elif live_price <= stop_loss and stop_loss > 0:
            status_update = 'CLOSED_LOSS'
            logging.info(f"{ticker} hit STOP-LOSS at {live_price}. (Entry: {entry_price})")
            
        if status_update:
            notes_dict['exit_price'] = live_price
            cur.execute("""
                INSERT INTO trade_events (trade_id, ticker, status, price, quantity, notes)
                VALUES (%s, %s, %s, %s, 0, %s)
            """, (trade_id, ticker, status_update, live_price, json.dumps(notes_dict)))
            closed_count += 1
            
    conn.commit()
    cur.close()
    conn.close()
    logging.info(f"Checked {len(active_trades)} active trades. Closed {closed_count}.")

if __name__ == "__main__":
    logging.info("Running Daily Portfolio Update...")
    update_portfolio()
