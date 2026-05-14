import os
import logging
import psycopg2
from kiteconnect import KiteConnect
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_synthetic_ingestion():
    api_key = 'anywvvfkcyjhhqiy'
    access_token = os.getenv('KITE_ACCESS_TOKEN').strip("'")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    
    target_symbols = []
    if os.path.exists("daily_scan_list.csv"):
        import csv
        with open("daily_scan_list.csv", "r") as f:
            reader = csv.DictReader(f)
            target_symbols = [row['symbol'] for row in reader]

    inst = kite.instruments("NSE")
    eq_inst = {i['tradingsymbol']: i['instrument_token'] for i in inst 
               if i['tradingsymbol'] in target_symbols}
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    for sym, token in eq_inst.items():
        try:
            logging.info(f"Generating Synthetic EOD for {sym}...")
            # Fetch day bar
            d = kite.historical_data(token, today, today, "day")
            if not d: continue
            row = d[0]
            
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO daily_ohlcv (time, symbol, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (time, symbol) DO UPDATE SET
                        open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                        close = EXCLUDED.close, volume = EXCLUDED.volume;
                """, (row['date'], sym, row['open'], row['high'], row['low'], row['close'], row['volume']))
            conn.commit()
            logging.info(f"Successfully injected {sym} for {today}")
        except Exception as e:
            logging.error(f"Failed {sym}: {e}")
            
    conn.close()

if __name__ == "__main__":
    run_synthetic_ingestion()
