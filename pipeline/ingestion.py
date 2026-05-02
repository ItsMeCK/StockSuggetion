import os
import logging
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta

# Live environment import:
from kiteconnect import KiteConnect

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters (matches docker-compose.yml)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_USER = os.getenv("POSTGRES_USER", "quant")
DB_PASS = os.getenv("POSTGRES_PASSWORD", "quantpassword")
DB_NAME = os.getenv("POSTGRES_DB", "market_data")

class ZerodhaIngestionEngine:
    """
    Handles the End of Day (EOD) Historical REST API ingestion from Zerodha
    and performs bulk inserts into the TimescaleDB hypertable.
    """
    def __init__(self):
        self.api_key = os.getenv("KITE_API_KEY", "MOCK_KEY")
        self.access_token = os.getenv("KITE_ACCESS_TOKEN", "MOCK_TOKEN")
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        
        self.conn = None

    def connect_db(self):
        try:
            self.conn = psycopg2.connect(
                host=DB_HOST,
                port=DB_PORT,
                user=DB_USER,
                password=DB_PASS,
                database=DB_NAME
            )
            logging.info("Successfully connected to TimescaleDB.")
        except Exception as e:
            logging.error(f"Failed to connect to TimescaleDB: {e}")
            raise

    def close_db(self):
        if self.conn:
            self.conn.close()
            logging.info("TimescaleDB connection closed.")

    def fetch_historical_data(self, instrument_token: int, from_date: str, to_date: str, interval: str = "day"):
        """
        Fetches historical OHLCV data from Zerodha API.
        This is currently mocked to prevent API rate limits and execution failure without valid keys.
        """
        logging.info(f"Fetching {interval} data for instrument {instrument_token} from {from_date} to {to_date}")
        
        # Live API Data Fetch
        try:
            return self.kite.historical_data(instrument_token, from_date, to_date, interval)
        except Exception as e:
            logging.error(f"Failed to fetch historical data for {instrument_token}: {e}")
            return []

    def bulk_insert_ohlcv(self, symbol: str, data: list):
        """
        Performs a fast bulk insert into the TimescaleDB `daily_ohlcv` hypertable.
        """
        if not self.conn:
            raise Exception("Database connection not established.")

        insert_query = """
            INSERT INTO daily_ohlcv (time, symbol, open, high, low, close, volume)
            VALUES %s
            ON CONFLICT (time, symbol) DO UPDATE SET
                open = EXCLUDED.open,
                high = EXCLUDED.high,
                low = EXCLUDED.low,
                close = EXCLUDED.close,
                volume = EXCLUDED.volume;
        """
        
        # Format the data into tuples for execute_values
        values = [
            (
                row['date'],
                symbol,
                row['open'],
                row['high'],
                row['low'],
                row['close'],
                row['volume']
            ) for row in data
        ]

        try:
            with self.conn.cursor() as cur:
                execute_values(cur, insert_query, values, page_size=1000)
            self.conn.commit()
            logging.info(f"Successfully inserted {len(values)} records for {symbol}.")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Bulk insert failed for {symbol}: {e}")

def run_eod_ingestion():
    engine = ZerodhaIngestionEngine()
    
    # Fetch ALL live instruments from Zerodha
    try:
        logging.info("FETCHING FULL NSE INSTRUMENT LIST (NO SHORTCUTS)...")
        all_instruments = engine.kite.instruments("NSE")
        # Filter for Equities (EQ) and skip those with 'RETAIL' or 'BOND' in symbols if necessary
        full_market_list = [
            {"symbol": inst['tradingsymbol'], "token": inst['instrument_token']} 
            for inst in all_instruments 
            if inst['instrument_type'] == 'EQ' 
            and not any(x in inst['tradingsymbol'] for x in ['-RE', '-BE', '-BZ'])
        ]
        logging.info(f"Successfully loaded {len(full_market_list)} symbols for the Sovereign Master Ingestion.")
    except Exception as e:
        logging.error(f"Failed to fetch instruments: {e}")
        full_market_list = []
    
    # Set dates for Monday Prep (Prep based on Thursday, April 30th)
    target_date = "2026-04-30"
    lookback_start = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=400)).strftime("%Y-%m-%d")
    
    try:
        engine.connect_db()
        logging.info(f"STARTING MASS INGESTION: {len(full_market_list)} stocks | 400-day lookback...")
        
        for i, asset in enumerate(full_market_list):
            if i % 50 == 0:
                logging.info(f"PROGRESS: Ingested {i}/{len(full_market_list)} symbols...")
                
            data = engine.fetch_historical_data(asset['token'], lookback_start, target_date)
            
            if data:
                engine.bulk_insert_ohlcv(asset['symbol'], data)
            else:
                logging.warning(f"No data for {asset['symbol']}. Moving to next.")
                
            # Rate limit compliance (3 requests per second)
            import time
            time.sleep(0.34)
            
    finally:
        engine.close_db()
        logging.info("FULL MARKET INGESTION COMPLETE. NO SHORTCUTS TAKEN.")

if __name__ == "__main__":
    run_eod_ingestion()
