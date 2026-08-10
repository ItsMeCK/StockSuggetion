import os
import csv
import time
import logging
import polars as pl
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class IntradayIngestionEngine:
    def __init__(self):
        self.api_key = os.getenv("KITE_API_KEY", "MOCK_KEY").strip("'\"")
        self.access_token = os.getenv("KITE_ACCESS_TOKEN", "MOCK_TOKEN").strip("'\"")
        self.kite = KiteConnect(api_key=self.api_key)
        self.kite.set_access_token(self.access_token)
        self.data_dir = "data"
        os.makedirs(self.data_dir, exist_ok=True)
        self.parquet_path = os.path.join(self.data_dir, "intraday_ohlcv.parquet")

    def fetch_data(self):
        # We need end date (today) and lookback start (e.g. 15 calendar days = ~10 trading days = ~60 trading hours)
        # This is plenty of data to calculate a 20-period SMA on an hourly chart.
        target_date = datetime.now().strftime("%Y-%m-%d")
        lookback_start = (datetime.now() - timedelta(days=15)).strftime("%Y-%m-%d")

        target_symbols = []
        universe_path = "pipeline/master_universe.csv"
        if os.path.exists(universe_path):
            with open(universe_path, "r") as f:
                reader = csv.reader(f)
                target_symbols = [row[0] for row in reader if row and row[0] != 'Symbol']
        
        logging.info("Fetching full NSE instruments list...")
        all_instruments = None
        for attempt in range(3):
            try:
                all_instruments = self.kite.instruments("NSE")
                break
            except Exception as e:
                logging.warning(f"Failed to fetch instruments (Attempt {attempt+1}): {e}")
                time.sleep(2)
                
        if not all_instruments:
            logging.error("Could not fetch instruments from Kite API. Exiting.")
            return
        full_market_list = [
            {"symbol": inst['tradingsymbol'], "token": inst['instrument_token']} 
            for inst in all_instruments 
            if inst['tradingsymbol'] in target_symbols
        ]
        
        all_records = []
        logging.info(f"Starting 60minute data ingestion for {len(full_market_list)} symbols...")
        
        for i, asset in enumerate(full_market_list):
            if i % 50 == 0:
                logging.info(f"Progress: {i}/{len(full_market_list)} symbols fetched...")
            try:
                # Use 60minute interval to get hourly candles
                data = self.kite.historical_data(asset['token'], lookback_start, target_date, interval="60minute")
                for row in data:
                    all_records.append({
                        "time": row['date'], # This is a datetime object
                        "symbol": asset['symbol'],
                        "open": row['open'],
                        "high": row['high'],
                        "low": row['low'],
                        "close": row['close'],
                        "volume": row['volume']
                    })
            except Exception as e:
                logging.warning(f"Failed to fetch {asset['symbol']}: {e}")
            
            # Comply with 3 requests per second Kite API rate limits
            time.sleep(0.34)
            
        if all_records:
            df = pl.DataFrame(all_records)
            df.write_parquet(self.parquet_path)
            logging.info(f"✅ Successfully saved {len(df)} hourly records to {self.parquet_path}")
        else:
            logging.error("❌ No records fetched.")

if __name__ == "__main__":
    engine = IntradayIngestionEngine()
    engine.fetch_data()
