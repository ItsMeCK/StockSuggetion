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
        target_date = datetime.now().strftime("%Y-%m-%d")
        
        # Default lookback if no parquet exists
        default_lookback_days = 40
        lookback_start = (datetime.now() - timedelta(days=default_lookback_days)).strftime("%Y-%m-%d")
        
        existing_df = None
        if os.path.exists(self.parquet_path):
            try:
                existing_df = pl.read_parquet(self.parquet_path)
                if existing_df.height > 0:
                    max_time = existing_df.select(pl.max("time")).item()
                    if max_time:
                        # Subtract 1 day for safety to catch any late data. Deduplication handles overlap.
                        lookback_start = (max_time - timedelta(days=1)).strftime("%Y-%m-%d")
                        logging.info(f"Existing data found up to {max_time}. Running incremental fetch from {lookback_start}.")
            except Exception as e:
                logging.warning(f"Failed to read existing parquet, doing full fetch: {e}")
                existing_df = None

        target_symbols = []
        universe_path = "pipeline/master_universe.csv"
        if os.path.exists(universe_path):
            with open(universe_path, "r") as f:
                reader = csv.reader(f)
                target_symbols = [row[0] for row in reader if row and row[0] != 'Symbol']
        
        logging.info("Fetching full NSE instruments list...")
        all_instruments = None
        nfo_instruments = None
        for attempt in range(3):
            try:
                all_instruments = self.kite.instruments("NSE")
                nfo_instruments = self.kite.instruments("NFO")
                break
            except Exception as e:
                logging.warning(f"Failed to fetch instruments (Attempt {attempt+1}): {e}")
                time.sleep(2)
                
        if not all_instruments or not nfo_instruments:
            logging.error("Could not fetch instruments from Kite API. Exiting.")
            return
            
        fo_symbols = set(inst['name'] for inst in nfo_instruments if inst['segment'] == 'NFO-OPT')
            
        full_market_list = [
            {"symbol": inst['tradingsymbol'], "token": inst['instrument_token']} 
            for inst in all_instruments 
            if inst['tradingsymbol'] in target_symbols and inst['tradingsymbol'] in fo_symbols
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
            import pytz
            ist_tz = pytz.timezone('Asia/Kolkata')
            now_ist = datetime.now(ist_tz)
            
            valid_records = []
            for r in all_records:
                c_time = r["time"]
                if c_time.tzinfo is None:
                    c_time = ist_tz.localize(c_time)
                
                if now_ist >= c_time + timedelta(minutes=60):
                    valid_records.append(r)
            
            if not valid_records:
                logging.info("No completed new records fetched.")
                return
                
            new_df = pl.DataFrame(valid_records)
            
            if existing_df is not None:
                # Align column order before concatenation to avoid schema errors
                new_df = new_df.select(existing_df.columns)
                
                # Merge old and new
                merged_df = pl.concat([existing_df, new_df])
                # Deduplicate keeping the latest fetched record for that specific hour
                merged_df = merged_df.unique(subset=["symbol", "time"], keep="last")
                # Keep only the last 250 candles (~40 trading days) per symbol to prevent infinite bloat
                merged_df = merged_df.sort(["symbol", "time"])
                merged_df = merged_df.group_by("symbol").tail(250)
                final_df = merged_df
            else:
                final_df = new_df
                
            final_df = final_df.sort(["symbol", "time"])
            final_df.write_parquet(self.parquet_path)
            logging.info(f"✅ Successfully saved {final_df.height} hourly records to {self.parquet_path}")
        else:
            logging.info("No new records fetched.")

if __name__ == "__main__":
    engine = IntradayIngestionEngine()
    engine.fetch_data()
