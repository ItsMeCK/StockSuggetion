import os
import sys
import datetime
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()

def fetch_intraday():
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN").strip("'")
    
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    symbols = ["GLAND", "SCI", "INDUSTOWER"]
    all_instruments = kite.instruments("NSE")
    
    symbol_to_token = {}
    for inst in all_instruments:
        if inst["tradingsymbol"] in symbols:
            symbol_to_token[inst["tradingsymbol"]] = inst["instrument_token"]
            
    print(f"Instrument Tokens: {symbol_to_token}")
    
    today = datetime.datetime.now().strftime("%Y-%m-%d")
    
    for sym, token in symbol_to_token.items():
        print(f"\n=== Hourly candles for {sym} ({today}) ===")
        try:
            # Fetch 60minute candles for today
            data = kite.historical_data(
                instrument_token=token,
                from_date=today,
                to_date=today,
                interval="60minute"
            )
            for row in data:
                print(f"Time: {row['date']}, O: {row['open']}, H: {row['high']}, L: {row['low']}, C: {row['close']}, V: {row['volume']}")
        except Exception as e:
            print(f"Error fetching {sym}: {e}")

if __name__ == "__main__":
    fetch_intraday()
