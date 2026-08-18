import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()
kite_data = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
kite_data.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

target_date = datetime.now().strftime("%Y-%m-%d")
lookback_start = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")

all_instruments = kite_data.instruments("NSE")
targets = ['DLF', 'LODHA', 'PRESTIGE', 'SUZLON']
tokens = {}
for inst in all_instruments:
    if inst['tradingsymbol'] in targets:
        tokens[inst['tradingsymbol']] = inst['instrument_token']

print("--- 60 Minute Data ---")
for sym, token in tokens.items():
    print(f"\n{sym}:")
    data_60 = kite_data.historical_data(token, lookback_start, target_date, "60minute")
    for d in data_60[-4:]: # last 4 hours
        print(f"  {d['date']}: O:{d['open']} H:{d['high']} L:{d['low']} C:{d['close']} V:{d['volume']}")

print("\n--- 15 Minute Data ---")
for sym, token in tokens.items():
    print(f"\n{sym}:")
    data_15 = kite_data.historical_data(token, lookback_start, target_date, "15minute")
    for d in data_15[-16:]: # last 4 hours
        print(f"  {d['date']}: O:{d['open']} H:{d['high']} L:{d['low']} C:{d['close']} V:{d['volume']}")
