import os
import sys
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()
api_key = os.getenv("KITE_API_KEY")
access_token = os.getenv("KITE_ACCESS_TOKEN").strip("'")

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

# Get today's close prices for the symbols
symbols = ["AIIL", "APOLLOHOSP", "GRAPHITE", "ANGELONE", "GRASIM", "IRB", "HONAUT"]

# Fetch OHLC
quotes = kite.quote([f"NSE:{s}" for s in symbols])

for sym in symbols:
    if f"NSE:{sym}" in quotes:
        data = quotes[f"NSE:{sym}"]
        close_price = data['last_price']
        print(f"{sym}: CLOSE = {close_price}")
    else:
        print(f"{sym}: Not found in quotes")
