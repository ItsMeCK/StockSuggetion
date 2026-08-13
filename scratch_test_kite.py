import os
import polars as pl
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()
kite = KiteConnect(api_key=os.getenv("KITE_API_KEY", "").strip("'\""))
kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN", "").strip("'\""))
try:
    print(kite.profile())
except Exception as e:
    print("API Error:", e)
