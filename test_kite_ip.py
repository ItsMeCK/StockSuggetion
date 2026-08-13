import os
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()
try:
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY").strip("'\""))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'\""))
    # Just fetch profile to test if IP is blocked
    profile = kite.profile()
    print("SUCCESS: Local IP is still whitelisted!")
except Exception as e:
    print(f"ERROR: {e}")
