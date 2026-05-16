import os
from kiteconnect import KiteConnect
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv("KITE_API_KEY")
api_secret = os.getenv("KITE_API_SECRET")
request_token = "C61SohvT2oEjl5a3KMa2yDT710p07I3F"

kite = KiteConnect(api_key=api_key)
try:
    data = kite.generate_session(request_token, api_secret=api_secret)
    print(f"ACCESS_TOKEN: {data['access_token']}")
except Exception as e:
    print(f"Error: {e}")
