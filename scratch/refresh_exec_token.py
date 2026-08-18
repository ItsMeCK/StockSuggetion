import os
from dotenv import load_dotenv
load_dotenv()
from kiteconnect import KiteConnect

api_key = os.getenv("EXEC_KITE_API_KEY")
api_secret = os.getenv("EXEC_KITE_API_SECRET")
request_token = "thhc6XKkjDgY56BU4SQD1a6YyiDgUsCm"

try:
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    print(f"SUCCESS_EXEC_TOKEN:{access_token}")
except Exception as e:
    print(f"ERROR: {e}")
