import os
import sys
from kiteconnect import KiteConnect
from dotenv import load_dotenv, set_key

load_dotenv()
api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
api_secret = os.getenv("EXEC_KITE_API_SECRET", "").strip("'\"")

if not api_key or not api_secret:
    print("EXEC API credentials missing")
    sys.exit(1)

kite = KiteConnect(api_key=api_key)
request_token = "s2V4fCbt39BmRsfa2a3sriKzh1QcIgJW"

try:
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    print(f"Generated EXEC Access Token: {access_token[:10]}...")
    
    # Update local .env
    env_file = os.path.join(os.getcwd(), ".env")
    set_key(env_file, "EXEC_KITE_ACCESS_TOKEN", access_token)
    print("Local .env updated!")
except Exception as e:
    print(f"Failed to generate token: {e}")
