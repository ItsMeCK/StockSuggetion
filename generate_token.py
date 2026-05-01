import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

api_key = os.getenv("KITE_API_KEY")
api_secret = os.getenv("KITE_API_SECRET")
request_token = "XH7keHYmqYmLMQchcPZhqFLdNXAtGxvY"

try:
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    
    # Read .env and replace KITE_ACCESS_TOKEN
    with open(".env", "r") as f:
        lines = f.readlines()
        
    with open(".env", "w") as f:
        for line in lines:
            if line.startswith("KITE_ACCESS_TOKEN="):
                f.write(f"KITE_ACCESS_TOKEN='{access_token}'\n")
            else:
                f.write(line)
                
    print(f"Success! Access Token generated and saved to .env")
except Exception as e:
    print(f"Error: {e}")
