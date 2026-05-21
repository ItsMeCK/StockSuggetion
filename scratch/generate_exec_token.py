import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

api_key = os.getenv("EXEC_KITE_API_KEY").strip("'\"")
api_secret = os.getenv("EXEC_KITE_API_SECRET").strip("'\"")
request_token = "e17sV4GsRl70WprjnIwqbwxuStnQfJ1K"

try:
    print(f"Generating session for Execution Account... API Key: {api_key}")
    kite = KiteConnect(api_key=api_key)
    data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = data["access_token"]
    
    # Read .env and replace EXEC_KITE_ACCESS_TOKEN
    with open(".env", "r") as f:
        lines = f.readlines()
        
    with open(".env", "w") as f:
        for line in lines:
            if line.startswith("EXEC_KITE_ACCESS_TOKEN="):
                f.write(f"EXEC_KITE_ACCESS_TOKEN='{access_token}'\n")
            else:
                f.write(line)
                
    print(f"Success! EXEC Access Token generated and saved to .env")
except Exception as e:
    print(f"Error: {e}")
