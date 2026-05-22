import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def update_env(key, value):
    with open(".env", "r") as f:
        lines = f.readlines()
        
    with open(".env", "w") as f:
        for line in lines:
            if line.startswith(f"{key}="):
                f.write(f"{key}='{value}'\n")
            else:
                f.write(line)

load_dotenv()

# Data App
data_api_key = os.getenv("KITE_API_KEY").strip("'\"")
data_api_secret = os.getenv("KITE_API_SECRET").strip("'\"")
data_request_token = "zSL5s0jQPXJZBS6IutY7iGvkmHobfIrZ"

try:
    print("Generating session for Data Account...")
    kite = KiteConnect(api_key=data_api_key)
    data = kite.generate_session(data_request_token, api_secret=data_api_secret)
    access_token = data["access_token"]
    update_env("KITE_ACCESS_TOKEN", access_token)
    print("Success! Data Access Token generated and saved to .env")
except Exception as e:
    print(f"Data Token Error: {e}")

# Execution App
exec_api_key = os.getenv("EXEC_KITE_API_KEY").strip("'\"")
exec_api_secret = os.getenv("EXEC_KITE_API_SECRET").strip("'\"")
exec_request_token = "xZTnkY949XGfQC6cPdtb2VctR1S3avIE"

try:
    print("Generating session for Execution Account...")
    kite = KiteConnect(api_key=exec_api_key)
    data = kite.generate_session(exec_request_token, api_secret=exec_api_secret)
    access_token = data["access_token"]
    update_env("EXEC_KITE_ACCESS_TOKEN", access_token)
    print("Success! Execution Access Token generated and saved to .env")
except Exception as e:
    print(f"Execution Token Error: {e}")
