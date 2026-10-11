import os
import sys
from kiteconnect import KiteConnect
from dotenv import load_dotenv

def generate_token(request_token):
    load_dotenv()
    
    # We update the Data API token
    api_key = os.getenv("KITE_API_KEY")
    api_secret = os.getenv("KITE_API_SECRET")
    
    if not api_key or not api_secret:
        print("Error: KITE_API_KEY or KITE_API_SECRET not found in .env")
        sys.exit(1)
        
    kite = KiteConnect(api_key=api_key)
    
    try:
        data = kite.generate_session(request_token, api_secret=api_secret)
        access_token = data["access_token"]
        print(f"✅ Successfully generated access token: {access_token}")
        
        # Update .env
        env_path = "/Users/poonamsalke/.gemini/antigravity/scratch/midnight_sovereign/.env"
        with open(env_path, "r") as f:
            lines = f.readlines()
            
        with open(env_path, "w") as f:
            for line in lines:
                if line.startswith("KITE_ACCESS_TOKEN="):
                    f.write(f"KITE_ACCESS_TOKEN={access_token}\n")
                elif line.startswith("EXEC_KITE_ACCESS_TOKEN="):
                    f.write(f"EXEC_KITE_ACCESS_TOKEN={access_token}\n")  # If using same account
                else:
                    f.write(line)
                    
        print("✅ Successfully updated .env with new token!")
    except Exception as e:
        print(f"❌ Error generating session: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_token.py <request_token>")
        sys.exit(1)
    generate_token(sys.argv[1])
