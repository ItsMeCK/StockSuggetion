import os
import sys
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def update_env(key, value):
    env_path = ".env"
    with open(env_path, "r") as f:
        lines = f.readlines()
        
    found = False
    with open(env_path, "w") as f:
        for line in lines:
            if line.startswith(f"{key}="):
                f.write(f"{key}='{value}'\n")
                found = True
            else:
                f.write(line)
        if not found:
            f.write(f"{key}='{value}'\n")

def main():
    if len(sys.argv) < 2:
        print("Usage: python scratch/refresh_live_token.py <request_token>")
        sys.exit(1)
        
    req_token = sys.argv[1].strip()
    load_dotenv(override=True)
    
    data_k = (os.getenv("KITE_API_KEY") or "").strip("'\"")
    data_s = (os.getenv("KITE_API_SECRET") or "").strip("'\"")
    
    exec_k = (os.getenv("EXEC_KITE_API_KEY") or "").strip("'\"")
    exec_s = (os.getenv("EXEC_KITE_API_SECRET") or "").strip("'\"")
    
    print(f"Attempting token exchange for request_token: {req_token[:6]}...{req_token[-4:]}")
    
    success = False
    # Try 1: Data account
    if data_k and data_s:
        try:
            print(f"Trying Data App (API Key: {data_k[:4]}...)...")
            kite_data = KiteConnect(api_key=data_k)
            sess = kite_data.generate_session(req_token, api_secret=data_s)
            acc_tok = sess["access_token"]
            update_env("KITE_ACCESS_TOKEN", acc_tok)
            print(f"✅ SUCCESS for Data App! Access token: {acc_tok[:6]}...{acc_tok[-4:]}")
            kite_data.set_access_token(acc_tok)
            prof = kite_data.profile()
            print(f"✅ Profile verified: user_id={prof.get('user_id')}, user_name={prof.get('user_name')}")
            success = True
        except Exception as e:
            print(f"❌ Data App exchange failed: {e}")
            
    # Try 2: Exec account (if not success or if it belonged to exec)
    if not success and exec_k and exec_s:
        try:
            print(f"Trying Exec App (API Key: {exec_k[:4]}...)...")
            kite_exec = KiteConnect(api_key=exec_k)
            sess = kite_exec.generate_session(req_token, api_secret=exec_s)
            acc_tok = sess["access_token"]
            update_env("EXEC_KITE_ACCESS_TOKEN", acc_tok)
            print(f"✅ SUCCESS for Exec App! Access token: {acc_tok[:6]}...{acc_tok[-4:]}")
            kite_exec.set_access_token(acc_tok)
            prof = kite_exec.profile()
            print(f"✅ Profile verified: user_id={prof.get('user_id')}, user_name={prof.get('user_name')}")
            success = True
        except Exception as e:
            print(f"❌ Exec App exchange failed: {e}")
            
    if success:
        print("\n🎉 Token refresh complete and .env updated successfully!")
    else:
        print("\n❌ Failed to generate token for both accounts.")
        sys.exit(1)

if __name__ == "__main__":
    main()
