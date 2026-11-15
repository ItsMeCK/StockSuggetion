#!/usr/bin/env python3
import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

# Verify Exec Account
exec_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
exec_tok = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
k_exec = KiteConnect(api_key=exec_key)
k_exec.set_access_token(exec_tok)
p_exec = k_exec.profile()
m_exec = k_exec.margins(segment="equity")
print(f"✅ EXEC ACCOUNT: {p_exec.get('user_id')} ({p_exec.get('user_name')}) | Available Live Balance: ₹{m_exec.get('available', {}).get('live_balance')}")

# Verify Data Account
data_key = os.getenv("KITE_API_KEY", "").strip("'\"")
data_tok = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
k_data = KiteConnect(api_key=data_key)
k_data.set_access_token(data_tok)
p_data = k_data.profile()
q_data = k_data.quote(["NSE:INFY"])
print(f"✅ DATA ACCOUNT: {p_data.get('user_id')} ({p_data.get('user_name')}) | INFY LTP: ₹{q_data.get('NSE:INFY', {}).get('last_price')}")
print("🎯 ALL TOKENS 100% OPERATIONAL!")
