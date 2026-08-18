import os
from dotenv import load_dotenv
load_dotenv()
from kiteconnect import KiteConnect

kite = KiteConnect(api_key=os.getenv("EXEC_KITE_API_KEY"))
kite.set_access_token(os.getenv("EXEC_KITE_ACCESS_TOKEN"))

positions = kite.positions()['net']
print("--- KITE POSITIONS ---")
for p in positions:
    print(f"Symbol: {p['tradingsymbol']}, Qty: {p['quantity']}, Avg Price: {p['average_price']}")

