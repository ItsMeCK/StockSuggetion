import os
import sys
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()
api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")

if not api_key or not access_token:
    print("Missing EXEC_KITE credentials in .env")
    sys.exit(1)

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

try:
    print("Attempting to place a dummy out-of-the-money order to test IP...")
    order_id = kite.place_order(
        variety=kite.VARIETY_REGULAR,
        exchange=kite.EXCHANGE_NSE,
        tradingsymbol="GOLDBEES",
        transaction_type=kite.TRANSACTION_TYPE_BUY,
        quantity=1,
        product=kite.PRODUCT_CNC,
        order_type=kite.ORDER_TYPE_LIMIT,
        price=1.0,  # Will never execute
        validity=kite.VALIDITY_DAY
    )
    print(f"SUCCESS: Dummy order placed successfully! Order ID: {order_id}")
    
    # Instantly cancel it just in case
    print("Cancelling the dummy order...")
    kite.cancel_order(
        variety=kite.VARIETY_REGULAR,
        order_id=order_id
    )
    print("Cancelled successfully.")
except Exception as e:
    print(f"FAILED: {e}")
