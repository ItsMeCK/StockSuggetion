"""
scripts/place_policybzr_sl.py

Places broker-side Stop-Loss order for POLICYBZR26NOV1080CE on HU0092.
Parameters:
- Tradingsymbol: POLICYBZR26NOV1080CE
- Transaction: SELL
- Quantity: 350
- Order Type: SL
- Trigger Price: ₹46.50
- Limit Price: ₹46.00
- Product: NRML
"""

import os
import sys
import json
import time
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def place_policybzr_sl():
    load_dotenv()
    exec_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    data_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    data_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")

    kite_exec = KiteConnect(api_key=exec_key)
    kite_exec.set_access_token(exec_token)

    kite_data = KiteConnect(api_key=data_key)
    kite_data.set_access_token(data_token)

    # 1. Fetch live quote
    q = kite_data.quote(["NFO:POLICYBZR26NOV1080CE"]).get("NFO:POLICYBZR26NOV1080CE", {})
    ltp = q.get("last_price", 50.0)
    print(f"📊 Live POLICYBZR26NOV1080CE LTP: ₹{ltp:.2f}")

    # Ensure trigger price is below LTP for a sell SL
    trigger_px = 46.50
    limit_px = 46.00

    if ltp <= trigger_px:
        # If price already fell below 46.50, place trigger slightly below current LTP
        trigger_px = round((ltp - 1.0) / 0.05) * 0.05
        limit_px = round((trigger_px - 0.50) / 0.05) * 0.05
        print(f"⚠️ Adjusted Trigger to ₹{trigger_px:.2f} due to current LTP")

    print(f"🚀 Placing SL Order: POLICYBZR26NOV1080CE | Qty: 350 | Trigger: ₹{trigger_px:.2f} | Limit: ₹{limit_px:.2f}")

    try:
        order_id = kite_exec.place_order(
            variety=kite_exec.VARIETY_REGULAR,
            exchange="NFO",
            tradingsymbol="POLICYBZR26NOV1080CE",
            transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
            quantity=350,
            order_type=kite_exec.ORDER_TYPE_SL,
            price=limit_px,
            trigger_price=trigger_px,
            product=kite_exec.PRODUCT_NRML,
            validity=kite_exec.VALIDITY_DAY,
            tag="sovereign_sl"
        )
        print(f"✅ Stop-Loss Placed Successfully! Order ID: {order_id}")
        
        time.sleep(1)
        orders = kite_exec.orders()
        for o in orders:
            if o.get("order_id") == str(order_id):
                print(f"📋 Order Verification: ID: {o['order_id']} | Status: {o['status']} | Trigger: ₹{o.get('trigger_price')} | Price: ₹{o.get('price')}")
                return {"success": True, "order_id": order_id, "status": o['status'], "trigger": trigger_px, "price": limit_px}

        return {"success": True, "order_id": order_id, "trigger": trigger_px, "price": limit_px}
    except Exception as e:
        print(f"❌ Failed to place Stop-Loss order: {e}")
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    place_policybzr_sl()
