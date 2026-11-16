"""
scripts/place_equity_buys_round2.py

Places remaining fresh equity buy orders (GVT&D and ENRIN) on HU0092.
Ensures correct tick sizes:
- GVT&D: tick_size = 0.10
- ENRIN: tick_size = 0.05
"""

import os
import sys
import time
import json
import math
from datetime import datetime
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def place_remaining_buys():
    load_dotenv()
    exec_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    data_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    data_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")

    kite_exec = KiteConnect(api_key=exec_key)
    kite_exec.set_access_token(exec_token)

    kite_data = KiteConnect(api_key=data_key)
    kite_data.set_access_token(data_token)

    symbols = ["NSE:GVT&D", "NSE:ENRIN"]
    quotes = kite_data.quote(symbols)

    orders_to_place = [
        {
            "symbol": "GVT&D",
            "quantity": 4,
            "tick_size": 0.10,
            "ref_ltp": quotes.get("NSE:GVT&D", {}).get("last_price", 4370.0)
        },
        {
            "symbol": "ENRIN",
            "quantity": 6,
            "tick_size": 0.05,
            "ref_ltp": quotes.get("NSE:ENRIN", {}).get("last_price", 3135.0)
        }
    ]

    placed = []
    for item in orders_to_place:
        sym = item["symbol"]
        qty = item["quantity"]
        tick = item["tick_size"]
        ltp = item["ref_ltp"]
        # Limit price with 0.2% cushion to get immediate fill, snapped strictly to tick_size
        raw_price = ltp * 1.002
        limit_px = round(math.ceil(raw_price / tick) * tick, 2)
        if tick == 0.10:
            limit_px = round(limit_px, 1)

        print(f"🛒 Placing BUY Order: {sym} | Qty: {qty} | LTP: ₹{ltp:.2f} | Limit Px: ₹{limit_px:.2f} (Tick: {tick})")
        try:
            order_id = kite_exec.place_order(
                variety=kite_exec.VARIETY_REGULAR,
                exchange="NSE",
                tradingsymbol=sym,
                transaction_type=kite_exec.TRANSACTION_TYPE_BUY,
                quantity=qty,
                order_type=kite_exec.ORDER_TYPE_LIMIT,
                price=limit_px,
                product=kite_exec.PRODUCT_CNC,
                validity=kite_exec.VALIDITY_DAY,
                tag="sovereign_eq"
            )
            print(f"✅ {sym} BUY Placed | Order ID: {order_id}")
            placed.append({"symbol": sym, "order_id": order_id, "status": "PLACED", "qty": qty, "price": limit_px})
        except Exception as e:
            print(f"❌ Failed to place BUY for {sym}: {e}")
            placed.append({"symbol": sym, "status": "FAILED", "error": str(e)})

    time.sleep(2)
    all_orders = kite_exec.orders()
    recent = [o for o in all_orders if o.get("tag") == "sovereign_eq"]
    print("\n" + "=" * 80)
    print("📋 AUDIT OF ALL SOVEREIGN EQUITY ORDERS TODAY:")
    print("=" * 80)
    for o in recent:
        print(f"ID: {o['order_id']} | {o['tradingsymbol']:<10} | {o['transaction_type']} | Status: {o['status']:<10} | Filled: {o['filled_quantity']}/{o['quantity']} | Px: ₹{o['price']} | Avg Px: ₹{o['average_price']}")

    return placed

if __name__ == "__main__":
    place_remaining_buys()
