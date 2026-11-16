"""
scripts/place_fresh_buys_gtt_stops.py

Places GTT Stop-Loss orders for the 5 freshly acquired equity positions:
1. AMBER (3 shares) @ trigger ₹6,159.00 / limit ₹6,150.00
2. MFSL (14 shares) @ trigger ₹1,232.00 / limit ₹1,230.00
3. PETRONET (61 shares) @ trigger ₹296.00 / limit ₹295.00
4. GVT&D (4 shares) @ trigger ₹3,946.00 / limit ₹3,940.00
5. ENRIN (6 shares) @ trigger ₹2,794.00 / limit ₹2,790.00
"""

import os
import json
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def place_fresh_stops():
    load_dotenv()
    exec_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    data_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    data_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")

    kite_exec = KiteConnect(api_key=exec_key)
    kite_exec.set_access_token(exec_token)

    kite_data = KiteConnect(api_key=data_key)
    kite_data.set_access_token(data_token)

    symbols = ["NSE:AMBER", "NSE:MFSL", "NSE:PETRONET", "NSE:GVT&D", "NSE:ENRIN"]
    quotes = kite_data.quote(symbols)

    stop_configs = [
        {"symbol": "AMBER", "qty": 3, "trigger": 6159.0, "limit": 6150.0},
        {"symbol": "MFSL", "qty": 14, "trigger": 1232.0, "limit": 1230.0},
        {"symbol": "PETRONET", "qty": 61, "trigger": 296.0, "limit": 295.0},
        {"symbol": "GVT&D", "qty": 4, "trigger": 3946.0, "limit": 3940.0},
        {"symbol": "ENRIN", "qty": 6, "trigger": 2794.0, "limit": 2790.0}
    ]

    results = []
    for cfg in stop_configs:
        sym = cfg["symbol"]
        q = quotes.get(f"NSE:{sym}", {})
        ltp = q.get("last_price", cfg["trigger"] + 100.0)

        orders = [{
            "transaction_type": kite_exec.TRANSACTION_TYPE_SELL,
            "quantity": cfg["qty"],
            "order_type": kite_exec.ORDER_TYPE_LIMIT,
            "product": kite_exec.PRODUCT_CNC,
            "price": cfg["limit"]
        }]

        try:
            resp = kite_exec.place_gtt(
                trigger_type=kite_exec.GTT_TYPE_SINGLE,
                tradingsymbol=sym,
                exchange="NSE",
                trigger_values=[cfg["trigger"]],
                last_price=ltp,
                orders=orders
            )
            gtt_id = resp.get("trigger_id") or resp.get("id")
            print(f"✅ GTT SL Created: {sym:<10} | ID: {gtt_id} | Qty: {cfg['qty']} | Trigger: ₹{cfg['trigger']} | Limit: ₹{cfg['limit']}")
            results.append({"symbol": sym, "gtt_id": gtt_id, "status": "SUCCESS"})
        except Exception as e:
            print(f"❌ Failed to place GTT SL for {sym}: {e}")
            results.append({"symbol": sym, "status": "FAILED", "error": str(e)})

    return results

if __name__ == "__main__":
    place_fresh_stops()
