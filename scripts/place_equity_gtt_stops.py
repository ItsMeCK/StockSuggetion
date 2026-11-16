"""
scripts/place_equity_gtt_stops.py

Places Trailing Stop-Loss GTT orders for Equity Engine recommendations:
1. BANKINDIA: 148 shares @ trigger 148.0, limit 147.5 CNC
2. ADANIPOWER: 102 shares @ trigger 194.5, limit 194.0 CNC
3. ANANDRATHI: 6 shares @ trigger 2280.0, limit 2275.0 CNC

STRICT RULE: DO NOT TOUCH TMPV.
"""

import os
import sys
import json
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def place_trailing_stops():
    load_dotenv()
    exec_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    
    if not exec_key or not exec_token:
        print("❌ Missing EXEC_KITE_API_KEY or EXEC_KITE_ACCESS_TOKEN in .env")
        return False
        
    kite = KiteConnect(api_key=exec_key)
    kite.set_access_token(exec_token)
    
    # 1. Fetch current live quote for LTP reference
    data_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    data_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    kite_data = KiteConnect(api_key=data_key)
    kite_data.set_access_token(data_token)
    
    symbols = ["NSE:BANKINDIA", "NSE:ADANIPOWER", "NSE:ANANDRATHI"]
    quotes = kite_data.quote(symbols)
    
    sl_configs = [
        {
            "tradingsymbol": "BANKINDIA",
            "trigger_price": 148.0,
            "limit_price": 147.5,
            "quantity": 148,
            "note": "Locking +9.6% profit above avg 135.00"
        },
        {
            "tradingsymbol": "ADANIPOWER",
            "trigger_price": 194.5,
            "limit_price": 194.0,
            "quantity": 102,
            "note": "Breakeven stop-loss at entry 194.50"
        },
        {
            "tradingsymbol": "ANANDRATHI",
            "trigger_price": 2280.0,
            "limit_price": 2275.0,
            "quantity": 6,
            "note": "Protecting 6 long-term shares above avg 2273.00"
        }
    ]
    
    results = []
    for cfg in sl_configs:
        sym = cfg["tradingsymbol"]
        full_sym = f"NSE:{sym}"
        q = quotes.get(full_sym, {})
        ltp = q.get("last_price") or cfg["trigger_price"] + 5.0
        
        order_payload = [
            {
                "transaction_type": kite.TRANSACTION_TYPE_SELL,
                "quantity": cfg["quantity"],
                "order_type": kite.ORDER_TYPE_LIMIT,
                "product": kite.PRODUCT_CNC,
                "price": cfg["limit_price"]
            }
        ]
        
        try:
            resp = kite.place_gtt(
                trigger_type=kite.GTT_TYPE_SINGLE,
                tradingsymbol=sym,
                exchange="NSE",
                trigger_values=[cfg["trigger_price"]],
                last_price=ltp,
                orders=order_payload
            )
            gtt_id = resp.get("id") or resp.get("trigger_id")
            results.append({
                "symbol": sym,
                "gtt_id": gtt_id,
                "status": "SUCCESS",
                "trigger_price": cfg["trigger_price"],
                "limit_price": cfg["limit_price"],
                "quantity": cfg["quantity"],
                "ltp": ltp,
                "note": cfg["note"]
            })
            print(f"✅ GTT Created: {sym} | ID: {gtt_id} | Trigger: ₹{cfg['trigger_price']} | Limit: ₹{cfg['limit_price']} | Qty: {cfg['quantity']}")
        except Exception as e:
            print(f"❌ Failed to place GTT for {sym}: {e}")
            results.append({
                "symbol": sym,
                "status": "FAILED",
                "error": str(e)
            })
            
    return results

if __name__ == "__main__":
    res = place_trailing_stops()
    print("\nSummary:", json.dumps(res, indent=2))
