"""
scripts/place_equity_orders.py

Executes Monday Opening Equity Orders for Account HU0092:
1. ANANDRATHI: CNC Limit Sell 3 shares @ 2,415.00 (Partial Profit Booking).
2. Fresh Buys (Top 4 sized at ~20k each within available cash of ~78k):
   - AMBER: 3 shares CNC Limit near opening/LTP
   - GVT&D: 4 shares CNC Limit near opening/LTP
   - MFSL: 14 shares CNC Limit near opening/LTP
   - PETRONET: 61 shares CNC Limit near opening/LTP

Rules:
- DO NOT touch TMPV.
- Verification and audit logging.
"""

import os
import sys
import time
import json
from datetime import datetime
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def execute_equity_orders():
    load_dotenv()
    exec_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    data_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    data_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")

    kite_exec = KiteConnect(api_key=exec_key)
    kite_exec.set_access_token(exec_token)

    kite_data = KiteConnect(api_key=data_key)
    kite_data.set_access_token(data_token)

    # 1. Fetch current live quotes to observe opening print
    symbols = ["NSE:AMBER", "NSE:GVT&D", "NSE:MFSL", "NSE:PETRONET", "NSE:ANANDRATHI"]
    quotes = kite_data.quote(symbols)
    
    print("=" * 80)
    print(f"📊 LIVE OPENING QUOTES ({datetime.now().strftime('%Y-%m-%d %H:%M:%S')}):")
    print("=" * 80)
    for s in symbols:
        q = quotes.get(s, {})
        ltp = q.get("last_price", 0.0)
        ohlc = q.get("ohlc", {})
        print(f"{s:<16} | LTP: ₹{ltp:>8.2f} | Open: ₹{ohlc.get('open', 0.0):>8.2f} | High: ₹{ohlc.get('high', 0.0):>8.2f} | Low: ₹{ohlc.get('low', 0.0):>8.2f}")

    # 2. Check Margin
    margins = kite_exec.margins(segment="equity")
    net_margin = margins.get("net", 0.0)
    avail_cash = margins.get("available", {}).get("cash", 0.0)
    print(f"\n💰 HU0092 Net Equity Margin: ₹{net_margin:,.2f} | Available Cash: ₹{avail_cash:,.2f}")

    placed_orders = []

    # 3. Order 1: ANANDRATHI Profit Booking (Sell 3 shares @ Limit 2,415.00 CNC)
    try:
        print("\n🚀 Placing Profit Booking Order: ANANDRATHI...")
        sell_order_id = kite_exec.place_order(
            variety=kite_exec.VARIETY_REGULAR,
            exchange="NSE",
            tradingsymbol="ANANDRATHI",
            transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
            quantity=3,
            order_type=kite_exec.ORDER_TYPE_LIMIT,
            price=2415.0,
            product=kite_exec.PRODUCT_CNC,
            validity=kite_exec.VALIDITY_DAY,
            tag="sovereign_tp"
        )
        print(f"✅ ANANDRATHI Sell Order Placed | Order ID: {sell_order_id} | 3 shares @ ₹2,415.00")
        placed_orders.append({
            "symbol": "ANANDRATHI",
            "type": "SELL_PROFIT_BOOKING",
            "quantity": 3,
            "price": 2415.0,
            "order_id": sell_order_id,
            "status": "PLACED"
        })
    except Exception as e:
        print(f"❌ Failed to place ANANDRATHI sell order: {e}")
        placed_orders.append({
            "symbol": "ANANDRATHI",
            "type": "SELL_PROFIT_BOOKING",
            "status": "FAILED",
            "error": str(e)
        })

    # 4. Fresh Buys - Top 4 Candidates (~₹20k each)
    # Target prices set slightly above LTP (by ~0.15% to 0.25%) or at opening price to ensure smooth limit execution
    buy_targets = [
        {
            "symbol": "AMBER",
            "quantity": 3,
            "ref_price": quotes.get("NSE:AMBER", {}).get("last_price", 6690.0),
            "est_val": 3 * quotes.get("NSE:AMBER", {}).get("last_price", 6690.0)
        },
        {
            "symbol": "GVT&D",
            "quantity": 4,
            "ref_price": quotes.get("NSE:GVT&D", {}).get("last_price", 4440.0),
            "est_val": 4 * quotes.get("NSE:GVT&D", {}).get("last_price", 4440.0)
        },
        {
            "symbol": "MFSL",
            "quantity": 14,
            "ref_price": quotes.get("NSE:MFSL", {}).get("last_price", 1401.0),
            "est_val": 14 * quotes.get("NSE:MFSL", {}).get("last_price", 1401.0)
        },
        {
            "symbol": "PETRONET",
            "quantity": 61,
            "ref_price": quotes.get("NSE:PETRONET", {}).get("last_price", 324.05),
            "est_val": 61 * quotes.get("NSE:PETRONET", {}).get("last_price", 324.05)
        }
    ]

    for item in buy_targets:
        sym = item["symbol"]
        qty = item["quantity"]
        # Limit price set at round(ref_price * 1.002, 1) or ref_price + buffer
        ltp = item["ref_price"]
        # Add a tiny 0.15% cushion for immediate fill on opening volatility, with 0.05 step
        limit_px = round(ltp * 1.002 / 0.05) * 0.05
        limit_px = round(limit_px, 2)
        
        print(f"\n🛒 Placing BUY Order: {sym} | Qty: {qty} | Ref LTP: ₹{ltp:.2f} | Limit Px: ₹{limit_px:.2f} | Est: ₹{qty*limit_px:,.2f}")
        try:
            buy_order_id = kite_exec.place_order(
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
            print(f"✅ {sym} BUY Placed | Order ID: {buy_order_id}")
            placed_orders.append({
                "symbol": sym,
                "type": "BUY",
                "quantity": qty,
                "price": limit_px,
                "order_id": buy_order_id,
                "status": "PLACED"
            })
        except Exception as e:
            print(f"❌ Failed to place BUY order for {sym}: {e}")
            placed_orders.append({
                "symbol": sym,
                "type": "BUY",
                "status": "FAILED",
                "error": str(e)
            })

    # 5. Fetch updated order status
    time.sleep(2)
    all_orders = kite_exec.orders()
    today_orders = [o for o in all_orders if o.get("tag") in ["sovereign_tp", "sovereign_eq"]]
    
    print("\n" + "=" * 80)
    print("📋 ORDER AUDIT & STATUS REPORT:")
    print("=" * 80)
    for o in today_orders:
        print(f"ID: {o['order_id']} | {o['tradingsymbol']:<12} | {o['transaction_type']} | Status: {o['status']:<10} | Qty: {o['filled_quantity']}/{o['quantity']} | Px: ₹{o['price']} | Avg: ₹{o['average_price']}")

    return placed_orders

if __name__ == "__main__":
    execute_equity_orders()
