import os
from datetime import datetime
from dotenv import load_dotenv
load_dotenv()
from kiteconnect import KiteConnect
from core.db_manager import save_active_position

kite = KiteConnect(api_key=os.getenv("EXEC_KITE_API_KEY"))
kite.set_access_token(os.getenv("EXEC_KITE_ACCESS_TOKEN"))

positions = kite.positions()['net']

targets = ['PRESTIGE', 'SUZLON']
injected_count = 0

for pos in positions:
    tradingsymbol = pos['tradingsymbol']
    qty = pos['quantity']
    
    if qty > 0 and any(t in tradingsymbol for t in targets):
        avg_price = pos['average_price']
        instrument_token = pos['instrument_token']
        
        base_symbol = "PRESTIGE" if "PRESTIGE" in tradingsymbol else "SUZLON"
        
        db_pos = {
            "order_id": f"MANUAL_{tradingsymbol}",
            "sl_order_id": "FAILED", 
            "status": "LIVE_TRADED",
            "entry_time": datetime.now().isoformat(),
            "symbol": base_symbol,
            "option_symbol": tradingsymbol,
            "option_token": instrument_token,
            "spot_entry": 0.0, 
            "entry_premium": avg_price,
            "qty": qty,
            "score": 90.0, 
            "catalyst": "User manually entered trade via Zerodha.",
            "current_sl": avg_price * 0.5, 
            "highest_high": avg_price,
            "trailing_active": False,
            "initial_oi": 0,
            "highest_oi": 0,
            "initial_volume": 0
        }
        save_active_position(db_pos)
        print(f"✅ Injected: {qty}x {tradingsymbol} @ {avg_price}")
        injected_count += 1

if injected_count == 0:
    print("❌ Could not find PRESTIGE or SUZLON in your open Kite positions!")
