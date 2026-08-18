import os
from datetime import datetime
from dotenv import load_dotenv
import psycopg2

load_dotenv()
from kiteconnect import KiteConnect

kite_exec = KiteConnect(api_key=os.getenv("EXEC_KITE_API_KEY"))
kite_exec.set_access_token(os.getenv("EXEC_KITE_ACCESS_TOKEN"))

kite_data = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
kite_data.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

from core.db_manager import get_db_connection

positions = kite_exec.positions()['net']
targets = ['LODHA', 'DLF']
injected = []

for pos in positions:
    tradingsymbol = pos['tradingsymbol']
    qty = pos['quantity']
    
    if qty > 0 and any(t in tradingsymbol for t in targets):
        avg_price = pos['average_price']
        instrument_token = pos['instrument_token']
        base_symbol = "LODHA" if "LODHA" in tradingsymbol else "DLF"
        
        # Get live data for metadata baseline
        quote_key = f"NFO:{tradingsymbol}"
        quote = kite_data.quote([quote_key])
        live_oi = quote[quote_key].get("oi", 0) if quote_key in quote else 0
        live_volume = quote[quote_key].get("volume", 0) if quote_key in quote else 0
        
        db_pos = {
            "order_id": f"MANUAL_{tradingsymbol}",
            "sl_order_id": "FAILED", 
            "status": "LIVE_TRADED",
            "entry_time": "2026-08-18 12:15:00",
            "symbol": base_symbol,
            "option_symbol": tradingsymbol,
            "option_token": instrument_token,
            "spot_entry": 0.0, 
            "entry_premium": avg_price,
            "qty": qty,
            "score": 88 if base_symbol == "LODHA" else 82, 
            "catalyst": "User manually entered trade.",
            "current_sl": avg_price * 0.5, 
            "highest_high": avg_price,
            "trailing_active": False,
            "initial_oi": live_oi,
            "highest_oi": live_oi,
            "initial_volume": live_volume
        }
        injected.append(db_pos)

conn = get_db_connection()
cur = conn.cursor()

for db_pos in injected:
    cur.execute("""
        INSERT INTO trades (
            order_id, sl_order_id, status, entry_time, symbol, option_symbol, option_token, 
            spot_entry, entry_premium, qty, score, catalyst, current_sl, highest_high, 
            trailing_active, initial_oi, highest_oi, initial_volume
        ) VALUES (
            %(order_id)s, %(sl_order_id)s, %(status)s, %(entry_time)s, %(symbol)s, %(option_symbol)s, %(option_token)s, 
            %(spot_entry)s, %(entry_premium)s, %(qty)s, %(score)s, %(catalyst)s, %(current_sl)s, %(highest_high)s, 
            %(trailing_active)s, %(initial_oi)s, %(highest_oi)s, %(initial_volume)s
        ) ON CONFLICT (order_id) DO UPDATE SET 
            initial_oi = EXCLUDED.initial_oi,
            highest_oi = EXCLUDED.highest_oi,
            initial_volume = EXCLUDED.initial_volume,
            qty = EXCLUDED.qty,
            entry_premium = EXCLUDED.entry_premium
    """, db_pos)
    print(f"✅ Injected: {db_pos['qty']}x {db_pos['option_symbol']} @ {db_pos['entry_premium']}")
    print(f"   Metadata Baseline -> OI: {db_pos['initial_oi']}, Vol: {db_pos['initial_volume']}")

conn.commit()
cur.close()
conn.close()

if not injected:
    print("❌ Could not find LODHA or DLF in your open Kite positions!")
