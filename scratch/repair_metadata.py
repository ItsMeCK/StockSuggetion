import os
from dotenv import load_dotenv
load_dotenv()
from kiteconnect import KiteConnect
import psycopg2
from core.db_manager import get_db_connection, get_all_active_positions

kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN"))

positions = get_all_active_positions()

# Collect symbols to quote
symbols_to_quote = []
for pos in positions:
    if pos["status"] == "LIVE_TRADED" and pos["symbol"] in ["PRESTIGE", "SUZLON"]:
        symbols_to_quote.append(f"NFO:{pos['option_symbol']}")

if not symbols_to_quote:
    print("No PRESTIGE or SUZLON active positions found in database.")
    exit()

try:
    quotes = kite.quote(symbols_to_quote)
except Exception as e:
    print(f"Failed to fetch quotes: {e}")
    exit()

conn = get_db_connection()
cur = conn.cursor()

for pos in positions:
    if pos["status"] == "LIVE_TRADED" and pos["symbol"] in ["PRESTIGE", "SUZLON"]:
        opt_symbol = pos["option_symbol"]
        quote_key = f"NFO:{opt_symbol}"
        if quote_key in quotes:
            live_oi = quotes[quote_key].get("oi", 0)
            live_volume = quotes[quote_key].get("volume", 0)
            
            # Update DB
            cur.execute("""
                UPDATE trades 
                SET initial_oi = %s, 
                    highest_oi = %s, 
                    initial_volume = %s
                WHERE order_id = %s
            """, (live_oi, live_oi, live_volume, pos["order_id"]))
            print(f"✅ Fixed DB Metadata for {opt_symbol} -> OI: {live_oi}, Vol: {live_volume}")

conn.commit()
cur.close()
conn.close()
print("🎉 Metadata successfully repaired! The AI Momentum Monitor is fully restored.")
