import os
import sys
from dotenv import load_dotenv
import psycopg2
from kiteconnect import KiteConnect

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('DB_HOST','localhost'), 
    port=os.getenv('DB_PORT','5432'), 
    user=os.getenv('POSTGRES_USER','quant'), 
    password=os.getenv('POSTGRES_PASSWORD','quantpassword'), 
    database=os.getenv('POSTGRES_DB','market_data')
)
cur = conn.cursor()

symbols = ['TIMKEN', 'POLICYBZR', 'TATACOMM', 'TRITURBINE', 'MANKIND', 'ENRIN']
entries = {}
for sym in symbols:
    # Get latest trade for 2026-05-20
    cur.execute("SELECT price FROM trade_events WHERE ticker = %s AND system_time >= '2026-05-20' AND system_time < '2026-05-21' ORDER BY system_time DESC LIMIT 1", (sym,))
    row = cur.fetchone()
    if row:
        entries[sym] = float(row[0])

api_key = os.getenv("KITE_API_KEY")
access_token = os.getenv("KITE_ACCESS_TOKEN").strip("'")

kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)

quotes = kite.quote([f"NSE:{s}" for s in entries.keys()])
for sym, entry in entries.items():
    if f"NSE:{sym}" in quotes:
        close_price = quotes[f"NSE:{sym}"]['last_price']
        print(f"{sym}: ENTRY = {entry}, CLOSE = {close_price}")
    else:
        print(f"{sym}: ENTRY = {entry}, CLOSE = Not Found")
