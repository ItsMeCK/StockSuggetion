from core.db_manager import get_db_connection
conn = get_db_connection()
cur = conn.cursor()
cur.execute("SELECT option_symbol, entry_time FROM trades WHERE status='LIVE_TRADED'")
for row in cur.fetchall():
    print(f"{row[0]}: {row[1]}")
