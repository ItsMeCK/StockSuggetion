from core.db_manager import get_db_connection
conn = get_db_connection()
cur = conn.cursor()
cur.execute("UPDATE trades SET entry_time = '2026-08-18 09:15:00' WHERE status = 'LIVE_TRADED'")
conn.commit()
cur.close()
conn.close()
print("Fixed entry_time to 09:15 AM!")
