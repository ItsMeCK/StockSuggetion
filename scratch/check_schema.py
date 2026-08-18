from core.db_manager import get_db_connection
conn = get_db_connection()
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
print([row[0] for row in cur.fetchall()])
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='active_positions'")
print("active_positions columns:", [row[0] for row in cur.fetchall()])
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name='trades'")
print("trades columns:", [row[0] for row in cur.fetchall()])
