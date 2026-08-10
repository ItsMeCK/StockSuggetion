import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime

DB_PARAMS = {
    "dbname": "sovereign_state",
    "user": "agent",
    "password": "agentpassword",
    "host": "localhost",
    "port": "5433"
}

def get_db_connection():
    return psycopg2.connect(**DB_PARAMS)

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS active_positions (
            order_id TEXT PRIMARY KEY,
            status TEXT,
            entry_time TIMESTAMP,
            symbol TEXT,
            option_symbol TEXT,
            option_token INTEGER,
            spot_entry REAL,
            entry_premium REAL,
            qty INTEGER,
            score INTEGER,
            catalyst TEXT,
            current_sl REAL,
            highest_high REAL,
            trailing_active BOOLEAN DEFAULT FALSE,
            sl_order_id TEXT
        );
    """)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS closed_positions (
            order_id TEXT PRIMARY KEY,
            status TEXT,
            entry_time TIMESTAMP,
            exit_time TIMESTAMP,
            symbol TEXT,
            option_symbol TEXT,
            entry_premium REAL,
            exit_price REAL,
            pnl_pct REAL,
            exit_reason TEXT
        );
    """)
    conn.commit()
    cursor.close()
    conn.close()
    
def save_active_position(pos):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO active_positions 
        (order_id, status, entry_time, symbol, option_symbol, option_token, spot_entry, entry_premium, qty, score, catalyst, current_sl, highest_high, trailing_active, sl_order_id)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING;
    """, (
        pos['order_id'], pos['status'], pos['entry_time'], pos['symbol'], pos['option_symbol'],
        pos['option_token'], pos['spot_entry'], pos['entry_premium'], pos['qty'], pos['score'],
        pos['catalyst'], pos['current_sl'], pos['highest_high'], pos.get('trailing_active', False),
        pos.get('sl_order_id')
    ))
    conn.commit()
    cursor.close()
    conn.close()

def get_all_active_positions():
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    cursor.execute("SELECT * FROM active_positions;")
    rows = cursor.fetchall()
    cursor.close()
    conn.close()
    return [dict(row) for row in rows]

def update_position_sl(order_id, highest_high, current_sl, trailing_active):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE active_positions 
        SET highest_high = %s, current_sl = %s, trailing_active = %s
        WHERE order_id = %s;
    """, (highest_high, current_sl, trailing_active, order_id))
    conn.commit()
    cursor.close()
    conn.close()

def close_position(pos, exit_price, pnl_pct, exit_reason):
    conn = get_db_connection()
    cursor = conn.cursor()
    # Insert to closed
    cursor.execute("""
        INSERT INTO closed_positions 
        (order_id, status, entry_time, exit_time, symbol, option_symbol, entry_premium, exit_price, pnl_pct, exit_reason)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO NOTHING;
    """, (
        pos['order_id'], "CLOSED", pos['entry_time'], datetime.now(), pos['symbol'], pos['option_symbol'],
        pos['entry_premium'], exit_price, pnl_pct, exit_reason
    ))
    # Delete from active
    cursor.execute("DELETE FROM active_positions WHERE order_id = %s;", (pos['order_id'],))
    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    init_db()
    print("Database Initialized successfully.")
