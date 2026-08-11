import os
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from datetime import datetime

load_dotenv()

def get_db_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME", "sovereign_state"),
        user=os.getenv("POSTGRES_USER", "agent"),
        password=os.getenv("POSTGRES_PASSWORD", "agentpassword"),
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5433")
    )

def init_db():
    conn = get_db_connection()
    cur = conn.cursor()
    # Existing trades table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS trades (
            id SERIAL PRIMARY KEY,
            order_id VARCHAR(50) UNIQUE NOT NULL,
            sl_order_id VARCHAR(50),
            status VARCHAR(20) NOT NULL,
            entry_time TIMESTAMP NOT NULL,
            symbol VARCHAR(20) NOT NULL,
            option_symbol VARCHAR(50) NOT NULL,
            option_token INTEGER NOT NULL,
            spot_entry FLOAT NOT NULL,
            entry_premium FLOAT NOT NULL,
            qty INTEGER NOT NULL,
            score INTEGER NOT NULL,
            catalyst TEXT,
            current_sl FLOAT NOT NULL,
            highest_high FLOAT NOT NULL,
            trailing_active BOOLEAN DEFAULT FALSE,
            exit_time TIMESTAMP,
            exit_price FLOAT,
            pnl_pct FLOAT,
            initial_oi INTEGER DEFAULT 0,
            highest_oi INTEGER DEFAULT 0,
            initial_volume INTEGER DEFAULT 0
        )
    """)
    # New Anticipatory Watchlist table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS anticipatory_watchlist (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) UNIQUE NOT NULL,
            bull_thesis TEXT,
            bear_thesis TEXT,
            conviction_score INTEGER NOT NULL,
            added_at TIMESTAMP NOT NULL
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def save_active_position(position_dict):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trades (
            order_id, sl_order_id, status, entry_time, symbol, option_symbol, option_token,
            spot_entry, entry_premium, qty, score, catalyst, current_sl, highest_high, trailing_active,
            initial_oi, highest_oi, initial_volume
        ) VALUES (
            %(order_id)s, %(sl_order_id)s, %(status)s, %(entry_time)s, %(symbol)s, %(option_symbol)s, %(option_token)s,
            %(spot_entry)s, %(entry_premium)s, %(qty)s, %(score)s, %(catalyst)s, %(current_sl)s, %(highest_high)s, %(trailing_active)s,
            %(initial_oi)s, %(highest_oi)s, %(initial_volume)s
        )
    """, position_dict)
    conn.commit()
    cur.close()
    conn.close()

def get_all_active_positions():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    cur.execute("""
        SELECT * FROM trades 
        WHERE status IN ('LIVE_TRADED', 'PAPER_TRADED')
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def update_position_sl(order_id, new_sl, highest_high, trailing_active):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE trades
        SET current_sl = %s, highest_high = %s, trailing_active = %s
        WHERE order_id = %s
    """, (new_sl, highest_high, trailing_active, order_id))
    conn.commit()
    cur.close()
    conn.close()

def update_sl_order_id(order_id, new_sl_order_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE trades
        SET sl_order_id = %s
        WHERE order_id = %s
    """, (new_sl_order_id, order_id))
    conn.commit()
    cur.close()
    conn.close()

def close_position(position, exit_price, pnl_pct, status_override="CLOSED"):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE trades
        SET status = %s, exit_time = %s, exit_price = %s, pnl_pct = %s
        WHERE order_id = %s
    """, (status_override, datetime.now(), exit_price, pnl_pct, position['order_id']))
    conn.commit()
    cur.close()
    conn.close()

def update_highest_oi(order_id, new_highest_oi):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE trades
        SET highest_oi = %s
        WHERE order_id = %s
    """, (new_highest_oi, order_id))
    conn.commit()
    cur.close()
    conn.close()

def add_anticipatory_stock(symbol, bull_thesis, bear_thesis, score):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO anticipatory_watchlist (symbol, bull_thesis, bear_thesis, conviction_score, added_at)
        VALUES (%s, %s, %s, %s, %s)
        ON CONFLICT (symbol) 
        DO UPDATE SET 
            bull_thesis = EXCLUDED.bull_thesis,
            bear_thesis = EXCLUDED.bear_thesis,
            conviction_score = EXCLUDED.conviction_score,
            added_at = EXCLUDED.added_at
    """, (symbol, bull_thesis, bear_thesis, score, datetime.now()))
    conn.commit()
    cur.close()
    conn.close()

def get_anticipatory_watchlist():
    conn = get_db_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)
    # Only get stocks added in the last 24 hours
    cur.execute("""
        SELECT * FROM anticipatory_watchlist 
        WHERE added_at > NOW() - INTERVAL '24 hours'
        ORDER BY conviction_score DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows
