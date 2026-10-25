import os
import logging
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from datetime import datetime
from typing import Optional, Dict, Any
from core.dynamic_exit_engine import calculate_statutory_friction

logger = logging.getLogger(__name__)

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
            profit_locked BOOLEAN DEFAULT FALSE,
            holding_type VARCHAR(30) DEFAULT 'TYPE_A_SCALP',
            exit_time TIMESTAMP,
            exit_price FLOAT,
            pnl_pct FLOAT,
            initial_oi INTEGER DEFAULT 0,
            highest_oi INTEGER DEFAULT 0,
            initial_volume INTEGER DEFAULT 0,
            turnover FLOAT DEFAULT 0.0,
            statutory_charges FLOAT DEFAULT 0.0,
            net_pnl FLOAT DEFAULT 0.0
        )
    """)
    # Migration safety for existing tables
    cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS holding_type VARCHAR(30) DEFAULT 'TYPE_A_SCALP';")
    cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS profit_locked BOOLEAN DEFAULT FALSE;")
    cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS turnover FLOAT DEFAULT 0.0;")
    cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS statutory_charges FLOAT DEFAULT 0.0;")
    cur.execute("ALTER TABLE trades ADD COLUMN IF NOT EXISTS net_pnl FLOAT DEFAULT 0.0;")

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

    # Daily Focus Watchlist table (Google Grounded Watchlist Builder)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS daily_focus_watchlist (
            id SERIAL PRIMARY KEY,
            symbol VARCHAR(20) UNIQUE NOT NULL,
            direction VARCHAR(10) NOT NULL,
            conviction_score FLOAT NOT NULL,
            catalyst_summary TEXT,
            upcoming_events TEXT,
            market_buzz TEXT,
            status VARCHAR(20) DEFAULT 'ACTIVE',
            added_at TIMESTAMP NOT NULL,
            last_refreshed TIMESTAMP NOT NULL
        )
    """)
    conn.commit()
    cur.close()
    conn.close()

def save_active_position(position_dict):
    conn = get_db_connection()
    cur = conn.cursor()
    pos_data = position_dict.copy()
    pos_data.setdefault("holding_type", "TYPE_A_SCALP")
    pos_data.setdefault("profit_locked", False)
    cur.execute("""
        INSERT INTO trades (
            order_id, sl_order_id, status, entry_time, symbol, option_symbol, option_token,
            spot_entry, entry_premium, qty, score, catalyst, current_sl, highest_high, trailing_active,
            profit_locked, holding_type,
            initial_oi, highest_oi, initial_volume
        ) VALUES (
            %(order_id)s, %(sl_order_id)s, %(status)s, %(entry_time)s, %(symbol)s, %(option_symbol)s, %(option_token)s,
            %(spot_entry)s, %(entry_premium)s, %(qty)s, %(score)s, %(catalyst)s, %(current_sl)s, %(highest_high)s, %(trailing_active)s,
            %(profit_locked)s, %(holding_type)s,
            %(initial_oi)s, %(highest_oi)s, %(initial_volume)s
        )
    """, pos_data)
    conn.commit()
    cur.close()
    conn.close()

def get_all_active_positions():
    try:
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
    except Exception as e:
        print(f"⚠️ Warning: Database error in get_all_active_positions ({e}). Returning empty list.")
        return []

def get_daily_trades_count(target_date: Optional[datetime] = None) -> int:
    """
    Returns total count of trades entered today (or on target_date).
    Includes LIVE_TRADED, PAPER_TRADED, and trades that opened today and have closed.
    Used to enforce the hard 5 trades per day ceiling.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        t_date = (target_date or datetime.now()).date()
        cur.execute("""
            SELECT COUNT(*) FROM trades 
            WHERE entry_time::date = %s
        """, (t_date,))
        row = cur.fetchone()
        count = int(row[0]) if row else 0
        cur.close()
        conn.close()
        return count
    except Exception as e:
        print(f"⚠️ Warning: Database error in get_daily_trades_count ({e}). Returning 0.")
        return 0

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

def update_position_profit_locked(order_id, profit_locked=True):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE trades
        SET profit_locked = %s
        WHERE order_id = %s
    """, (profit_locked, order_id))
    conn.commit()
    cur.close()
    conn.close()

def update_position_qty(order_id, new_qty):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE trades
        SET qty = %s
        WHERE order_id = %s
    """, (new_qty, order_id))
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

    entry_premium = float(position.get("entry_premium", 0.0) or 0.0)
    qty = int(position.get("qty", 0) or 0)
    
    # Section 43(5) F&O turnover = |Exit Value - Entry Value|
    turnover = round(abs(float(exit_price) - entry_premium) * qty, 2)
    friction = calculate_statutory_friction(entry_premium, float(exit_price), qty)
    statutory_charges = friction["total_cost"]
    gross_pnl = (float(exit_price) - entry_premium) * qty
    net_pnl = round(gross_pnl - statutory_charges, 2)

    cur.execute("""
        UPDATE trades
        SET status = %s, exit_time = %s, exit_price = %s, pnl_pct = %s,
            turnover = %s, statutory_charges = %s, net_pnl = %s
        WHERE order_id = %s
    """, (status_override, datetime.now(), exit_price, pnl_pct, turnover, statutory_charges, net_pnl, position['order_id']))
    conn.commit()
    cur.close()
    conn.close()

def get_cumulative_fy_turnover(as_of_date: Optional[datetime] = None) -> Dict[str, Any]:
    """
    Computes Section 43(5) & Section 44AB cumulative F&O turnover and statutory costs
    for the Indian Financial Year (April 1 to March 31).
    Monitors proximity to the ₹10 Crore Section 44AB mandatory tax audit threshold.
    """
    target_dt = as_of_date or datetime.now()
    if target_dt.month >= 4:
        fy_start = datetime(target_dt.year, 4, 1, 0, 0, 0)
        fy_end = datetime(target_dt.year + 1, 3, 31, 23, 59, 59)
    else:
        fy_start = datetime(target_dt.year - 1, 4, 1, 0, 0, 0)
        fy_end = datetime(target_dt.year, 3, 31, 23, 59, 59)

    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT 
                COALESCE(SUM(turnover), 0.0) as total_turnover,
                COALESCE(SUM(statutory_charges), 0.0) as total_charges,
                COALESCE(SUM(net_pnl), 0.0) as total_net_pnl,
                COUNT(*) as total_closed_trades
            FROM trades
            WHERE status LIKE 'CLOSED%'
              AND exit_time >= %s AND exit_time <= %s
        """, (fy_start, fy_end))
        row = cur.fetchone() or {}
        cur.close()
        conn.close()

        total_turnover = float(row.get("total_turnover", 0.0) or 0.0)
        total_charges = float(row.get("total_charges", 0.0) or 0.0)
        total_net_pnl = float(row.get("total_net_pnl", 0.0) or 0.0)
        closed_trades = int(row.get("total_closed_trades", 0) or 0)
    except Exception as e:
        print(f"Warning: Database error in get_cumulative_fy_turnover ({e}). Returning zero ledger.")
        total_turnover = 0.0
        total_charges = 0.0
        total_net_pnl = 0.0
        closed_trades = 0

    audit_threshold = 100_000_000.0  # ₹10 Crore under Section 44AB
    return {
        "fy_start": fy_start.strftime("%Y-%m-%d"),
        "fy_end": fy_end.strftime("%Y-%m-%d"),
        "cumulative_turnover": round(total_turnover, 2),
        "statutory_charges": round(total_charges, 2),
        "net_pnl": round(total_net_pnl, 2),
        "closed_trades": closed_trades,
        "audit_threshold": audit_threshold,
        "turnover_pct_of_threshold": round((total_turnover / audit_threshold) * 100.0, 4),
        "requires_audit": total_turnover >= audit_threshold
    }

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
    cur.execute("""
        SELECT * FROM anticipatory_watchlist 
        WHERE added_at > NOW() - INTERVAL '24 hours'
        AND conviction_score >= 90
        ORDER BY conviction_score DESC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def save_daily_focus_stock(
    symbol: str,
    direction: str,
    conviction_score: float,
    catalyst_summary: str,
    upcoming_events: str,
    market_buzz: str,
    status: str = "ACTIVE"
):
    """
    Saves or updates a stock in the daily focus watchlist with grounding intelligence.
    """
    try:
        now = datetime.now()
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO daily_focus_watchlist (
                symbol, direction, conviction_score, catalyst_summary,
                upcoming_events, market_buzz, status, added_at, last_refreshed
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol)
            DO UPDATE SET
                direction = EXCLUDED.direction,
                conviction_score = EXCLUDED.conviction_score,
                catalyst_summary = EXCLUDED.catalyst_summary,
                upcoming_events = EXCLUDED.upcoming_events,
                market_buzz = EXCLUDED.market_buzz,
                status = EXCLUDED.status,
                last_refreshed = EXCLUDED.last_refreshed
        """, (symbol, direction, conviction_score, catalyst_summary, upcoming_events, market_buzz, status, now, now))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.warning(f"Could not persist daily focus stock {symbol} to database: {e}")

def get_active_daily_focus_watchlist(min_score: float = 50.0):
    """
    Fetches all ACTIVE daily focus stocks refreshed in the last 24 hours.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT * FROM daily_focus_watchlist
            WHERE status = 'ACTIVE'
              AND last_refreshed > NOW() - INTERVAL '24 hours'
              AND conviction_score >= %s
            ORDER BY conviction_score DESC
        """, (min_score,))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return rows
    except Exception as e:
        print(f"⚠️ Warning: Database error in get_active_daily_focus_watchlist ({e}). Checking local JSON cache.")
        cache_file = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data/daily_focus_watchlist.json")
        if os.path.exists(cache_file):
            try:
                import json
                with open(cache_file, "r") as f:
                    data = json.load(f)
                    return [d for d in data if d.get("status") == "ACTIVE" and float(d.get("conviction_score", 0.0)) >= min_score]
            except Exception:
                pass
        return []

def update_watchlist_stock_status(symbol: str, new_status: str, reason: str = ""):
    """
    Updates the status of a watchlist symbol (e.g. ACTIVE -> PRUNED, REVIEW).
    """
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        UPDATE daily_focus_watchlist
        SET status = %s, catalyst_summary = COALESCE(catalyst_summary, '') || %s, last_refreshed = %s
        WHERE symbol = %s
    """, (new_status, f" [Status: {new_status} - {reason}]", datetime.now(), symbol))
    conn.commit()
    cur.close()
    conn.close()

def update_entry_fill(order_id, fill_price, new_sl=None, new_sl_order_id=None):
    conn = get_db_connection()
    cur = conn.cursor()
    if new_sl is not None and new_sl_order_id is not None:
        cur.execute("""
            UPDATE trades
            SET entry_premium = %s, current_sl = %s, sl_order_id = %s
            WHERE order_id = %s
        """, (fill_price, new_sl, new_sl_order_id, order_id))
    elif new_sl is not None:
        cur.execute("""
            UPDATE trades
            SET entry_premium = %s, current_sl = %s
            WHERE order_id = %s
        """, (fill_price, new_sl, order_id))
    elif new_sl_order_id is not None:
        cur.execute("""
            UPDATE trades
            SET entry_premium = %s, sl_order_id = %s
            WHERE order_id = %s
        """, (fill_price, new_sl_order_id, order_id))
    else:
        cur.execute("""
            UPDATE trades
            SET entry_premium = %s
            WHERE order_id = %s
        """, (fill_price, order_id))
    conn.commit()
    cur.close()
    conn.close()

