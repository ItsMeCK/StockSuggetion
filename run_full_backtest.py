import os
import json
import logging
import pandas as pd
from datetime import datetime, timedelta
from midnight_sovereign.pipeline.screener import SovereignScreener
from midnight_sovereign.run_historical import run_historical_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_trading_dates(start_date, end_date):
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    query = """
        SELECT DISTINCT time::date 
        FROM daily_ohlcv 
        WHERE time >= %s AND time <= %s
        ORDER BY time ASC
    """
    cur.execute(query, (start_date, end_date))
    dates = [row[0].strftime('%Y-%m-%d') for row in cur.fetchall()]
    cur.close()
    conn.close()
    return dates

def track_performance(symbol, entry_date, entry_price):
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    query = """
        SELECT time, close, high, low 
        FROM daily_ohlcv 
        WHERE symbol = %s AND time::date > %s::date
        ORDER BY time ASC
    """
    cur.execute(query, (symbol, entry_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    target = entry_price * 1.15
    max_high = entry_price
    stop = entry_price * 0.93 # 7% Hard Stop
    
    for row in rows:
        close_price = float(row[1])
        high_price = float(row[2])
        low_price = float(row[3])
        
        # Update trailing stop baseline
        if high_price > max_high:
            max_high = high_price
            # Trail the stop: 10% below the new peak (Institutional Breath)
            stop = max_high * 0.90
        
        # Priority 1: Check if target hit (Institutional Priority)
        if high_price >= target:
            return "PROFIT", 15.0, row[0].strftime('%Y-%m-%d'), target
            
        # Priority 2: Check if trailing stop hit
        if low_price <= stop:
            pnl = ((stop - entry_price) / entry_price) * 100
            return "LOSS" if pnl < 0 else "PROFIT", pnl, row[0].strftime('%Y-%m-%d'), stop
            
    # If still open, calculate current PnL based on last known price
    if rows:
        last_price = float(rows[-1][1])
        floating_pnl = ((last_price - entry_price) / entry_price) * 100
        return "OPEN", round(floating_pnl, 2), "N/A", last_price
        
    return "OPEN", 0.0, "N/A", entry_price

def run_backtest():
    start_date = "2026-02-01"
    end_date = "2026-04-30"
    trading_dates = get_trading_dates(start_date, end_date)
    
    results = []
    
    for date in trading_dates:
        logging.info(f"RUNNING BACKTEST FOR: {date}")
        run_data = run_historical_engine(date)
        
        if not run_data:
            results.append({
                "date": date,
                "gate_1_candidates": [],
                "gate_2_approved": [],
                "trades": []
            })
            continue
            
        trades = []
        for symbol in run_data["approved"]:
            # Use the entry price (Close of the approval day)
            import psycopg2
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                database=os.getenv("POSTGRES_DB", "market_data")
            )
            cur = conn.cursor()
            cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s AND time::date = %s::date", (symbol, date))
            row = cur.fetchone()
            entry_price = float(row[0]) if row else 0.0
            cur.close()
            conn.close()
            
            if entry_price > 0:
                # Velocity Entry Triggered Immediately
                outcome, pnl, exit_date, current_price = track_performance(symbol, date, entry_price)
                trades.append({
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "current_price": current_price,
                    "outcome": outcome,
                    "pnl": pnl,
                    "exit_date": exit_date,
                    "fill_date": date
                })
            else:
                # ORDER EXPIRED (AMO NOT HIT within 5 days)
                logging.info(f"ORDER EXPIRED: {symbol} never reached pivot {entry_price} within 5 days")
                # Update DB status so it's no longer excluded
                import psycopg2
                conn = psycopg2.connect(
                    host=os.getenv("DB_HOST", "localhost"),
                    port=os.getenv("DB_PORT", "5432"),
                    user=os.getenv("POSTGRES_USER", "quant"),
                    password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                    database=os.getenv("POSTGRES_DB", "market_data")
                )
                cur = conn.cursor()
                cur.execute("UPDATE trade_events SET status = 'EXPIRED_UNFILLED' WHERE ticker = %s AND status = 'SIGNALED'", (symbol,))
                conn.commit()
                cur.close()
                conn.close()
            
        results.append({
            "date": date,
            "gate_1_candidates": run_data["candidates"],
            "gate_2_approved": run_data["approved"],
            "trades": trades
        })
        
    with open("backtest_results.json", "w") as f:
        json.dump(results, f, indent=4)
        
    logging.info("Backtest complete. Results saved to backtest_results.json")

if __name__ == "__main__":
    run_backtest()
