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
        SELECT time, close 
        FROM daily_ohlcv 
        WHERE symbol = %s AND time > %s
        ORDER BY time ASC
    """
    cur.execute(query, (symbol, entry_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    
    target = entry_price * 1.10
    stop = entry_price * 0.95
    
    for row in rows:
        price = float(row[1])
        if price >= target:
            return "PROFIT", 10.0, row[0].strftime('%Y-%m-%d'), price
        if price <= stop:
            return "LOSS", -5.0, row[0].strftime('%Y-%m-%d'), price
            
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
            # Get entry price (close of that day)
            import psycopg2
            conn = psycopg2.connect(
                host=os.getenv("DB_HOST", "localhost"),
                port=os.getenv("DB_PORT", "5432"),
                user=os.getenv("POSTGRES_USER", "quant"),
                password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                database=os.getenv("POSTGRES_DB", "market_data")
            )
            cur = conn.cursor()
            cur.execute("SELECT close FROM daily_ohlcv WHERE symbol = %s AND time >= %s AND time < %s", 
                       (symbol, date, (datetime.strptime(date, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')))
            row = cur.fetchone()
            entry_price = float(row[0]) if row else 0.0
            cur.close()
            conn.close()
            
            outcome, pnl, exit_date, current_price = track_performance(symbol, date, entry_price)
            trades.append({
                "symbol": symbol,
                "entry_price": entry_price,
                "current_price": current_price,
                "outcome": outcome,
                "pnl": pnl,
                "exit_date": exit_date
            })
            
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
