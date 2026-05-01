import os
import json
import logging
from datetime import datetime, timedelta
from pipeline.screener import SovereignScreener
from run_historical import run_historical_engine

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
    
    # Sovereign Aggressive Exit Logic
    target = entry_price * 1.50 # 50% Profit Target
    base_stop = entry_price * 0.85 # 15% Stop Loss
    current_stop = base_stop
    breakeven_triggered = False
    
    for row in rows:
        price = float(row[1])
        
        # Breakeven Trigger (Protect the base)
        if not breakeven_triggered and price >= (entry_price * 1.15):
            current_stop = entry_price
            breakeven_triggered = True
            
        if price >= target:
            return "PROFIT", 50.0, row[0].strftime('%Y-%m-%d'), price
        if price <= current_stop:
            pnl = 0.0 if breakeven_triggered else -15.0
            return "LOSS", pnl, row[0].strftime('%Y-%m-%d'), price
            
    # If still open, calculate current PnL based on last known price
    if rows:
        last_price = float(rows[-1][1])
        floating_pnl = ((last_price - entry_price) / entry_price) * 100
        return "OPEN", round(floating_pnl, 2), "N/A", last_price
        
    return "OPEN", 0.0, "N/A", entry_price

def run_backtest():
    start_date = datetime(2026, 3, 1)
    end_date = "2026-05-01"
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
            cur.execute("""
                SELECT close 
                FROM daily_ohlcv 
                WHERE symbol = %s 
                AND time >= %s::timestamp - interval '24 hours'
                AND time <= %s::timestamp + interval '24 hours'
                ORDER BY ABS(EXTRACT(EPOCH FROM (time - %s::timestamp)))
                LIMIT 1
            """, (symbol, date, date, date))
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
            "market_regime": run_data.get("macro_regime", "UNKNOWN"),
            "gate_1_candidates": run_data["candidates"],
            "gate_1_incubator": run_data.get("incubator", []),
            "gate_2_approved": run_data["approved"],
            "agent_scores": run_data.get("agent_scores", {}),
            "trades": trades
        })
        
        # Incremental Save for real-time study
        with open("backtest_results.json", "w") as f:
            json.dump(results, f, indent=4)
            
    logging.info("Backtest complete. Results saved to backtest_results.json")

if __name__ == "__main__":
    run_backtest()
