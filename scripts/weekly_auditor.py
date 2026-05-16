import os
import logging
import psycopg2
import polars as pl
from datetime import datetime, timedelta
from run_historical import run_historical_engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_price_data(symbol, start_date, days=3):
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    query = f"""
        SELECT time, open, high, low, close 
        FROM daily_ohlcv 
        WHERE symbol = '{symbol}' AND time >= '{start_date}'
        ORDER BY time ASC LIMIT {days+1}
    """
    df = pl.read_database(query, conn)
    conn.close()
    return df

def simulate_trade(symbol, entry_date, entry_price):
    df = get_price_data(symbol, entry_date, days=3)
    if len(df) < 2:
        return "INSUFFICIENT_DATA", 0.0, None, 0.0

    next_days = df.filter(pl.col("time").dt.date() > datetime.strptime(entry_date, "%Y-%m-%d").date())
    
    for i in range(min(2, len(next_days))):
        row = next_days.row(i, named=True)
        if (row['low'] - entry_price) / entry_price <= -0.05:
            return "STOP_LOSS_5%", entry_price * 0.95, row['time'], -5.0
            
    if len(next_days) >= 2:
        exit_row = next_days.row(1, named=True)
        pnl = (exit_row['close'] - entry_price) / entry_price
        return "EXIT_2_DAYS", exit_row['close'], exit_row['time'], pnl * 100
    elif len(next_days) == 1:
        exit_row = next_days.row(0, named=True)
        pnl = (exit_row['close'] - entry_price) / entry_price
        return "EXIT_1_DAY", exit_row['close'], exit_row['time'], pnl * 100
        
    return "HELD", 0.0, None, 0.0

def run_weekly_audit():
    # Force HISTORICAL mode for simulation
    os.environ["TRADING_MODE"] = "HISTORICAL"
    
    dates = ["2026-05-11", "2026-05-12", "2026-05-13", "2026-05-14", "2026-05-15"]
    all_trades = []

    for date in dates:
        logging.info(f"Auditing {date}...")
        result = run_historical_engine(date)
        
        for symbol in result['approved']:
            price_df = get_price_data(symbol, date, days=1)
            if not price_df.is_empty():
                entry_price = price_df.row(0, named=True)['close']
                status, exit_price, exit_date, pnl = simulate_trade(symbol, date, entry_price)
                all_trades.append({
                    "date": date,
                    "symbol": symbol,
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "exit_date": exit_date.strftime("%Y-%m-%d") if exit_date else "N/A",
                    "status": status,
                    "pnl": pnl
                })

    # Print Markdown Table
    print("\n| Date | Symbol | Entry Price | Exit Price | Exit Date | Status | PnL% |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    for t in all_trades:
        print(f"| {t['date']} | {t['symbol']} | {t['entry_price']:.2f} | {t['exit_price']:.2f} | {t['exit_date']} | {t['status']} | {t['pnl']:+.2f}% |")

if __name__ == "__main__":
    run_weekly_audit()
