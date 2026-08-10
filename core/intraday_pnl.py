import os
import polars as pl
from datetime import datetime

def generate_pnl_report():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print("No parquet data found.")
        return

    df = pl.read_parquet(parquet_path)
    
    # Define our Intraday Trades and their exact entry hourly candle (UTC)
    intraday_trades = [
        {"symbol": "NETWEB", "entry_time": "2026-08-03 03:45:00+00:00"},
        {"symbol": "HFCL", "entry_time": "2026-08-03 03:45:00+00:00"},
        {"symbol": "APARINDS", "entry_time": "2026-08-03 04:45:00+00:00"},
        {"symbol": "URBANCO", "entry_time": "2026-08-03 04:45:00+00:00"},
        {"symbol": "PINELABS", "entry_time": "2026-08-03 05:45:00+00:00"},
        {"symbol": "ABB", "entry_time": "2026-08-03 05:45:00+00:00"},
        {"symbol": "LTM", "entry_time": "2026-08-03 06:45:00+00:00"},
        {"symbol": "APLAPOLLO", "entry_time": "2026-08-03 06:45:00+00:00"},
        {"symbol": "REDINGTON", "entry_time": "2026-08-03 07:45:00+00:00"},
        {"symbol": "ESCORTS", "entry_time": "2026-08-03 07:45:00+00:00"},
        {"symbol": "PTCIL", "entry_time": "2026-08-03 08:45:00+00:00"},
    ]
    
    print("\n| Time | Symbol | Entry Price | EOD Price | Spot Move % | Option P&L % |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- |")
    
    total_pnl = 0
    count = 0
    
    for trade in intraday_trades:
        symbol = trade["symbol"]
        entry_time = trade["entry_time"]
        
        # Get data for this symbol on this day
        symbol_df = df.filter(
            (pl.col("symbol") == symbol) & 
            (pl.col("time").dt.to_string("%Y-%m-%d") == "2026-08-03")
        ).sort("time")
        
        if len(symbol_df) == 0:
            continue
            
        # Get entry candle (we enter at the close of this hourly candle)
        entry_candle = symbol_df.filter(pl.col("time").dt.to_string("%Y-%m-%d %H:%M:%S+00:00") == entry_time)
        if len(entry_candle) == 0:
            # Maybe the timezone string is slightly different
            entry_candle = symbol_df.filter(pl.col("time").dt.to_string("%Y-%m-%d %H:%M:%S") == entry_time[:19])
            
        if len(entry_candle) == 0:
            continue
            
        entry_price = float(entry_candle["close"][0])
        
        # Get EOD candle (the very last candle of the day for this symbol)
        eod_candle = symbol_df.tail(1)
        eod_price = float(eod_candle["close"][0])
        
        # Calculate Option PnL (Delta 0.5, Premium 5% of Spot)
        spot_change = eod_price - entry_price
        spot_percent = (spot_change / entry_price) * 100
        
        option_premium = entry_price * 0.05
        option_change = spot_change * 0.5
        option_pnl_percent = (option_change / option_premium) * 100
        
        # Convert UTC to IST for display
        hour = int(entry_time[11:13]) + 5
        minute = int(entry_time[14:16]) + 30
        if minute >= 60:
            hour += 1
            minute -= 60
        # The entry happens at the CLOSE of the candle (1 hour after start)
        hour += 1
        
        time_str = f"{hour:02d}:{minute:02d} AM" if hour < 12 else f"{hour if hour == 12 else hour-12:02d}:{minute:02d} PM"
        
        print(f"| {time_str} | **{symbol}** | ₹{entry_price:.2f} | ₹{eod_price:.2f} | {spot_percent:+.2f}% | **{option_pnl_percent:+.2f}%** |")
        
        total_pnl += option_pnl_percent
        count += 1
        
    if count > 0:
        avg_pnl = total_pnl / count
        print(f"\n**Average Intraday Option P&L for August 3:** **{avg_pnl:+.2f}%**")

if __name__ == "__main__":
    generate_pnl_report()
