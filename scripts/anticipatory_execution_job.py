import os
import polars as pl
from datetime import datetime
import pytz

from core.db_manager import get_anticipatory_watchlist, get_all_active_positions
from core.live_trading import execute_trade

def run_anticipatory_execution():
    print(f"[{datetime.now()}] 🚀 Initiating Anticipatory Execution Job...")
    
    # 1. Fetch the Anticipatory Watchlist from DB
    watchlist = get_anticipatory_watchlist()
    if not watchlist:
        print("No high-conviction candidates in the anticipatory watchlist.")
        return
        
    debated_symbols = [row["symbol"] for row in watchlist]
    print(f"Debated Targets: {debated_symbols}")
    
    # 2. Risk Management (Max 2 trades total across the entire system)
    active_positions = get_all_active_positions()
    current_active_count = len(active_positions)
    max_trades = 2
    
    if current_active_count >= max_trades:
        print(f"🚫 Risk Management: Already holding {current_active_count} trades. Max allowed is {max_trades}. Skipping execution.")
        return
        
    slots_available = max_trades - current_active_count
    print(f"Slots available for execution: {slots_available}")
    
    # 3. Load latest hourly market data
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"Error: {parquet_path} not found.")
        return
        
    df = pl.read_parquet(parquet_path)
    
    # Get the latest completed candle
    latest_time = df.select(pl.col("time").max()).item()
    current_hour_df = df.filter(pl.col("time") == latest_time).sort("symbol")
    
    # Calculate indicators
    current_hour_df = current_hour_df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20")
    ])
    
    current_hour_df = current_hour_df.with_columns([
        (pl.col("sma_20") + 2 * pl.col("std_20")).alias("upper_band"),
        (pl.col("sma_20") - 2 * pl.col("std_20")).alias("lower_band"),
    ])
    
    current_hour_df = current_hour_df.with_columns([
        ((pl.col("upper_band") - pl.col("lower_band")) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_sma_20")).alias("vol_surge")
    ])
    
    current_hour_df = current_hour_df.drop_nulls()
    
    # 4. Filter for only the Debated Symbols
    debated_df = current_hour_df.filter(pl.col("symbol").is_in(debated_symbols))
    
    # 5. Apply the "Silent Accumulation" Math Filter
    # Instead of > 2.5, we look for > 1.2
    anticipatory_breakouts = debated_df.filter(
        (pl.col("bbw") < 0.25) & 
        (pl.col("vol_surge") > 1.2) & 
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") > pl.col("open"))
    ).sort("vol_surge", descending=True).head(slots_available)
    
    if anticipatory_breakouts.height == 0:
        print("❌ No debated candidates passed the Silent Accumulation math filter.")
        return
        
    candidates = anticipatory_breakouts.select("symbol").to_series().to_list()
    print(f"🎯 Math Engine confirmed Silent Accumulation for: {candidates}")
    
    # 6. Execute Trades
    for sym in candidates:
        # Find the conviction score from the DB
        score = next(row["conviction_score"] for row in watchlist if row["symbol"] == sym)
        catalyst = next(row["bull_thesis"] for row in watchlist if row["symbol"] == sym)
        execute_trade(sym, score, catalyst, latest_time)

if __name__ == "__main__":
    run_anticipatory_execution()
