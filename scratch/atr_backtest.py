import polars as pl

def backtest_atr_stops():
    print("Loading data from data/intraday_ohlcv.parquet...")
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    df = df.sort(["symbol", "time"])
    
    # Calculate True Range (TR)
    df = df.with_columns([
        pl.col("close").shift(1).over("symbol").alias("prev_close")
    ])
    df = df.with_columns([
        (pl.col("high") - pl.col("low")).alias("hl"),
        (pl.col("high") - pl.col("prev_close")).abs().alias("hpc"),
        (pl.col("low") - pl.col("prev_close")).abs().alias("lpc")
    ])
    df = df.with_columns([
        pl.max_horizontal("hl", "hpc", "lpc").alias("tr")
    ])
    
    # Calculate 14-period ATR
    df = df.with_columns([
        pl.col("tr").rolling_mean(window_size=14).over("symbol").alias("atr_14")
    ])
    
    # Filter for the most recent day in the dataset
    latest_date = df.select(pl.col("time").dt.date().max()).item()
    print(f"Latest trading date in dataset: {latest_date}")
    
    today_df = df.filter(pl.col("time").dt.date() == latest_date)
    symbols = today_df.select("symbol").unique().to_series().to_list()
    
    # Multiplier for the ATR trailing stop
    ATR_MULTIPLIER = 2.0
    
    print("\n--- Analysing Trades for ATR Stop ---")
    for sym in symbols:
        sym_df = today_df.filter(pl.col("symbol") == sym).sort("time")
        if sym_df.height < 5: 
            continue
            
        entry_price = sym_df["open"][0]
        exit_price = sym_df["close"][-1]
        pnl_pct = ((exit_price - entry_price) / entry_price) * 100
        
        # Look for the specific trades that lost around -0.47% or -0.43%
        if -0.60 <= pnl_pct <= -0.35:
            print(f"\nPotential Match: {sym} | Close PnL: {pnl_pct:.2f}% (Entry: {entry_price}, Exit: {exit_price})")
            
            # Simulate ATR trailing stop logic
            highest_high = entry_price
            stop_loss_triggered = False
            
            for row in sym_df.iter_rows(named=True):
                if row["high"] > highest_high:
                    highest_high = row["high"]
                
                current_atr = row["atr_14"]
                if current_atr is None: 
                    continue
                
                trail_stop = highest_high - (ATR_MULTIPLIER * current_atr)
                
                if row["low"] <= trail_stop:
                    sl_pnl = ((trail_stop - entry_price) / entry_price) * 100
                    print(f"  -> [CUT EARLY] ATR Trailing Stop hit at {trail_stop:.2f} (Time: {row['time']}). New PnL: {sl_pnl:.2f}%")
                    stop_loss_triggered = True
                    break
            
            if not stop_loss_triggered:
                print(f"  -> [HELD] ATR Trailing Stop (2x) would NOT have triggered. The stop was too wide to cut this loss.")

if __name__ == "__main__":
    backtest_atr_stops()
