import polars as pl
from datetime import datetime
import pytz

def find_missed_runners():
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    # Filter for today
    today = datetime(2026, 8, 21).date()
    df = df.with_columns(pl.col("time").dt.date().alias("date"))
    df_today = df.filter(pl.col("date") == today)
    
    symbols = df_today["symbol"].unique().to_list()
    
    runners = []
    
    for symbol in symbols:
        sym_df = df_today.filter(pl.col("symbol") == symbol).sort("time")
        if sym_df.height < 2:
            continue
            
        # Find 10:15 close (UTC 04:45:00)
        # Note: 10:15 IST is 04:45 UTC
        entry_candle = sym_df.filter(pl.col("time").dt.hour() == 4).filter(pl.col("time").dt.minute() == 45)
        
        if entry_candle.height == 0:
            continue
            
        entry_price = entry_candle["close"][0]
        
        # Find high after 10:15
        subsequent_df = sym_df.filter(pl.col("time") > entry_candle["time"][0])
        if subsequent_df.height == 0:
            continue
            
        max_high = subsequent_df["high"].max()
        
        pct_move = ((max_high - entry_price) / entry_price) * 100
        
        if pct_move >= 3.0: # Stocks that moved at least 3% after 10:15
            runners.append({
                "symbol": symbol,
                "10_15_price": entry_price,
                "max_high": max_high,
                "pct_move": pct_move
            })
            
    runners = sorted(runners, key=lambda x: x["pct_move"], reverse=True)
    
    print(f"Found {len(runners)} stocks that moved > 3% after 10:15 AM today:")
    for r in runners[:20]:
        print(f"{r['symbol']}: {r['pct_move']:.2f}% (Entry: {r['10_15_price']:.2f}, High: {r['max_high']:.2f})")

if __name__ == "__main__":
    find_missed_runners()
