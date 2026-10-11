import polars as pl
from datetime import datetime
import os
import sys
import os
import sys

def find_momentum_stocks():
    # 1. Find Yesterday's (Aug 24) runners from Parquet
    parquet_path = "data/intraday_ohlcv.parquet"
    yesterday_runners = []
    
    if os.path.exists(parquet_path):
        df = pl.read_parquet(parquet_path)
        # 10:15 IST is 04:45 UTC. Wait, the 9:15 candle starts at 03:45 UTC.
        # Let's just find the max High of the day vs the Open of the day.
        # Actually, let's look at the first hourly candle of Aug 24 (03:45 UTC)
        aug24 = datetime(2026, 8, 24).date()
        if "date" not in df.columns:
            df = df.with_columns(pl.col("time").dt.date().alias("date"))
        
        df_yesterday = df.filter(pl.col("date") == aug24)
        
        if df_yesterday.height > 0:
            first_candle = df_yesterday.filter((pl.col("time").dt.hour() == 3) & (pl.col("time").dt.minute() == 45))
            
            first_candle = first_candle.with_columns([
                (((pl.col("high") - pl.col("open")) / pl.col("open")) * 100).alias("surge_pct")
            ])
            
            top_yesterday = first_candle.filter(pl.col("surge_pct") > 2.0).sort("surge_pct", descending=True).head(5)
            yesterday_runners = top_yesterday.select("symbol").to_series().to_list()
            print(f"Yesterday's Top 5 Runners (Aug 24): {yesterday_runners}")
    
    # 2. Find Today's (Aug 25) runners using Kite live quotes
    try:
        from kiteconnect import KiteConnect
        import json
        from dotenv import load_dotenv
        
        load_dotenv()
        api_key = os.getenv("KITE_API_KEY")
        access_token = os.getenv("KITE_ACCESS_TOKEN")
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        
        # Fetch directly from kite
        instruments = kite.instruments("NSE")
        
        import pandas as pd
        df_inst = pd.DataFrame(instruments)
        eq_df = df_inst[df_inst.instrument_type == 'EQ']
        all_symbols = [f"NSE:{sym}" for sym in eq_df.tradingsymbol.unique()][:490]
        
        quotes = kite.quote(all_symbols)
        
        today_surges = []
        for symbol, quote in quotes.items():
            if quote["ohlc"]["open"] > 0:
                surge = ((quote["last_price"] - quote["ohlc"]["open"]) / quote["ohlc"]["open"]) * 100
                gap = ((quote["ohlc"]["open"] - quote["ohlc"]["close"]) / quote["ohlc"]["close"]) * 100
                total_move = surge + gap
                today_surges.append({
                    "symbol": symbol.replace("NSE:", ""),
                    "surge": total_move
                })
        
        df_today = pd.DataFrame(today_surges)
        top_today = df_today[df_today.surge > 2.0].sort_values("surge", ascending=False).head(5)
        today_runners = top_today["symbol"].tolist()
        print(f"Today's Top 5 Runners (Aug 25): {today_runners}")
        
    except Exception as e:
        print(f"Error getting today's runners: {e}")

if __name__ == "__main__":
    find_momentum_stocks()
