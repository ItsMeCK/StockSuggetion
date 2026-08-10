import json
import os
import time
import polars as pl
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from agents.llm_ranking_agent import LLMRankingAgent

def run_intraday_backtest():
    load_dotenv()
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY", "").strip("'\""))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN", "").strip("'\""))
    
    print("Fetching FNO universe...")
    try:
        instruments = kite.instruments("NFO")
        fno_symbols = set([inst['name'] for inst in instruments if inst['instrument_type'] in ['CE', 'PE']])
        print(f"Loaded {len(fno_symbols)} FNO symbols.")
    except Exception as e:
        print(f"Error fetching FNO symbols: {e}")
        return

    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print("No parquet data found.")
        return

    df = pl.read_parquet(parquet_path)
    # Filter ONLY for FNO eligible stocks
    df = df.filter(pl.col("symbol").is_in(list(fno_symbols)))
    df = df.sort(["symbol", "time"])
    
    # Calculate rolling technicals
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])
    
    # Calculate Coiled Spring metrics
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
    ])
    
    # Filter for ONLY today's dates
    today_str = datetime.now().strftime("%Y-%m-%d")
    today_df = df.filter(pl.col("time").dt.to_string("%Y-%m-%d") == today_str)
    
    # Get distinct hours
    # Get distinct hours for today
    hours = today_df.select("time").unique().sort("time")
    # Evaluate all hours for today
    raw_hour_list = [row['time'] for row in hours.iter_rows(named=True)]
    
    # Smart Filter: Ignore incomplete candles
    now_utc = datetime.now(timezone.utc)
    hour_list = []
    for current_time in raw_hour_list:
        if now_utc >= current_time + timedelta(minutes=60):
            hour_list.append(current_time)
        else:
            print(f"Skipping incomplete candle at {current_time} (closes at {current_time + timedelta(minutes=60)})")
    
    agent = LLMRankingAgent()
    
    for current_time in hour_list:
        print(f"\n{'='*50}")
        print(f"🕒 RUNNING ENGINE AT INTRADAY HOUR: {current_time}")
        print(f"{'='*50}")
        
        # Get data strictly AT or BEFORE this current hour to simulate running live at that hour
        # Actually, if we just filter for this exact hour, we get the candle that closed at this hour.
        current_hour_df = today_df.filter(pl.col("time") == current_time)
        
        breakouts = current_hour_df.filter(
            (pl.col("bbw") < 0.22) & 
            (pl.col("vol_surge") > 2.5) & 
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") > pl.col("open"))
        )
        
        valid_symbols = [row['symbol'] for row in breakouts.iter_rows(named=True)]
        print(f"🎯 Math Engine found {len(valid_symbols)} Intraday Breakouts: {valid_symbols}")
        
        if len(valid_symbols) > 0:
            top_trades = agent.rank_trades(valid_symbols, max_picks=2)
            print("\n🚀 FINAL LLM EXECUTION SIGNALS 🚀")
            
            signals_file = "data/intraday_signals.json"
            saved_signals = []
            if os.path.exists(signals_file):
                try:
                    with open(signals_file, "r") as f:
                        saved_signals = json.load(f)
                except Exception:
                    pass
                    
            for i, trade in enumerate(top_trades, 1):
                print(f"Rank {i}: {trade['symbol']} | Score: {trade['conviction_score']}")
                print(f"Catalyst: {trade['catalyst_summary']}\n")
                
                saved_signals.append({
                    "timestamp": current_time.isoformat(),
                    "symbol": trade['symbol'],
                    "rank": i,
                    "score": trade['conviction_score'],
                    "catalyst": trade['catalyst_summary']
                })
                
            os.makedirs(os.path.dirname(signals_file), exist_ok=True)
            with open(signals_file, "w") as f:
                json.dump(saved_signals, f, indent=4)

if __name__ == "__main__":
    run_intraday_backtest()
