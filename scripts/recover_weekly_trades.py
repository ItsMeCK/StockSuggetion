import os
import json
import polars as pl
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from agents.llm_ranking_agent import LLMRankingAgent
from kiteconnect import KiteConnect

def run_recovery():
    load_dotenv()
    
    # Try getting fno universe
    kite = KiteConnect(api_key=os.getenv("KITE_API_KEY", "").strip("'\""))
    kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN", "").strip("'\""))
    
    try:
        instruments = kite.instruments("NFO")
        fno_symbols = set([inst['name'] for inst in instruments if inst['instrument_type'] in ['CE', 'PE']])
    except Exception as e:
        print(f"Error fetching FNO symbols: {e}")
        # fallback to a known list or just use all
        fno_symbols = None
        
    parquet_path = "data/intraday_ohlcv.parquet"
    df = pl.read_parquet(parquet_path)
    
    if fno_symbols:
        df = df.filter(pl.col("symbol").is_in(list(fno_symbols)))
        
    df = df.sort(["symbol", "time"])
    
    # Calculate metrics
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
    ])
    
    agent = LLMRankingAgent()
    
    target_dates = ["2026-08-07"]
    
    all_trades = []
    
    for target_date in target_dates:
        print(f"\n{'='*50}\n📅 PROCESSING DATE: {target_date}\n{'='*50}")
        day_df = df.filter(pl.col("time").dt.to_string("%Y-%m-%d") == target_date)
        
        if len(day_df) == 0:
            print(f"No data for {target_date}")
            continue
            
        hours = day_df.select("time").unique().sort("time")
        all_hours = [row['time'] for row in hours.iter_rows(named=True)]
        
        # Grab the last available hour of the day
        hour_list = [all_hours[-1]] if all_hours else []
        print(f"DEBUG: Selected hour for 3 PM (Last hour): {hour_list}")
        
        print(f"DEBUG: Found {len(hour_list)} matching hours for 3 PM.")
        
        for current_time in hour_list:
            print(f"\n🕒 HOUR: {current_time}")
            current_hour_df = day_df.filter(pl.col("time") == current_time)
            print(f"DEBUG: Data shape for current hour: {current_hour_df.shape}")
            
            breakouts = current_hour_df.filter(
                (pl.col("bbw") < 0.22) & 
                (pl.col("vol_surge") > 2.5) & 
                (pl.col("close") > pl.col("sma_20")) &
                (pl.col("close") > pl.col("open"))
            )
            
            valid_symbols = [row['symbol'] for row in breakouts.iter_rows(named=True)]
            print(f"🎯 Math Breakouts ({len(valid_symbols)}): {valid_symbols}")
            
            if len(valid_symbols) > 0:
                top_trades = agent.rank_trades(valid_symbols, max_picks=2)
                for i, trade in enumerate(top_trades, 1):
                    symbol = trade['symbol']
                    
                    # Calculate PnL
                    symbol_df = day_df.filter(pl.col("symbol") == symbol).sort("time")
                    entry_candle = symbol_df.filter(pl.col("time") == current_time)
                    if len(entry_candle) == 0:
                        continue
                    entry_price = float(entry_candle["close"][0])
                    
                    eod_candle = symbol_df.tail(1)
                    eod_price = float(eod_candle["close"][0])
                    
                    spot_change = eod_price - entry_price
                    spot_percent = (spot_change / entry_price) * 100
                    
                    option_premium = entry_price * 0.05
                    option_change = spot_change * 0.5
                    option_pnl_percent = (option_change / option_premium) * 100
                    
                    trade_record = {
                        "date": target_date,
                        "time": str(current_time),
                        "symbol": symbol,
                        "score": trade['conviction_score'],
                        "entry_price": entry_price,
                        "eod_price": eod_price,
                        "spot_pct": spot_percent,
                        "option_pct": option_pnl_percent,
                        "catalyst": trade['catalyst_summary']
                    }
                    all_trades.append(trade_record)
                    
                    print(f"🚀 Rank {i}: {symbol} | Entry: {entry_price:.2f} | Exit: {eod_price:.2f} | Option PnL: {option_pnl_percent:+.2f}%")

    # Save to file
    out_file = "data/intraday_signals.json"
    existing = []
    if os.path.exists(out_file):
        with open(out_file, "r") as f:
            try:
                existing = json.load(f)
            except:
                pass
                
    existing.extend(all_trades)
    with open(out_file, "w") as f:
        json.dump(existing, f, indent=4)
        
    print(f"\n✅ Saved {len(all_trades)} trades to {out_file}")
    
    # Summarize PnL
    if len(all_trades) > 0:
        total_pnl = sum([t['option_pct'] for t in all_trades])
        avg_pnl = total_pnl / len(all_trades)
        print(f"\n💰 Total Average Option PnL for recovered trades: {avg_pnl:+.2f}%")

if __name__ == "__main__":
    run_recovery()
