import os
import json
import time
import polars as pl
from datetime import datetime, timezone
from dotenv import load_dotenv
from kiteconnect import KiteConnect

from agents.llm_ranking_agent import LLMRankingAgent
from core.live_trading import execute_trade, get_kite_data_client, get_kite_exec_client

def run_hourly_evaluation():
    load_dotenv()
    kite_data = get_kite_data_client()
    kite_exec = get_kite_exec_client()
    
    # 1. Check time - Only run if between 10:15 and 15:00
    now = datetime.now()
    if now.hour < 10 or (now.hour == 10 and now.minute < 15):
        print("Too early for intraday run. Starts at 10:15.")
        return
        
    if now.hour >= 15 and now.minute > 5:
        print("Past 15:00 execution window. (Only 3:15 squash allowed later)")
        return
        
    print(f"\n{'='*50}")
    print(f"🕒 RUNNING LIVE EVALUATION AT: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*50}")
    
    # 2. Risk Management: Momentum Decay & Max Trades
    from core.db_manager import get_all_active_positions, close_position
    active_positions = get_all_active_positions()
    live_buy_flag = os.getenv("LIVE_BUY", "False")
    
    print(f"\n🛡️ Running Risk Management Check... (Current Active Trades: {len(active_positions)})")
    
    surviving_positions = []
    for pos in active_positions:
        entry_time = pos.get("entry_time")
        if not entry_time:
            surviving_positions.append(pos)
            continue
            
        hours_open = (now - entry_time).total_seconds() / 3600
        
        # Momentum Decay Exit: Open for > 2 hours AND trailing SL never activated (meaning it never reached +50%)
        if hours_open >= 2.0 and not pos.get("trailing_active", False):
            opt_symbol = pos["option_symbol"]
            print(f"⚠️ MOMENTUM DECAY DETECTED: {opt_symbol} has been stagnant for {hours_open:.1f} hours.")
            print(f"🔪 Cutting {opt_symbol} to free up capital and avoid Theta bleed.")
            
            ltp = pos["highest_high"]
            try:
                quotes = kite_data.quote([f"NFO:{opt_symbol}"])
                if f"NFO:{opt_symbol}" in quotes:
                    ltp = quotes[f"NFO:{opt_symbol}"]["last_price"]
            except Exception as e:
                print(f"Warning: could not fetch LTP for decay exit via Data account: {e}")
                
            if live_buy_flag.lower() == "true":
                try:
                    # 1. Cancel the resting SL-M order first
                    sl_order_id = pos.get("sl_order_id")
                    if sl_order_id:
                        kite_exec.cancel_order(variety=kite_exec.VARIETY_REGULAR, order_id=sl_order_id)
                        print(f"✅ Cancelled resting SL order {sl_order_id}")
                        
                    # 2. Fire Market SELL to exit
                    kite_exec.place_order(
                        variety=kite_exec.VARIETY_REGULAR,
                        exchange=kite_exec.EXCHANGE_NFO,
                        tradingsymbol=opt_symbol,
                        transaction_type=kite_exec.TRANSACTION_TYPE_SELL,
                        quantity=pos["qty"],
                        product=kite_exec.PRODUCT_NRML,
                        order_type=kite_exec.ORDER_TYPE_MARKET
                    )
                    print(f"✅ DECAY EXIT SELL Order Placed for {opt_symbol}")
                except Exception as e:
                    print(f"❌ Failed to execute decay exit for {opt_symbol} via Exec account: {e}")
                    surviving_positions.append(pos)
                    continue
            else:
                print(f"Paper mode: Decay exited {opt_symbol}")
                
            pnl_pct = ((ltp - pos["entry_premium"]) / pos["entry_premium"]) * 100
            close_position(pos, ltp, pnl_pct, "Momentum Decay Time-Stop")
        else:
            surviving_positions.append(pos)
            
    MAX_OPEN_TRADES = 3
    if len(surviving_positions) >= MAX_OPEN_TRADES:
        print(f"🛑 MAX TRADES LIMIT REACHED ({len(surviving_positions)}/{MAX_OPEN_TRADES}). Skipping new trades to preserve capital.")
        return
        
    # 3. Fetch FNO Universe
    try:
        instruments = kite_data.instruments("NFO")
        fno_symbols = set([inst['name'] for inst in instruments if inst['instrument_type'] in ['CE', 'PE']])
    except Exception as e:
        print(f"Error fetching FNO symbols via Data account: {e}")
        return
        
    # 3. Load Parquet Data (Assuming external ingestion updates this file)
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print("No live parquet data found!")
        return
        
    df = pl.read_parquet(parquet_path)
    df = df.filter(pl.col("symbol").is_in(list(fno_symbols)))
    df = df.sort(["symbol", "time"])
    
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])
    
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
    ])
    
    # Get the latest completed candle
    # Since Kite stamps 60m candles at the START of the hour, the candle timestamped 10:15 IST is the 10:15-11:15 candle.
    # If we run at 10:15, we want the 09:15 IST (03:45 UTC) candle.
    now_utc = datetime.now(timezone.utc)
    
    # The cutoff is exactly the hour we are running for (e.g. at 10:15, the cutoff is 10:15 IST)
    # We want to drop any candles that have a timestamp >= cutoff, because they are currently forming!
    import pytz
    ist_tz = pytz.timezone('Asia/Kolkata')
    now_ist = datetime.now(ist_tz)
    
    # Calculate exactly what the completed candle timestamp should be
    if now_ist.hour == 10:
        target_hour, target_minute = 9, 15
    else:
        target_hour, target_minute = now_ist.hour - 1, 15
        
    target_ist = now_ist.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    target_utc = target_ist.astimezone(pytz.utc)
    
    print(f"Targeting Completed Candle: {target_ist.strftime('%H:%M IST')} ({target_utc})")
    
    # Filter the dataframe to only include candles up to the target_utc
    df = df.filter(pl.col("time") <= target_utc)
    
    latest_time = df.select(pl.col("time").max())[0, 0]
    print(f"Latest Candle in DB: {latest_time}")
    
    current_hour_df = df.filter(pl.col("time") == latest_time)
    
    # 4. Math Engine
    breakouts = current_hour_df.filter(
        (pl.col("bbw") < 0.22) & 
        (pl.col("vol_surge") > 2.5) & 
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") < (pl.col("sma_20") * 1.03)) & # Prevent buying overextended spikes
        (pl.col("close") > pl.col("open"))
    ).sort("vol_surge", descending=True).head(4)
    
    valid_symbols = [row['symbol'] for row in breakouts.iter_rows(named=True)]
    print(f"🎯 Math Engine found {len(valid_symbols)} top-tier Breakouts: {valid_symbols}")
    
    # 5. LLM Ranking & Execution
    if len(valid_symbols) > 0:
        agent = LLMRankingAgent()
        top_trades = agent.rank_trades(valid_symbols, max_picks=2)
        print("\n🚀 FINAL LLM EXECUTION SIGNALS 🚀")
        
        for i, trade in enumerate(top_trades, 1):
            symbol = trade['symbol']
            print(f"Rank {i}: {symbol} | Score: {trade['conviction_score']}")
            # Execute LIVE
            execute_trade(
                symbol=symbol, 
                score=trade['conviction_score'], 
                catalyst=trade['catalyst_summary'],
                entry_time=now
            )

def squash_old_positions():
    """Squares off all positions except those taken exactly around 15:00."""
    now = datetime.now()
    if now.hour != 15 or now.minute < 15:
        return # Only run after 15:15
        
    print(f"\n🧹 [15:15] SQUASHING OLD INTRADAY POSITIONS 🧹")
    
    from core.db_manager import get_all_active_positions, close_position
    positions = get_all_active_positions()
            
    if not positions:
        return
        
    kite = get_kite_client()
    live_buy_flag = os.getenv("LIVE_BUY", "False")
    
    for pos in positions:
        entry_time = pos.get("entry_time")
        if not entry_time:
            continue
            
        # If entered before 15:00, SQUASH IT. (Overnight holds only for >= 15:00 entries)
        if entry_time.hour < 15:
            opt_symbol = pos["option_symbol"]
            print(f"Squashing old trade: {opt_symbol} (Entered at {entry_time.strftime('%H:%M')})")
            
            ltp = pos["entry_premium"] # Fallback if we can't get quote
            try:
                quotes = kite.quote([f"NFO:{opt_symbol}"])
                if f"NFO:{opt_symbol}" in quotes:
                    ltp = quotes[f"NFO:{opt_symbol}"]["last_price"]
            except Exception as e:
                print(f"Warning: could not fetch LTP for squash: {e}")
                
            if live_buy_flag.lower() == "true":
                try:
                    kite.place_order(
                        variety=kite.VARIETY_REGULAR,
                        exchange=kite.EXCHANGE_NFO,
                        tradingsymbol=opt_symbol,
                        transaction_type=kite.TRANSACTION_TYPE_SELL,
                        quantity=pos["qty"],
                        product=kite.PRODUCT_NRML,
                        order_type=kite.ORDER_TYPE_MARKET
                    )
                    print(f"✅ SQUASH SELL Order Placed for {opt_symbol}")
                except Exception as e:
                    print(f"❌ Failed to squash {opt_symbol}: {e}")
                    continue # Keep it active if sell failed
            else:
                print(f"Paper mode: Squashed {opt_symbol}")
                
            # Move to closed positions
            pnl_pct = ((ltp - pos["entry_premium"]) / pos["entry_premium"]) * 100
            close_position(pos, ltp, pnl_pct, "15:15 Squash")
        else:
            print(f"Holding 3 PM Trade: {pos['option_symbol']}")


if __name__ == "__main__":
    now = datetime.now()
    if now.hour == 15 and now.minute >= 15:
        squash_old_positions()
    else:
        run_hourly_evaluation()
