import os
import json
import time
import polars as pl
from datetime import datetime, timezone
from dotenv import load_dotenv
from kiteconnect import KiteConnect

from agents.llm_ranking_agent import LLMRankingAgent
from core.live_trading import execute_trade, get_kite_data_client, get_kite_exec_client
from core.sector_mapping import get_sector_for_symbol

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
        
    # 3. Fetch Fresh Intraday Data
    print("📥 Fetching fresh intraday hourly candles...")
    try:
        from pipeline.intraday_ingestion import IntradayIngestionEngine
        engine = IntradayIngestionEngine()
        engine.fetch_data()
    except Exception as e:
        print(f"❌ Error fetching fresh intraday data: {e}")
        return
        
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
        pl.col("volume").rolling_sum(window_size=3).over("symbol").alias("vol_sum_3"),
        pl.col("time").dt.date().alias("date"),
        (pl.col("close") * pl.col("volume")).alias("pv")
    ])
    
    df = df.with_columns([
        pl.col("volume").cum_sum().over(["symbol", "date"]).alias("cum_vol"),
        pl.col("pv").cum_sum().over(["symbol", "date"]).alias("cum_pv")
    ])
    
    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (pl.col("vol_sum_3") / (3 * pl.col("vol_avg_20"))).alias("vol_surge_3h"),
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap"),
        ((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20") * 100).alias("dist_sma20"),
        ((pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("upper_wick_pct")
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
    
    if latest_time != target_utc:
        print(f"🛑 STALE DATA DETECTED! Expected {target_utc}, but DB only has {latest_time}.")
        print("Aborting run to prevent trading on old signals. Make sure intraday_ingestion.py is running.")
        return
    
    current_hour_df = df.filter(pl.col("time") == latest_time)
    
    # 4. Math Engine (Dual-Track Volume + Dual-Tier Gap & Go)
    base_filter = (pl.col("bbw") < 0.22) & (pl.col("close") > pl.col("sma_20")) & (pl.col("close") > pl.col("open"))
    
    track_a = (pl.col("vol_surge") > 2.5)
    track_b = (pl.col("vol_surge_3h") > 1.5) & (pl.col("vol_surge") > 1.1) & (pl.col("close") > pl.col("vwap"))
    volume_filter = (track_a | track_b)
    
    tier_1 = (pl.col("dist_sma20") <= 3.0) & (pl.col("upper_wick_pct") < 25.0)
    tier_2 = (pl.col("dist_sma20") > 3.0) & (pl.col("dist_sma20") <= 7.5) & (pl.col("upper_wick_pct") < 12.0) & (pl.col("vol_surge") > 3.5)
    tier_filter = (tier_1 | tier_2)
    
    breakouts = current_hour_df.filter(
        base_filter & volume_filter & tier_filter
    ).sort("vol_surge", descending=True).head(4)
    
    valid_symbols = [row['symbol'] for row in breakouts.iter_rows(named=True)]
    print(f"🎯 Math Engine found {len(valid_symbols)} top-tier Breakouts: {valid_symbols}")
    
    # 4.5 Macro Sector Veto Gate
    approved_symbols = []
    if len(valid_symbols) > 0:
        print("\n🌍 Running Macro Sector Veto Gate...")
        sector_tokens = {
            "NIFTY BANK": 260105,
            "NIFTY PSU BANK": 273673,
            "NIFTY IT": 259337,
            "NIFTY PHARMA": 260361,
            "NIFTY REALTY": 261385,
            "NIFTY METAL": 260617,
            "NIFTY FMCG": 258825,
            "NIFTY AUTO": 259849,
            "NIFTY CONSUMPTION": 264201,
            "NIFTY ENERGY": 264969,
            "NIFTY INFRA": 263689,
            "NIFTY FIN SERVICE": 257801,
        }
        
        today_str = now_ist.strftime('%Y-%m-%d')
        
        for symbol in valid_symbols:
            sector_name = get_sector_for_symbol(symbol)
            if not sector_name or sector_name not in sector_tokens:
                # If no mapping, we default to pass
                approved_symbols.append(symbol)
                continue
                
            token = sector_tokens[sector_name]
            try:
                hist = kite_data.historical_data(
                    instrument_token=token,
                    from_date=f"{today_str} 09:15:00",
                    to_date=f"{today_str} 15:30:00",
                    interval="day"
                )
                if hist and len(hist) > 0:
                    open_price = hist[0]['open']
                    close_price = hist[-1]['close'] # latest price for today
                    intraday_return = ((close_price - open_price) / open_price) * 100
                    
                    if intraday_return < 0.0:
                        print(f"🛑 REJECTED: {symbol} (Sector {sector_name} is bleeding: {intraday_return:.2f}%)")
                    else:
                        print(f"✅ APPROVED: {symbol} (Sector {sector_name} is green: +{intraday_return:.2f}%)")
                        approved_symbols.append(symbol)
                else:
                    approved_symbols.append(symbol)
            except Exception as e:
                print(f"Warning: Failed to fetch sector data for {sector_name}: {e}")
                approved_symbols.append(symbol)
                
        valid_symbols = approved_symbols
    # 5. LLM Ranking & Execution
    if len(valid_symbols) > 0:
        agent = LLMRankingAgent()
        
        # Build structured candidates for LLM
        llm_candidates = []
        for symbol in valid_symbols:
            row = breakouts.filter(pl.col("symbol") == symbol).row(0, named=True)
            candle_color = "GREEN" if row['close'] > row['open'] else "RED"
            vwap_status = "ABOVE_VWAP" if row['close'] > row['vwap'] else "BELOW_VWAP"
            llm_candidates.append({
                "symbol": symbol,
                "candle_color": candle_color,
                "upper_wick_pct": round(row['upper_wick_pct'], 2),
                "vwap_status": vwap_status,
                "vol_surge": round(row['vol_surge'], 2)
            })
            
        top_trades = agent.rank_trades(llm_candidates, max_picks=2)
        print("\n🚀 FINAL LLM EXECUTION SIGNALS 🚀")
        
        final_approved_trades = []
        
        for i, trade in enumerate(top_trades, 1):
            symbol = trade['symbol']
            score = trade['conviction_score']
            print(f"Rank {i}: {symbol} | Score: {score}")
            
            # Dynamic Conviction Threshold
            if now.hour in [10, 11, 12]:
                threshold = 70
            else:
                threshold = 85
                
            # Fetch VWAP and Sector for multipliers
            vwap_multiplier = 1.0
            sector_multiplier = 1.0
            
            try:
                sector_index = get_sector_for_symbol(symbol)
                quote_keys = [f"NSE:{symbol}"]
                if sector_index != "NIFTY 50":
                    quote_keys.append(f"NSE:{sector_index}")
                    
                quotes = kite_data.quote(quote_keys)
                
                # VWAP Check
                if f"NSE:{symbol}" in quotes:
                    vwap = quotes[f"NSE:{symbol}"].get("average_price", 0)
                    ltp = quotes[f"NSE:{symbol}"].get("last_price", 0)
                    
                    if vwap > 0 and ltp < vwap:
                        vwap_multiplier = 0.8
                        print(f"⚠️ {symbol} has bled below VWAP (LTP: {ltp} < VWAP: {vwap}). Applying 0.8x penalty for potential block-deal trap.")
                
                # Sector Check
                if sector_index != "NIFTY 50" and f"NSE:{sector_index}" in quotes:
                    sec_ltp = quotes[f"NSE:{sector_index}"].get("last_price", 0)
                    sec_close = quotes[f"NSE:{sector_index}"].get("ohlc", {}).get("close", 0)
                    
                    if sec_close > 0 and sec_ltp > sec_close:
                        sector_multiplier = 1.05
                        print(f"✅ Sector Tailwind: {sector_index} is positive. Applying 1.05x boost to {symbol}.")
                        
            except Exception as e:
                print(f"Warning: Could not fetch quotes for multipliers for {symbol}: {e}")
                
            final_score = score * vwap_multiplier * sector_multiplier
            
            if final_score < threshold:
                print(f"⚠️ {symbol} Final Score ({final_score}) is below the required threshold ({threshold}) for hour {now.hour}. Skipping.")
                continue
                
            # Collect for Email
            final_approved_trades.append({
                "ticker": symbol,
                "score": round(final_score, 1),
                "passed": [trade.get('catalyst_summary', "Quantitative Breakout")]
            })
                
            # Execute LIVE
            execute_trade(
                symbol=symbol, 
                score=score, 
                catalyst=trade['catalyst_summary'],
                entry_time=now
            )
            
        # Send Email Alert
        if final_approved_trades:
            try:
                from alerts.email_notifier import SovereignEmailer
                emailer = SovereignEmailer()
                emailer.send_scorecard(f"Live Hourly Execution ({now.strftime('%H:%M')})", final_approved_trades)
                print(f"📧 Sent Live Hourly Execution email with {len(final_approved_trades)} approved trades!")
            except Exception as e:
                print(f"❌ Failed to send email alert: {e}")

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
