import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

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
    from core.db_manager import get_all_active_positions, close_position, get_active_daily_focus_watchlist
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

    # Level 4 Execution Filter: Target Level 1 High-Beta Pool (+₹2.42L Institutional Edge)
    from core.watchlist_manager import DEFAULT_DEFENSIVE_BLACKLIST
    
    weekly_file = "data/weekly_eligible_universe.json"
    eligible_symbols = set()
    if os.path.exists(weekly_file):
        try:
            with open(weekly_file, "r") as f:
                w_data = json.load(f)
                eligible_symbols = {item["symbol"] for item in w_data.get("eligible_candidates", []) if "symbol" in item}
        except Exception as e:
            print(f"⚠️ Could not load weekly eligible universe: {e}")

    # Fallback to fno_symbols if weekly universe is unpopulated
    if not eligible_symbols:
        eligible_symbols = set(fno_symbols)

    # Exclude defensive blacklist (SBILIFE, HINDUNILVR, etc.) & keep NIFTY 50 for RS/Macro calculations
    high_beta_universe = (eligible_symbols - DEFAULT_DEFENSIVE_BLACKLIST).union({"NIFTY 50"})
    valid_targets = high_beta_universe.intersection(set(fno_symbols).union({"NIFTY 50"}))

    active_watchlist = get_active_daily_focus_watchlist()
    active_focus_count = len(active_watchlist) if active_watchlist else 0
    print(f"🎯 Scanning {len(valid_targets) - 1} Level 1 High-Beta Candidates (+₹2.42L Institutional Edge with Defensives Excluded)")
    print(f"⭐ Active Focus Watchlist holds {active_focus_count} priority candidates.")
    df = df.filter(pl.col("symbol").is_in(list(valid_targets)))

    df = df.sort(["symbol", "time"])
    
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").ewm_mean(span=50, adjust=False).over("symbol").alias("ema_50"),
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
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap"),
        (pl.col("close") * pl.col("volume")).rolling_mean(window_size=20*6).over("symbol").alias("atv") # 20-day Average Traded Value proxy
    ])

    # Extension Metrics (To prevent buying Liquidity Sweeps)
    df = df.with_columns([
        ((pl.col("close") - pl.col("vwap")) / pl.col("vwap") * 100).alias("vwap_ext_pct"),
        ((pl.col("close") - pl.col("open").first()) / pl.col("open").first() * 100).over(["symbol", "date"]).alias("intraday_ext_pct")
    ])

    # Dynamic BBW Percentile Calculation
    df = df.with_columns([
        ((pl.col("sma_20") + 2 * pl.col("std_20") - (pl.col("sma_20") - 2 * pl.col("std_20"))) / pl.col("sma_20")).alias("bbw")
    ])
    df = df.with_columns([
        pl.col("bbw").rolling_quantile(quantile=0.10, window_size=100).over("symbol").alias("bbw_threshold_10th")
    ])

    # IAF (Institutional Accumulation Footprint) Metrics
    df = df.with_columns([
        pl.max_horizontal(
            (pl.col("high") - pl.col("low")),
            (pl.col("high") - pl.col("close").shift(1).over("symbol")).abs(),
            (pl.col("low") - pl.col("close").shift(1).over("symbol")).abs()
        ).alias("true_range")
    ])
    df = df.with_columns([
        pl.col("true_range").rolling_mean(window_size=14).over("symbol").alias("atr_14")
    ])

    # Dynamic Breakout Energy (Z-Score)
    df = df.with_columns([
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_expansion"),
        (pl.col("true_range") / pl.col("atr_14")).alias("range_expansion")
    ])
    df = df.with_columns([
        (pl.col("vol_expansion") * pl.col("range_expansion")).alias("breakout_energy")
    ])
    df = df.with_columns([
        pl.col("breakout_energy").rolling_mean(window_size=50).over("symbol").alias("energy_mean"),
        pl.col("breakout_energy").rolling_std(window_size=50).over("symbol").alias("energy_std")
    ])
    df = df.with_columns([
        ((pl.col("breakout_energy") - pl.col("energy_mean")) / pl.col("energy_std")).alias("energy_zscore")
    ])

    # Time-Weighted Volume Decay (TWVD)
    df = df.with_columns([
        pl.when(pl.col("close") > pl.col("open")).then(1).otherwise(-1).alias("direction")
    ])
    df = df.with_columns([
        pl.when(pl.col("volume") > 1.5 * pl.col("vol_avg_20"))
        .then(pl.col("direction") * (pl.col("volume") / pl.col("vol_avg_20")))
        .otherwise(0)
        .alias("surge_magnitude")
    ])
    df = df.with_columns([
        pl.col("surge_magnitude").ewm_mean(alpha=0.2, ignore_nulls=True).over("symbol").alias("twvd")
    ])
    df = df.with_columns([
        pl.col("low").shift(1).over("symbol").alias("low_1"),
        pl.col("low").shift(2).over("symbol").alias("low_2"),
        pl.col("true_range").shift(1).over("symbol").alias("tr_1"),
        pl.col("true_range").shift(2).over("symbol").alias("tr_2"),
        ((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("close_range_pct"),
        pl.col("volume").shift(1).over("symbol").alias("vol_1")
    ])

    # Dynamic Slope/Velocity Metrics
    df = df.with_columns([
        pl.col("vwap").shift(3).over(["symbol", "date"]).alias("vwap_prev3"),
        pl.col("sma_20").shift(3).over("symbol").alias("sma_20_prev3"),
    ])
    df = df.with_columns([
        (((pl.col("vwap") - pl.col("vwap_prev3")) / pl.col("vwap_prev3")) * 100).alias("vwap_slope_pct"),
        (((pl.col("sma_20") - pl.col("sma_20_prev3")) / pl.col("sma_20_prev3")) * 100).alias("sma20_slope_pct"),
    ])
    
    # 3-Hour Return for RS Divergence
    df = df.with_columns([
        (((pl.col("close") - pl.col("close").shift(3).over("symbol")) / pl.col("close").shift(3).over("symbol")) * 100).alias("ret_3h")
    ])

    df = df.with_columns([
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (pl.col("vol_sum_3") / (3 * pl.col("vol_avg_20"))).alias("vol_surge_3h"),
        ((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20") * 100).alias("dist_sma20"),
        ((pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("upper_wick_pct"),
        ((pl.min_horizontal(pl.col("open"), pl.col("close")) - pl.col("low")) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("lower_wick_pct"),
        (pl.col("high") - pl.col("low")).alias("candle_range")
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
    
    # Pre-calculate Nifty 50 RS Divergence Benchmark & Macro Regime
    nifty_df = current_hour_df.filter(pl.col("symbol") == "NIFTY 50")
    nifty_ret_3h = 0.0
    from core.macro_regime import classify_macro_regime
    from core.fo_ban_manager import get_banned_fo_symbols, is_symbol_banned
    
    banned_fo_set = get_banned_fo_symbols()
    macro_regime = {"regime": "NEUTRAL_CHOP", "allow_ce": True, "allow_pe": True, "size_multiplier": 1.0}

    if not nifty_df.is_empty():
        nifty_ret_3h = nifty_df.select(pl.col("ret_3h"))[0, 0]
        if nifty_ret_3h is None: nifty_ret_3h = 0.0
        n_close = nifty_df.select(pl.col("close"))[0, 0] or 0.0
        n_sma20 = nifty_df.select(pl.col("sma_20"))[0, 0] or 0.0
        n_sma50 = nifty_df.select(pl.col("ema_50"))[0, 0] or 0.0
        if n_close > 0 and n_sma20 > 0:
            macro_regime = classify_macro_regime(n_close, n_sma20, n_sma50)
            print(f"🌍 MACRO REGIME ALIGNMENT: {macro_regime['regime']} -> {macro_regime['description']}")
    
    # 4. PIM Math Engine (Predictive Institutional Model)
    # Dynamic Coil (BBW in lowest 10% historically OR extremely tight absolute < 0.22 as fallback)
    dynamic_coil = (pl.col("bbw") < pl.col("bbw_threshold_10th")) | (pl.col("bbw") < 0.22)
    base_filter = dynamic_coil & (pl.col("close") > pl.col("sma_20")) & (pl.col("close") > pl.col("open"))
    
    # Moving Average Awareness: Ascending VWAP (> 0.1% slope) + Flat-to-Neutral SMA20
    ma_awareness = (pl.col("vwap_slope_pct") > 0.1) & (pl.col("sma20_slope_pct") > -0.05)
    
    # Track A: Breakout Thrust (Momentum Climax)
    track_a = (pl.col("vol_surge") > 2.5) & (pl.col("upper_wick_pct") < 25.0)
    
    # Track B: Stealth Accumulation (Multi-Candle IAF + Dynamic ATV Tiers)
    mega_cap_stealth = (pl.col("atv") >= 500_000_000) & (pl.col("vol_surge_3h") >= 1.60) & (pl.col("close") <= (pl.col("vwap") * 1.015))
    mid_cap_stealth = (pl.col("atv") < 500_000_000) & (pl.col("atv") >= 100_000_000) & (pl.col("vol_surge_3h") >= 1.83) & (pl.col("close") <= (pl.col("vwap") * 1.010))
    
    multi_candle_iaf = (
        (pl.col("low") >= pl.col("low_1")) & (pl.col("low_1") >= pl.col("low_2")) & # Higher Lows
        (pl.col("true_range") <= pl.col("tr_1")) & (pl.col("tr_1") <= pl.col("tr_2")) & # Range Contraction
        (pl.col("volume") >= pl.col("vol_1")) & # Churn/Absorption
        (pl.col("close_range_pct") <= 85.0) # Avoid top 15% closes (hiding from screeners)
    )
    
    # Anti-FOMO Veto: Prevent buying the absolute top of a Liquidity Sweep
    # 1. Reject if extended > 2.5% above VWAP (Mean Reversion Trap)
    # 2. Reject if already up > 6.0% intraday from the open (Exhaustion)
    # 3. Reject if Breakout Energy Z-Score < 1.0 (Low Energy Churn)
    # 4. Reject if TWVD <= 0 (Persistent Distribution)
    anti_fomo_filter = (
        (pl.col("vwap_ext_pct") <= 2.5) & 
        (pl.col("intraday_ext_pct") <= 6.0) & 
        (pl.col("energy_zscore") > 1.0) & 
        (pl.col("twvd") > 0.0)
    )
    
    # 4.3 Liquidity Sweep (The "Dip Before The Rip")
    # A true institutional stop-hunt footprint
    liquidity_sweep = (
        (pl.col("sma_20") > pl.col("ema_50")) &              # Macro uptrend intact
        (pl.col("low") < pl.col("vwap")) &                   # Action: Swept below VWAP to grab stops
        (pl.col("close") > pl.col("vwap")) &                 # Action: Recovered above VWAP showing absorption
        (pl.col("lower_wick_pct") >= 45.0) &                 # Footprint: Massive rejection wick (>45% of candle)
        (pl.col("candle_range") >= (0.8 * pl.col("atr_14"))) & # Significance: Must be a volatile flush, not a tiny doji
        (pl.col("vol_surge") > 1.2)                          # Volume: Institutional accumulation footprint
    )

    track_b = (mega_cap_stealth | mid_cap_stealth) & multi_candle_iaf & ma_awareness
    
    # Relative Strength (RS) Divergence: Must outperform Nifty by at least 0.5% over 3 hours
    rs_divergence = (pl.col("ret_3h") > (nifty_ret_3h + 0.5))
    
    breakouts = current_hour_df.filter(
        base_filter & (track_a | track_b | liquidity_sweep) & rs_divergence & anti_fomo_filter
    ).sort("vol_surge", descending=True).head(3)
    
    # 4.4 Short Breakdown Engine (PE) - Bi-Directional Execution
    short_base_filter = dynamic_coil & (pl.col("close") < pl.col("sma_20")) & (pl.col("close") < pl.col("open"))
    short_thrust = (pl.col("vol_surge") >= 1.4) & (pl.col("vol_surge") <= 3.2) & (pl.col("close_range_pct") <= 25.0)
    rw_divergence = (pl.col("ret_3h") < (nifty_ret_3h - 0.5))
    short_anti_fomo = (
        (pl.col("vwap_ext_pct") >= -2.5) & 
        (pl.col("intraday_ext_pct") >= -6.0) & 
        (pl.col("energy_zscore") > 1.0)
    )
    breakdowns = current_hour_df.filter(
        short_base_filter & short_thrust & rw_divergence & short_anti_fomo
    ).sort("vol_surge", descending=True).head(3)

    raw_candidates = []
    if macro_regime.get("allow_ce", True):
        for row in breakouts.iter_rows(named=True):
            sym = row['symbol']
            if sym != "NIFTY 50":
                if is_symbol_banned(sym, banned_fo_set):
                    print(f"🛑 VETO: {sym} is in NSE F&O Ban Period (MWPL > 95%). Skipping entry.")
                    continue
                raw_candidates.append({
                    "symbol": sym,
                    "direction": "LONG",
                    "instrument_type": "CE",
                    "row": row
                })
    else:
        print(f"🔒 Macro Regime ({macro_regime['regime']}) blocks Call (CE) buying to avoid falling-knife traps.")

    if macro_regime.get("allow_pe", True):
        for row in breakdowns.iter_rows(named=True):
            sym = row['symbol']
            if sym != "NIFTY 50":
                if is_symbol_banned(sym, banned_fo_set):
                    print(f"🛑 VETO: {sym} is in NSE F&O Ban Period (MWPL > 95%). Skipping entry.")
                    continue
                raw_candidates.append({
                    "symbol": sym,
                    "direction": "SHORT",
                    "instrument_type": "PE",
                    "row": row
                })
    else:
        print(f"🔒 Macro Regime ({macro_regime['regime']}) blocks Put (PE) buying to avoid fighting bull market momentum.")
            
    print(f"🎯 PIM Engine found {len(raw_candidates)} Bi-Directional Candidates: {[(c['symbol'], c['direction']) for c in raw_candidates]}")
    
    # 4.5 Macro Sector Veto Gate
    approved_candidates = []
    if len(raw_candidates) > 0:
        print("\n🌍 Running Macro Sector Veto Gate...")
        sector_tokens = {
            "NIFTY 50": 256265,
            "NIFTY BANK": 260105,
            "NIFTY PSU BANK": 262921,
            "NIFTY IT": 259849,
            "NIFTY PHARMA": 262409,
            "NIFTY REALTY": 261129,
            "NIFTY METAL": 263689,
            "NIFTY FMCG": 261897,
            "NIFTY AUTO": 263433,
            "NIFTY CONSUMPTION": 257545,
            "NIFTY ENERGY": 261641,
            "NIFTY INFRA": 261385,
            "NIFTY FIN SERVICE": 257801,
        }
        
        today_str = now_ist.strftime('%Y-%m-%d')
        
        for cand in raw_candidates:
            symbol = cand['symbol']
            direction = cand['direction']
            sector_name = get_sector_for_symbol(symbol)
            if not sector_name or sector_name not in sector_tokens:
                approved_candidates.append(cand)
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
                    close_price = hist[-1]['close']
                    intraday_return = ((close_price - open_price) / open_price) * 100
                    
                    nifty_return = 0.0
                    try:
                        nifty_hist = kite_data.historical_data(
                            instrument_token=256265,
                            from_date=f"{today_str} 09:15:00",
                            to_date=f"{today_str} 15:30:00",
                            interval="day"
                        )
                        if nifty_hist and len(nifty_hist) > 0:
                            n_open = nifty_hist[0]['open']
                            n_close = nifty_hist[-1]['close']
                            nifty_return = ((n_close - n_open) / n_open) * 100
                    except Exception as e:
                        print(f"Warning: Failed to fetch NIFTY 50 return for relative velocity: {e}")
                        
                    relative_velocity = intraday_return - nifty_return
                    
                    if direction == "LONG":
                        if intraday_return < 0.0 and relative_velocity < 0.0:
                            print(f"🛑 REJECTED LONG: {symbol} (Sector {sector_name} bleeding {intraday_return:.2f}% and underperforming Nifty)")
                        else:
                            print(f"✅ APPROVED LONG: {symbol} (Sector {sector_name}: {intraday_return:.2f}%)")
                            approved_candidates.append(cand)
                    else:
                        if intraday_return > 0.5 and relative_velocity > 0.2:
                            print(f"🛑 REJECTED SHORT: {symbol} (Sector {sector_name} rallying +{intraday_return:.2f}% outperforming Nifty)")
                        else:
                            print(f"✅ APPROVED SHORT: {symbol} (Sector {sector_name}: {intraday_return:.2f}%)")
                            approved_candidates.append(cand)
                else:
                    approved_candidates.append(cand)
            except Exception as e:
                print(f"Warning: Failed to fetch sector data for {sector_name}: {e}")
                approved_candidates.append(cand)
                
    # 4.75 15m Micro-Structure Veto Gate
    mtf_approved_candidates = []
    if len(approved_candidates) > 0:
        print("\n🔬 Running 15m Micro-Structure Veto Gate...")
        from core.mtf_helper import fetch_15m_structure
        
        for cand in approved_candidates:
            symbol = cand['symbol']
            direction = cand['direction']
            mtf_data = fetch_15m_structure(kite_data, symbol, target_utc)
            if not mtf_data:
                print(f"⚠️ Skipping MTF check for {symbol} (No data)")
                mtf_approved_candidates.append(cand)
                continue
                
            hourly_vol = mtf_data['hourly_vol']
            final_15m_vol = mtf_data['final_15m_vol']
            has_consecutive_higher_highs = mtf_data['has_consecutive_higher_highs']
            is_vol_climax = mtf_data['is_vol_climax']
            
            if direction == "LONG":
                if not has_consecutive_higher_highs and not is_vol_climax:
                    print(f"🛑 VETO LONG: {symbol} (Pump & Churn: 1H accumulation lacks 15m structural synergy)")
                    continue
                is_red = mtf_data['final_15m_close'] < mtf_data['final_15m_open']
                if is_red and final_15m_vol > (hourly_vol * 0.35):
                    print(f"🛑 VETO LONG: {symbol} (Distribution Dump: Final 15m is red and holds >35% vol)")
                    continue
            else:
                is_green = mtf_data['final_15m_close'] > mtf_data['final_15m_open']
                if is_green and final_15m_vol > (hourly_vol * 0.35):
                    print(f"🛑 VETO SHORT: {symbol} (Short-Covering Bounce: Final 15m is strong green with >35% vol)")
                    continue
                    
            print(f"✅ APPROVED MTF: {symbol} ({direction})")
            mtf_approved_candidates.append(cand)

    final_approved_trades = []
    rejected_trades = []

    # 5. Intraday Multi-Agent Debate & Execution
    if len(mtf_approved_candidates) > 0:
        from agents.intraday_debate.intraday_orchestrator import IntradayDebateOrchestrator
        orchestrator = IntradayDebateOrchestrator()
        
        debate_candidates = []
        for cand in mtf_approved_candidates:
            symbol = cand['symbol']
            direction = cand['direction']
            inst_type = cand['instrument_type']
            row = cand['row']
            candle_color = "GREEN" if row['close'] > row['open'] else "RED"
            vwap_status = "ABOVE_VWAP" if row['close'] > row['vwap'] else "BELOW_VWAP"
            context = f"Direction: {direction} ({inst_type}), Candle: {candle_color}, Wick: {round(row['upper_wick_pct'], 2)}%, VWAP: {vwap_status}, Vol Surge: {round(row['vol_surge'], 2)}x"
            debate_candidates.append({
                "symbol": symbol,
                "direction": direction,
                "instrument_type": inst_type,
                "context": context
            })
            
        print(f"\n🧠 Spinning up Multi-Agent Debate for {len(debate_candidates)} symbols...")
        results = orchestrator.evaluate_all_sync(debate_candidates)
        
        top_trades = []
        active_focus_symbols = {item['symbol'] for item in active_watchlist if item.get('symbol')} if active_watchlist else set()
        for cand, res in zip(debate_candidates, results):
            conviction = res.conviction_score
            is_focus = cand['symbol'] in active_focus_symbols
            if is_focus:
                conviction = min(100.0, conviction + 5.0)

            top_trades.append({
                "symbol": cand['symbol'],
                "direction": cand['direction'],
                "instrument_type": cand['instrument_type'],
                "conviction_score": conviction,
                "catalyst_summary": (f"[Focus Watchlist ⭐] " if is_focus else "") + res.primary_catalyst,
                "bull_thesis": res.bull_thesis,
                "bear_thesis": res.bear_thesis,
                "arbiter_verdict": res.arbiter_verdict
            })
        
        top_trades = sorted(top_trades, key=lambda x: x['conviction_score'], reverse=True)
        top_2_trades = top_trades[:2]
        
        print("\n🚀 FINAL LLM EXECUTION SIGNALS 🚀")
        
        for trade in top_trades[2:]:
            rejected_trades.append({
                "ticker": trade['symbol'],
                "score": trade['conviction_score'],
                "reason": "Ranked outside Top 2 by LLM Arbiter"
            })
        
        for i, trade in enumerate(top_2_trades, 1):
            symbol = trade['symbol']
            direction = trade['direction']
            inst_type = trade['instrument_type']
            score = trade['conviction_score']
            print(f"Rank {i}: {symbol} ({direction} {inst_type}) | Score: {score}")
            
            # Dynamic Conviction Threshold
            if now.hour in [10, 11, 12]:
                threshold = 70
            else:
                threshold = 75
                
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
                    
                    if direction == "LONG":
                        if vwap > 0 and ltp < vwap:
                            vwap_multiplier = 0.8
                            print(f"⚠️ {symbol} LONG has bled below VWAP (LTP: {ltp} < VWAP: {vwap}). Applying 0.8x penalty.")
                    else:
                        if vwap > 0 and ltp > vwap:
                            vwap_multiplier = 0.8
                            print(f"⚠️ {symbol} SHORT has rallied above VWAP (LTP: {ltp} > VWAP: {vwap}). Applying 0.8x penalty.")
                
                # Sector Check
                if sector_index != "NIFTY 50" and f"NSE:{sector_index}" in quotes:
                    sec_ltp = quotes[f"NSE:{sector_index}"].get("last_price", 0)
                    sec_close = quotes[f"NSE:{sector_index}"].get("ohlc", {}).get("close", 0)
                    
                    if direction == "LONG":
                        if sec_close > 0 and sec_ltp > sec_close:
                            sector_multiplier = 1.05
                            print(f"✅ Sector Tailwind: {sector_index} is positive for {symbol} LONG.")
                    else:
                        if sec_close > 0 and sec_ltp < sec_close:
                            sector_multiplier = 1.05
                            print(f"✅ Sector Tailwind: {sector_index} is falling for {symbol} SHORT.")
                        
            except Exception as e:
                print(f"Warning: Could not fetch quotes for multipliers for {symbol}: {e}")
                
            final_score = score * vwap_multiplier * sector_multiplier
            
            if final_score < threshold:
                print(f"⚠️ {symbol} Final Score ({final_score}) is below the required threshold ({threshold}) for hour {now.hour}. Skipping.")
                rejected_trades.append({
                    "ticker": symbol,
                    "score": round(final_score, 1),
                    "reason": f"Final Score ({final_score:.1f}) < Threshold ({threshold})"
                })
                continue
                
            final_approved_trades.append({
                "ticker": symbol,
                "score": round(final_score, 1),
                "passed": [f"{direction} {inst_type}: " + trade.get('catalyst_summary', "Breakout/Breakdown")]
            })
                
            # Session Routing: Morning Paper (10:15, 11:15) vs Afternoon Live BTST (12:15, 13:15, 14:15)
            from datetime import time as dt_time
            now_time = now.time()
            is_afternoon_session = (now_time >= dt_time(12, 15) and now_time <= dt_time(15, 15))
            
            holding_type = "TYPE_C_BTST" if is_afternoon_session else "TYPE_A_SCALP"
            engine_type = "ENGINE_2_BTST" if is_afternoon_session else "ENGINE_1_HOURLY"
            session_label = "LIVE AFTERNOON BTST" if is_afternoon_session else "MORNING PAPER SCALP (₹0 Live Risk)"

            # Execute trade
            execute_trade(
                symbol=symbol, 
                score=score, 
                catalyst=f"[{session_label}] " + trade.get('catalyst_summary', "Breakout"),
                entry_time=now,
                holding_type=holding_type,
                engine_type=engine_type,
                direction=direction,
                instrument_type=inst_type
            )
            
    else:
        print("No candidates passed the Math Engine. Skipping AI Debate.")
        rejected_trades.append({
            "ticker": "MARKET CHOP (ALL STOCKS)",
            "score": 0,
            "reason": "Failed PIM Math Engine Quantitative Filters (Volume Surge & Anti-FOMO extensions)"
        })

    # Send Email Alert
    try:
        from alerts.email_notifier import SovereignEmailer
        emailer = SovereignEmailer()
        emailer.send_scorecard(f"Live Hourly Execution ({now.strftime('%H:%M')})", final_approved_trades, rejected_trades)
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
