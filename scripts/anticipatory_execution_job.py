import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import polars as pl
from datetime import datetime
import pytz
from google import genai
from google.genai import types

from core.live_trading import execute_trade
from core.db_manager import get_all_active_positions
from alerts.email_notifier import SovereignEmailer
from pipeline.intraday_ingestion import IntradayIngestionEngine

def run_anticipatory_execution():
    print(f"\n==================================================")
    print(f"🕒 RUNNING 3:15 PM INSIDER SNIPER AT: {datetime.now()}")
    print(f"==================================================")
    
    # 1. Clean up any expired daytime intraday trades (Type A/B)
    try:
        active_positions = get_all_active_positions()
        from scripts.btst_liquidator import is_btst_position
        from core.db_manager import close_position
        
        for pos in active_positions:
            if not is_btst_position(pos):
                print(f"🧹 Closing expired intraday position at 15:15: {pos.get('option_symbol', pos.get('symbol'))}")
                try:
                    close_position(pos, pos.get("entry_premium", 0.0), 0.0, "EOD_INTRADAY_EXPIRY")
                except Exception as e:
                    print(f"Warning closing expired position: {e}")
    except Exception as e:
        print(f"Warning during position cleanup: {e}")
    
    # 2. Ingest Latest Data (Crucial at 3:15 PM)
    print("📥 Triggering live data ingestion to fetch the 14:15 - 15:15 candle...")
    ingestion_engine = IntradayIngestionEngine()
    ingestion_engine.fetch_data()
    
    # 3. Load latest hourly market data
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"❌ Error: {parquet_path} not found.")
        return
        
    print("📥 Fetching fresh intraday hourly candles...")
    df = pl.read_parquet(parquet_path)
    
    # Calculate rolling indicators over the entire dataset first
    df = df.sort(["symbol", "time"])
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20"),
        pl.col("close").shift(1).over("symbol").alias("prev_close"),
        pl.col("low").shift(1).over("symbol").alias("prev_low")
    ])

    # Wolf ATR True Range & Volatility Metrics
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
    
    # Isolate the latest candle
    latest_time = df.select(pl.col("time").max()).item()
    print(f"Targeting Completed Candle: {latest_time} UTC")
    
    current_hour_df = df.filter(pl.col("time") == latest_time).sort("symbol")
    
    # 3. Calculate the Insider Footprint Math Engine
    current_hour_df = current_hour_df.with_columns([
        (pl.col("volume") / pl.col("vol_sma_20")).alias("vol_surge"),
        # Protect against division by zero if high == low
        (
            pl.when(pl.col("high") != pl.col("low"))
            .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
            .otherwise(0.0)
        ).alias("close_to_high_ratio"),
        (
            pl.when(pl.col("atr_14") > 0.0)
            .then((pl.col("close") - pl.col("sma_20")) / pl.col("atr_14"))
            .otherwise(0.0)
        ).alias("atr_dist_sma20")
    ])
    
    # Apply the 4-part Filter (and exclude low-beta defensives)
    from core.watchlist_manager import DEFAULT_DEFENSIVE_BLACKLIST

    # Wolf Breakout Pattern: Clean top 15% close OR VCP Base Expansion (vol_surge > 2.0 with close_to_high > 0.65)
    valid_breakout_pattern = (
        (pl.col("close_to_high_ratio") > 0.85) |
        ((pl.col("vol_surge") > 2.0) & (pl.col("close_to_high_ratio") > 0.65))
    )

    insider_breakouts = current_hour_df.filter(
        (~pl.col("symbol").is_in(list(DEFAULT_DEFENSIVE_BLACKLIST))) &
        # 1. Closing near HOD or VCP Base Expansion
        valid_breakout_pattern &
        # 2. Volume expansion (at least 1.2x, no upper cap for 3 PM run)
        (pl.col("vol_surge") > 1.2) &
        # 3. Structural Integrity (Green candle, above 20 SMA)
        (pl.col("close") > pl.col("open")) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") > pl.col("prev_close")) &
        (pl.col("low") > pl.col("prev_low")) &
        # 4. Wolf Anti-Trap Mean-Reversion Gate: Max 4.0x ATR distance above 20 SMA (prevents buying parabolic exhaustion in 99th percentile)
        (pl.col("atr_dist_sma20") <= 4.0)
    ).sort("vol_surge", descending=True).head(5)
    
    candidates = insider_breakouts.select("symbol").to_series().to_list()
    print(f"🎯 Math Engine found {len(candidates)} Insider Accumulation footprints: {candidates}")
    
    if len(candidates) == 0:
        print("No candidates passed the Math Engine. Skipping AI Debate.")
        send_email_report([], [{"ticker": "MARKET CHOP", "score": 0, "reason": "Failed Insider Footprint Math"}])
        return

    # 4. Hardened 3D Grounding Intelligence Scout (Anti-Sycophancy & Deterministic Veto)
    final_approved_trades = []
    rejected_trades = []
    
    try:
        from agents.grounding_intelligence import HardenedGroundingAgent
        grounding_agent = HardenedGroundingAgent()
    except Exception as e:
        print(f"❌ Failed to initialize HardenedGroundingAgent: {e}")
        return

    for sym in candidates:
        # Wolf Anti-Duplicate Chase: If symbol already hit a profitable daytime target today, skip duplicate BTST entry
        try:
            from core.db_manager import get_db_connection
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                SELECT COUNT(*) FROM trades 
                WHERE symbol = %s 
                  AND entry_time::date = CURRENT_DATE 
                  AND holding_type IN ('TYPE_A_SCALP', 'HOURLY / BTST')
                  AND pnl_pct > 3.0
            """, (sym,))
            row = cur.fetchone()
            already_won_today = (row[0] > 0) if row else False
            cur.close()
            conn.close()
            if already_won_today:
                print(f"🛡️ WOLF ANTI-CHASE: {sym} already yielded a profitable trade today. Skipping duplicate late-day BTST entry to prevent top-ticking.")
                rejected_trades.append({
                    "ticker": sym,
                    "score": 0,
                    "reason": "WOLF ANTI-CHASE: Duplicate same-day entry blocked (already booked profits earlier today)."
                })
                continue
        except Exception as e:
            print(f"Warning in anti-chase check: {e}")

        print(f"\n🌍 Running Hardened 3D Grounding Evaluation for {sym}...")
        try:
            approved, score, rationale, audit_data = grounding_agent.evaluate_symbol(sym)
            print(f"Grounding Verdict for {sym}: {rationale}")
            if approved:
                # Execute Trade as TYPE_C_BTST (Engine 2 BTST)
                execute_trade(symbol=sym, score=score, catalyst=rationale, entry_time=datetime.now(), holding_type="TYPE_C_BTST", engine_type="ENGINE_2_BTST")
                
                final_approved_trades.append({
                    "ticker": sym,
                    "score": score,
                    "passed": [rationale]
                })
                print(f"✅ APPROVED & EXECUTED: {sym} (Score: {score:.1f})")
            else:
                rejected_trades.append({
                    "ticker": sym,
                    "score": score,
                    "reason": rationale
                })
                print(f"🛑 REJECTED: {sym} -> {rationale}")
                
        except Exception as e:
            print(f"❌ Grounding evaluation failed for {sym}: {e}")
            rejected_trades.append({
                "ticker": sym,
                "score": 0,
                "reason": f"Evaluation Error: {e}"
            })

    # 5. Send Email Alert
    send_email_report(final_approved_trades, rejected_trades)

def send_email_report(approved, rejected):
    try:
        emailer = SovereignEmailer()
        now_str = datetime.now().strftime('%H:%M')
        emailer.send_scorecard(f"3:15 PM Insider Sniper Execution ({now_str})", approved, rejected)
        print(f"📧 Sent 3:15 PM Execution email with {len(approved)} approved trades!")
    except Exception as e:
        print(f"❌ Failed to send email alert: {e}")

if __name__ == "__main__":
    run_anticipatory_execution()
