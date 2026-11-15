import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import polars as pl
from datetime import datetime
import pytz
from dotenv import load_dotenv

from core.live_trading import execute_trade, get_kite_data_client
from core.db_manager import get_all_active_positions, get_db_connection
from alerts.email_notifier import SovereignEmailer
from pipeline.intraday_ingestion import IntradayIngestionEngine
from core.watchlist_manager import DEFAULT_DEFENSIVE_BLACKLIST
from agents.grounding_intelligence import HardenedGroundingAgent

def evaluate_pe_macro_regime(df: pl.DataFrame, kite_data=None) -> tuple[bool, str]:
    """
    Evaluates whether the macro environment permits 3:15 PM PE (STBT) entries.
    Empirical Rule:
    - If Nifty 50 is in strong Bull Expansion (above 20 SMA & green day > +0.4%),
      buying Puts carries extreme overnight gap-up risk.
    - If Nifty 50 is in Neutral Chop, Bear Distribution, or Red Day, PE buying is permitted.
    """
    # 1. Try Kite live quote for NIFTY 50 if client is available
    if kite_data:
        try:
            today_str = datetime.now().strftime("%Y-%m-%d")
            hist = kite_data.historical_data(
                instrument_token=256265, # NIFTY 50
                from_date=f"{today_str} 09:15:00",
                to_date=f"{today_str} 15:30:00",
                interval="day"
            )
            if hist and len(hist) > 0:
                n_open = hist[0]["open"]
                n_close = hist[-1]["close"]
                day_ret = ((n_close - n_open) / n_open) * 100.0
                if day_ret > 0.40:
                    return False, f"NIFTY 50 in strong Bull Expansion (+{day_ret:.2f}%). Blocking PE to prevent overnight gap-up trap."
                return True, f"NIFTY 50 return {day_ret:.2f}% allows PE execution."
        except Exception as e:
            pass

    # 2. Market Breadth Fallback from hourly dataset
    try:
        latest_time = df.select(pl.col("time").max()).item()
        curr_bar = df.filter(pl.col("time") == latest_time)
        advances = curr_bar.filter(pl.col("close") > pl.col("open")).height
        total = curr_bar.height
        if total > 0:
            advance_pct = advances / total
            if advance_pct > 0.70:
                return False, f"Market Breadth is Euphoric ({advance_pct*100:.1f}% advancing). Blocking PE."
            return True, f"Market Breadth ({advance_pct*100:.1f}% advancing) allows PE."
    except Exception as e:
        pass

    return True, "Default Neutral Regime permits PE"

def run_anticipatory_pe_execution():
    """
    Engine 2 dedicated 3:15 PM PE (STBT - Put Option) Insider Sniper.
    Identifies high-conviction institutional distribution setups closing near Low of Day (LOD).
    Protected by:
    1. Macro Regime Alignment (Nifty Kill-Switch)
    2. Relative Weakness vs Sector/Market
    3. Downside Mean-Reversion Floor (atr_dist <= 4.0)
    4. Anti-Duplicate same-day win check
    """
    load_dotenv()
    print(f"\n==================================================")
    print(f"🕒 RUNNING 3:15 PM STBT (PE) INSIDER SNIPER AT: {datetime.now()}")
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

    # 2. Shared Data Check: Reuse data already downloaded by CE engine if fresh
    parquet_path = "data/intraday_ohlcv.parquet"
    data_already_fresh = False
    if os.path.exists(parquet_path):
        try:
            mtime = os.path.getmtime(parquet_path)
            age_seconds = datetime.now().timestamp() - mtime
            # If updated within last 20 minutes (e.g. by 15:15 CE engine), reuse directly
            if age_seconds < 1200:
                data_already_fresh = True
                print(f"⚡ SHARED DATA REUSE: {parquet_path} was updated {int(age_seconds)}s ago. Reusing shared Zerodha data without duplicate download.")
        except Exception as e:
            print(f"Warning checking parquet freshness: {e}")

    if not data_already_fresh:
        print("📥 Ingesting fresh intraday hourly candles from Zerodha...")
        try:
            ingestion_engine = IntradayIngestionEngine()
            ingestion_engine.fetch_data()
        except Exception as e:
            print(f"Warning during ingestion: {e}")

    # 3. Load latest hourly market data
    if not os.path.exists(parquet_path):
        print(f"❌ Error: {parquet_path} not found.")
        return

    df = pl.read_parquet(parquet_path)
    df = df.sort(["symbol", "time"])
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20"),
        pl.col("close").shift(1).over("symbol").alias("prev_close"),
        pl.col("high").shift(1).over("symbol").alias("prev_high"),
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

    latest_time = df.select(pl.col("time").max()).item()
    print(f"Targeting Completed Candle: {latest_time} UTC")
    current_hour_df = df.filter(pl.col("time") == latest_time).sort("symbol")

    # 4. Improvement 1: Macro Regime Alignment Gate
    kite_data = None
    try:
        kite_data = get_kite_data_client()
    except Exception:
        pass

    pe_allowed, regime_reason = evaluate_pe_macro_regime(df, kite_data)
    print(f"🌍 MACRO REGIME STATUS: {regime_reason}")
    if not pe_allowed:
        print(f"🔒 {regime_reason}. Skipping 3:15 PM PE execution.")
        send_email_report([], [{"ticker": "MACRO_VETO", "score": 0, "reason": regime_reason}])
        return

    # 5. Calculate the Distribution Footprint Math Engine
    current_hour_df = current_hour_df.with_columns([
        (pl.col("volume") / pl.col("vol_sma_20")).alias("vol_surge"),
        # Close to Low Ratio: Closing in bottom 15% of the candle
        (
            pl.when(pl.col("high") != pl.col("low"))
            .then((pl.col("high") - pl.col("close")) / (pl.col("high") - pl.col("low")))
            .otherwise(0.0)
        ).alias("close_to_low_ratio"),
        # Downside ATR distance: distance below SMA20 in terms of ATR
        (
            pl.when(pl.col("atr_14") > 0.0)
            .then((pl.col("sma_20") - pl.col("close")) / pl.col("atr_14"))
            .otherwise(0.0)
        ).alias("downside_atr_dist")
    ])

    # Wolf Breakdown Pattern: Closing in bottom 15% OR VCP Distribution base expansion
    valid_breakdown_pattern = (
        (pl.col("close_to_low_ratio") > 0.85) |
        ((pl.col("vol_surge") > 2.0) & (pl.col("close_to_low_ratio") > 0.65))
    )

    insider_breakdowns = current_hour_df.filter(
        (~pl.col("symbol").is_in(list(DEFAULT_DEFENSIVE_BLACKLIST))) &
        # 1. Closing near Low of Day or VCP Distribution
        valid_breakdown_pattern &
        # 2. Volume expansion (at least 1.2x)
        (pl.col("vol_surge") > 1.2) &
        # 3. Structural Breakdown (Red candle, below 20 SMA, breaking lower)
        (pl.col("close") < pl.col("open")) &
        (pl.col("close") < pl.col("sma_20")) &
        (pl.col("close") < pl.col("prev_close")) &
        (pl.col("high") < pl.col("prev_high")) &
        # 4. Improvement 3: Downside Parabolic Floor Gate (Max 4.0x ATR below SMA20)
        (pl.col("downside_atr_dist") <= 4.0)
    ).sort("vol_surge", descending=True).head(5)

    candidates = insider_breakdowns.select("symbol").to_series().to_list()
    print(f"🎯 Math Engine found {len(candidates)} Insider Distribution footprints: {candidates}")

    if len(candidates) == 0:
        print("No candidates passed the Math Engine. Skipping AI Debate.")
        send_email_report([], [{"ticker": "MARKET CHOP", "score": 0, "reason": "Failed Insider Distribution Math"}])
        return

    # 6. Hardened Grounding & Anti-Duplicate Evaluation
    final_approved_trades = []
    rejected_trades = []

    try:
        grounding_agent = HardenedGroundingAgent()
    except Exception as e:
        print(f"❌ Failed to initialize HardenedGroundingAgent: {e}")
        return

    for sym in candidates:
        # Wolf Anti-Duplicate Chase: If symbol already won today, do not chase late
        try:
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
                print(f"🛡️ WOLF ANTI-CHASE: {sym} already yielded a profitable trade today. Skipping duplicate late-day entry.")
                rejected_trades.append({
                    "ticker": sym,
                    "score": 0,
                    "reason": "WOLF ANTI-CHASE: Duplicate same-day entry blocked (already booked profits earlier today)."
                })
                continue
        except Exception as e:
            print(f"Warning in anti-chase check: {e}")

        print(f"\n🌍 Running Hardened Grounding Evaluation for {sym} (PE SHORT)...")
        try:
            approved, score, rationale, audit_data = grounding_agent.evaluate_symbol(sym)
            
            # Grounding Interpretation for PE:
            # Toxic catalysts (regulatory fines, governance issues, debt overhang) are BEARISH TAILWINDS for PE
            # If toxic was detected, or stealth accumulation was low, this strengthens the short thesis
            is_toxic = audit_data.get("parsed_dimensions", {}).get("toxic_catalyst", {}).get("detected", False)
            if is_toxic:
                score = max(score, 75.0)
                approved = True
                rationale = f"APPROVED (Forensic Bearish Catalyst): Toxic structural overhang detected - {rationale}"

            print(f"Grounding Verdict for {sym}: {rationale} (Approved: {approved}, Score: {score:.1f})")
            
            if approved or score >= 50.0:
                # Execute Trade as SHORT PE under TYPE_C_BTST (Engine 2 STBT)
                execute_trade(
                    symbol=sym, 
                    score=score, 
                    catalyst=rationale, 
                    entry_time=datetime.now(), 
                    holding_type="TYPE_C_BTST", 
                    engine_type="ENGINE_2_PE_STBT",
                    direction="SHORT",
                    instrument_type="PE"
                )
                
                final_approved_trades.append({
                    "ticker": sym,
                    "score": score,
                    "passed": [rationale]
                })
                print(f"✅ APPROVED & EXECUTED SHORT (PE): {sym} (Score: {score:.1f})")
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

    # 7. Send Email Alert
    send_email_report(final_approved_trades, rejected_trades)

def send_email_report(approved, rejected):
    try:
        emailer = SovereignEmailer()
        now_str = datetime.now().strftime('%H:%M')
        emailer.send_scorecard(f"3:15 PM STBT (PE) Insider Sniper Execution ({now_str})", approved, rejected)
        print(f"📧 Sent 3:15 PM STBT (PE) Execution email with {len(approved)} approved trades!")
    except Exception as e:
        print(f"❌ Failed to send email alert: {e}")

if __name__ == "__main__":
    run_anticipatory_pe_execution()
