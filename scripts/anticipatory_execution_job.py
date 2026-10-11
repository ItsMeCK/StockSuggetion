import os
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
    
    # 1. Risk Management (Max 2 trades total across the entire system)
    active_positions = get_all_active_positions()
    current_active_count = len(active_positions)
    max_trades = 2
    
    if current_active_count >= max_trades:
        print(f"🚫 Risk Management: Already holding {current_active_count} trades. Max allowed is {max_trades}. Skipping execution.")
        return
        
    slots_available = max_trades - current_active_count
    print(f"🛡️ Slots available for execution: {slots_available}")
    
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
        ).alias("close_to_high_ratio")
    ])
    
    # Apply the 4-part Filter
    insider_breakouts = current_hour_df.filter(
        # 1. Closing near HOD (Top 15% of range, no supply)
        (pl.col("close_to_high_ratio") > 0.85) &
        # 2. Slow Grind (steady accumulation, not massive retail spike)
        (pl.col("vol_surge") > 1.2) & (pl.col("vol_surge") < 3.0) &
        # 3. Structural Integrity (Green candle, above 20 SMA)
        (pl.col("close") > pl.col("open")) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") > pl.col("prev_close")) &
        (pl.col("low") > pl.col("prev_low"))
    ).sort("vol_surge", descending=True).head(10)
    
    candidates = insider_breakouts.select("symbol").to_series().to_list()
    print(f"🎯 Math Engine found {len(candidates)} Insider Accumulation footprints: {candidates}")
    
    if len(candidates) == 0:
        print("No candidates passed the Math Engine. Skipping AI Debate.")
        send_email_report([], [{"ticker": "MARKET CHOP", "score": 0, "reason": "Failed Insider Footprint Math"}])
        return

    # 4. Google Grounding Live Veto
    final_approved_trades = []
    rejected_trades = []
    
    try:
        client = genai.Client()
    except Exception as e:
        print(f"❌ Failed to initialize Gemini Client: {e}")
        return

    for sym in candidates:
        if len(final_approved_trades) >= slots_available:
            print(f"🎯 Reached maximum available execution slots ({slots_available}). Stopping search.")
            break
            
        print(f"\n🌍 Grounding Search for {sym}...")
        prompt = f"The stock {sym} on the National Stock Exchange of India (NSE) is showing massive unexplained volume and price accumulation right now at 3:15 PM IST before the market closes. Search the live web for any breaking rumors, sector-wide tailwinds, regulatory updates, or unconfirmed chatter related to {sym} from today. Does this accumulation make logical sense? Respond with exactly one paragraph. Start your paragraph with the exact word 'APPROVED:' if you find a valid fundamental catalyst, sector tailwind, or rumor. Start with the exact word 'REJECTED:' if there is no news or logical reason."
        
        try:
            response = client.models.generate_content(
                model='gemini-3.6-flash',
                contents=prompt,
                config=types.GenerateContentConfig(
                    tools=[{"google_search": {}}],
                )
            )
            verdict_text = response.text.strip()
            print(f"Grounding Response: {verdict_text}")
            
            if verdict_text.startswith("APPROVED:"):
                catalyst = verdict_text.replace("APPROVED:", "").strip()
                score = 95.0
                
                # Execute Trade
                execute_trade(symbol=sym, score=score, catalyst=catalyst, entry_time=datetime.now())
                
                final_approved_trades.append({
                    "ticker": sym,
                    "score": score,
                    "passed": [catalyst]
                })
                print(f"✅ APPROVED: {sym}")
            else:
                reason = verdict_text.replace("REJECTED:", "").strip() if verdict_text.startswith("REJECTED:") else "Failed formatting / No news found."
                rejected_trades.append({
                    "ticker": sym,
                    "score": 0,
                    "reason": reason
                })
                print(f"🛑 REJECTED: {sym} (No Grounding Confirmation)")
                
        except Exception as e:
            print(f"❌ Gemini Grounding failed for {sym}: {e}")
            rejected_trades.append({
                "ticker": sym,
                "score": 0,
                "reason": f"API Error: {e}"
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
