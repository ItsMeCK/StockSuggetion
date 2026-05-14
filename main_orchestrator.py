import argparse
import logging
from pipeline.screener import SovereignScreener
from agents.librarian_agent import SovereignLibrarian
from alerts.email_notifier import SovereignEmailer
from execution.order_manager import SovereignExecutionEngine
import polars as pl

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_sovereign_session(mode: str):
    screener = SovereignScreener()
    librarian = SovereignLibrarian()
    emailer = SovereignEmailer()
    execution_engine = SovereignExecutionEngine()
    
    logging.info(f"🚀 Starting Sovereign {mode.upper()} Session...")
    
    # 0. The 3:15 PM Squash (Incubator only)
    if mode == 'incubator':
        execution_engine.squash_all_positions()
    
    # 1. Fetch historical base data
    raw_df = screener.fetch_market_data()
    
    # 1.5 Fetch & Inject LIVE Intraday Snapshot
    import os
    from kiteconnect import KiteConnect
    from datetime import datetime, timezone
    
    try:
        api_key = os.getenv("KITE_API_KEY")
        access_token = os.getenv("KITE_ACCESS_TOKEN").strip("'")
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        
        unique_symbols = raw_df["symbol"].unique().to_list()
        live_rows = []
        today_dt = datetime.now(timezone.utc)
        
        logging.info("Fetching live intraday quotes from Kite...")
        # Kite API allows max 500 instruments per quote request
        for i in range(0, len(unique_symbols), 400):
            chunk = unique_symbols[i:i+400]
            query_list = [f"NSE:{sym}" for sym in chunk]
            try:
                quotes = kite.quote(query_list)
                for sym in chunk:
                    q = quotes.get(f"NSE:{sym}")
                    if q and q.get("last_price"):
                        live_rows.append({
                            "time": today_dt,
                            "symbol": sym,
                            "open": float(q["ohlc"]["open"]),
                            "high": float(q["ohlc"]["high"]),
                            "low": float(q["ohlc"]["low"]),
                            "close": float(q["last_price"]),
                            "volume": float(q["volume"])
                        })
            except Exception as e:
                logging.error(f"Failed quote chunk: {e}")
                
        if live_rows:
            live_df = pl.DataFrame(live_rows)
            # Cast strictly to match the raw_df schema to prevent concatenation errors
            live_df = live_df.cast({
                "time": raw_df.schema["time"],
                "symbol": pl.Utf8,
                "open": pl.Float64,
                "high": pl.Float64,
                "low": pl.Float64,
                "close": pl.Float64,
                "volume": pl.Float64
            })
            raw_df = pl.concat([raw_df, live_df], how="vertical_relaxed")
            logging.info(f"Successfully injected {len(live_rows)} live intraday candles into the timeline.")
    except Exception as e:
        logging.error(f"Live injection failed. Defaulting to pure historical DB data: {e}")
        
    # 2. Calculate Stage 2 Filters
    full_df = screener.apply_stage_2_filter(raw_df)
    
    # Tag Stage 2 for the Librarian
    stage_2_df = full_df.filter(
        (pl.col("close") > pl.col("sma_50")) &
        ((pl.col("sma_50") >= pl.col("sma_200")) | (pl.col("volume") > 2.0 * pl.col("vol_avg_20"))) &
        (pl.col("sma_50_slope_10d") > -50) &
        (pl.col("sma_10") > pl.col("sma_20"))
    )
    s2_symbols = stage_2_df["symbol"].unique().to_list()
    full_df = full_df.with_columns(pl.col("symbol").is_in(s2_symbols).alias("is_stage_2"))
    
    # 2. Audit - Enforce STRICT uniqueness per symbol
    latest_df = full_df.sort("time").unique(subset=["symbol"], keep="last")
    potential = latest_df.filter(pl.col("extension_pct") <= 25.0)['symbol'].to_list()
    signals = []
    
    for sym in potential:
        rows = latest_df.filter(pl.col("symbol") == sym).to_dicts()
        if not rows: continue
        row = rows[0]
        audit = librarian.audit_setup(sym, row)
        if audit['status'] == 'SIGNALED' or (mode == 'incubator' and audit['score'] >= 60):
            signals.append(audit)
            
    # 3. Alert
    subject = f"{mode.upper()} Report"
    emailer.send_scorecard(subject, signals)
    
    # 4. Execute Live Orders
    execution_engine.buy_top_candidates(signals)
    
    logging.info(f"✅ {mode.upper()} Session Complete. Signals sent and trades routed.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=['regime', 'strike', 'incubator'], required=True)
    args = parser.parse_args()
    run_sovereign_session(args.mode)
