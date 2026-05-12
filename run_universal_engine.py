import os
import json
import logging
import datetime
import uuid
import psycopg2
import argparse
import polars as pl
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from pipeline.screener import SovereignScreener
from generate_portfolio_dashboard import generate_dashboard

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        database=os.getenv('POSTGRES_DB', 'market_data')
    )

def run_universal_engine(mode='live', target_date=None):
    load_dotenv()
    logging.info(f"--- STARTING UNIVERSAL SOVEREIGN ENGINE [MODE: {mode.upper()}] ---")
    
    kite = KiteConnect(api_key=os.getenv('KITE_API_KEY'))
    kite.set_access_token(os.getenv('KITE_ACCESS_TOKEN'))
    
    screener = SovereignScreener()
    
    # 1. Prepare Dataset
    if mode == 'live':
        logging.info("Building Intraday Hybrid Dataset...")
        try:
            master_df = pl.read_csv("pipeline/master_universe.csv")
            symbols = master_df["Symbol"].to_list()
            kite_symbols = [f"NSE:{s}" for s in symbols]
            
            # Fetch live quotes
            quotes = kite.quote(kite_symbols)
            hist_df = screener.fetch_market_data()
            
            # Create "Live" row for today
            live_rows = []
            today_dt = datetime.datetime.now().replace(hour=18, minute=30, second=0, microsecond=0)
            for s in symbols:
                q = quotes.get(f"NSE:{s}")
                if q:
                    live_rows.append({
                        "symbol": s, "open": q["ohlc"]["open"],
                        "high": q["ohlc"]["high"], "low": q["ohlc"]["low"],
                        "close": q["last_price"], "volume": q["volume"]
                    })
            
            live_df = pl.DataFrame(live_rows).with_columns([
                pl.lit(today_dt).alias("time").cast(pl.Datetime)
            ]).select(hist_df.columns) # Force exact same column order
            
            hist_df = hist_df.with_columns([pl.col("time").cast(pl.Datetime)])
            full_df = pl.concat([hist_df, live_df], how="vertical")
        except Exception as e:
            logging.error(f"Live injection failed: {e}")
            import traceback
            logging.error(traceback.format_exc())
            return
    else:
        logging.info(f"Using EOD Data from Database (Target: {target_date or 'Latest'})...")
        full_df = screener.fetch_market_data()
        if target_date:
            from datetime import datetime as dt_class, timezone, timedelta
            target_dt = dt_class.strptime(target_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
            full_df = full_df.filter(pl.col("time") < target_dt)

    # 2. Run Screener Logic
    full_df = screener.apply_stage_2_filter(full_df)
    full_df = screener.apply_avwap_filter(full_df)
    macro_regime = screener.calculate_market_regime(target_date)
    
    # Deterministic Filtering
    latest_df = full_df.group_by("symbol").tail(1)
    relaxed_window = True # Enabled as per user preference for leaders
    vol_mult_stage2 = 1.2 if relaxed_window else 1.3
    vol_mult_transition = 1.5 if relaxed_window else 1.8
    ext_limit = 15.0 if relaxed_window else 12.0
    
    candidates_df = latest_df.filter(
        ((pl.col("close") > pl.col("sma_50")) &
         (pl.col("sma_50") > pl.col("sma_200")) &
         (pl.col("sma_50_slope_10d") > 0) &
         (pl.col("extension_pct") <= ext_limit) & 
         (pl.col("sma_10") > pl.col("sma_20")) &
         (pl.col("volume") >= (vol_mult_stage2 * pl.col("vol_avg_20")))) |
        ((pl.col("close") > pl.col("sma_10")) &
         (pl.col("close") > pl.col("sma_20")) &
         (pl.col("volume") >= (vol_mult_transition * pl.col("vol_avg_20"))) & 
         (pl.col("close") >= (pl.col("sma_50") * 0.98)) & 
         (pl.col("extension_pct") <= ext_limit))
    )
    
    candidates = candidates_df["symbol"].unique().to_list()
    logging.info(f"Screener found {len(candidates)} candidates.")
    
    if not candidates:
        logging.info("No candidates found. Ending run.")
        return

    # 3. Budget-Safe AI Conviction Audit (Top 10 only)
    # Sort by Volume Surge (Volume / Avg Volume)
    candidates_df = candidates_df.with_columns([
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge")
    ]).sort("vol_surge", descending=True)
    
    top_candidates = candidates_df.head(10)["symbol"].to_list()
    logging.info(f"Budget-Safe Mode: Auditing Top {len(top_candidates)} candidates out of {len(candidates)}.")
    
    from agents.pattern_agent import VisionPatternAgent
    auditor = VisionPatternAgent()
    conviction_results = {}
    for sym in top_candidates:
        sym_hist = full_df.filter(pl.col("symbol") == sym).tail(30).to_dicts()
        conviction_results[sym] = auditor.analyze_chart(sym, "breakout", target_date)
        
    # 4. Ledger Persistence (With Overwrite Protection)
    conn = get_db_connection()
    cur = conn.cursor()
    
    # Clear old SIGNALED entries for today/target_date to avoid duplicates
    clear_date = target_date or datetime.datetime.now().strftime("%Y-%m-%d")
    cur.execute("DELETE FROM trade_events WHERE status = 'SIGNALED' AND system_time::date = %s", (clear_date,))
    
    for sym in top_candidates:
        score = conviction_results.get(sym, {}).get("vision_score", 0)
        if score >= 70:
            row = candidates_df.filter(pl.col("symbol") == sym).to_dicts()[0]
            price = row["close"]
            notes_data = {
                "ai_score": score, "mom_score": 100.0, "vol_z": 2.0,
                "turnover": (row["volume"] * row["close"]) / 10000000.0,
                "macro": macro_regime["regime"]
            }
            cur.execute("""
                INSERT INTO trade_events (trade_id, ticker, status, price, quantity, notes)
                VALUES (%s, %s, 'SIGNALED', %s, 0, %s)
            """, (str(uuid.uuid4()), sym, price, json.dumps(notes_data)))
            
    # 5. Incubator Logic (Watchlist for tomorrow)
    # Clear old INCUBATING entries for today
    cur.execute("DELETE FROM trade_events WHERE status = 'INCUBATING' AND system_time::date = %s", (clear_date,))

    # Find stocks with Stage 2 metrics but Volume Dry-up (< 0.9x avg)
    # We use the full Stage 2 universe, not just breakout candidates
    master_universe = pl.read_csv("pipeline/master_universe.csv")
    stage2_df = full_df.filter(pl.col("symbol").is_in(master_universe["Symbol"].to_list())).group_by("symbol").tail(1)
    
    incubator_df = stage2_df.filter(
        (pl.col("volume") < (0.8 * pl.col("vol_avg_20"))) & # Real dry-up
        (pl.col("extension_pct") < 3.0) & # Very tight
        (pl.col("close") > pl.col("sma_50")) # Must be in uptrend
    ).sort("extension_pct").head(15)
    
    for row in incubator_df.to_dicts():
        sym = row["symbol"]
        # Skip if already signaled as a breakout
        if sym in conviction_results and conviction_results[sym].get("vision_score", 0) >= 70:
            continue
            
        cur.execute("""
            INSERT INTO trade_events (trade_id, ticker, status, price, quantity, notes)
            VALUES (%s, %s, 'INCUBATING', %s, 0, %s)
        """, (str(uuid.uuid4()), sym, row["close"], json.dumps({
            "vol_surge": row["volume"] / row["vol_avg_20"],
            "extension": row["extension_pct"],
            "turnover": (row["volume"] * row["close"]) / 10000000.0,
            "regime": macro_regime["regime"]
        })))
        
    conn.commit()
    cur.close()
    conn.close()
    
    # 5. Dashboard Update
    generate_dashboard()
    logging.info(f"Run Complete. mode={mode}. Candidates={len(candidates)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Universal Sovereign Engine")
    parser.add_argument("--mode", choices=["live", "eod"], default="live", help="Operation mode")
    parser.add_argument("--date", help="Target date for EOD mode (YYYY-MM-DD)")
    args = parser.parse_args()
    
    run_universal_engine(mode=args.mode, target_date=args.date)
