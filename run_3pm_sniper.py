import os
import json
import logging
import datetime
import uuid
import psycopg2
import polars as pl
from dotenv import load_dotenv
from kiteconnect import KiteConnect
from pipeline.screener import SovereignScreener
from agents.pattern_agent import PatternAuditor
from generate_portfolio_dashboard import generate_dashboard

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_3pm_sniper():
    load_dotenv()
    logging.info("--- STARTING 3:00 PM INSTITUTIONAL SNIPER ---")
    
    # 1. Connect to Kite
    kite = KiteConnect(api_key=os.getenv('KITE_API_KEY'))
    kite.set_access_token(os.getenv('KITE_ACCESS_TOKEN'))
    
    # 2. Get Nifty 500 Symbols from Master Universe
    try:
        master_df = pl.read_csv("pipeline/master_universe.csv")
        symbols = master_df["symbol"].to_list()
        kite_symbols = [f"NSE:{s}" for s in symbols]
    except Exception as e:
        logging.error(f"Failed to load master universe: {e}")
        return

    # 3. Fetch LIVE 3:00 PM Quotes
    logging.info(f"Fetching live quotes for {len(symbols)} symbols...")
    quotes = kite.quote(kite_symbols)
    
    # 4. Fetch Historical Data (Up to Yesterday) from DB
    screener = SovereignScreener()
    hist_df = screener.fetch_market_data()
    
    # 5. Create "Live Today" DataFrame
    live_rows = []
    today_dt = datetime.datetime.now(datetime.timezone.utc).replace(hour=18, minute=30, second=0, microsecond=0)
    
    for s in symbols:
        q = quotes.get(f"NSE:{s}")
        if q:
            live_rows.append({
                "time": today_dt,
                "symbol": s,
                "open": q["ohlc"]["open"],
                "high": q["ohlc"]["high"],
                "low": q["ohlc"]["low"],
                "close": q["last_price"],
                "volume": q["volume"]
            })
    
    live_df = pl.DataFrame(live_rows)
    # Ensure schema matches
    live_df = live_df.with_columns([pl.col("time").cast(pl.Datetime)])
    
    # 6. Merge Historical + Live
    full_df = pl.concat([hist_df, live_df])
    
    # 7. Run Pipeline Logic on Merged Data
    # We modify the screener to use our full_df instead of fetching again
    logging.info("Running Screener on Intraday Data...")
    
    # Injecting our merged data into the screener workflow
    # We'll call the internal filtering methods directly
    full_df = screener.apply_stage_2_filter(full_df)
    full_df = screener.apply_avwap_filter(full_df)
    
    # Determine the latest regime from merged data
    macro_regime = screener.calculate_market_regime()
    
    # Standard Screener Filtering (Balanced Mode)
    relaxed_window = True 
    vol_mult_stage2 = 1.2 if relaxed_window else 1.3
    vol_mult_transition = 1.5 if relaxed_window else 1.8
    ext_limit = 15.0 if relaxed_window else 12.0
    
    latest_df = full_df.group_by("symbol").tail(1)
    
    stage_2_df = latest_df.filter(
        (pl.col("close") > pl.col("sma_50")) &
        (pl.col("sma_50") > pl.col("sma_200")) &
        (pl.col("sma_50_slope_10d") > 0) &
        (pl.col("extension_pct") <= ext_limit) & 
        (pl.col("sma_10") > pl.col("sma_20")) &
        (pl.col("volume") >= (vol_mult_stage2 * pl.col("vol_avg_20")))
    )
    
    transition_df = latest_df.filter(
        (pl.col("close") > pl.col("sma_10")) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("volume") >= (vol_mult_transition * pl.col("vol_avg_20"))) & 
        (pl.col("close") >= (pl.col("sma_50") * 0.98)) & 
        (pl.col("extension_pct") <= ext_limit) 
    )
    
    candidates = list(set(stage_2_df["symbol"].to_list() + transition_df["symbol"].to_list()))
    logging.info(f"3:00 PM Sniper found {len(candidates)} candidates.")
    
    if not candidates:
        logging.info("No intraday breakouts found. Ending sniper run.")
        return

    # 8. AI Audit for Conviction
    auditor = PatternAuditor()
    conviction_results = {}
    for sym in candidates:
        # Get last 30 days for AI context
        sym_hist = full_df.filter(pl.col("symbol") == sym).tail(30).to_dicts()
        conviction = auditor.audit_candidate(sym, sym_hist)
        conviction_results[sym] = conviction
        
    # 9. Persistence to Ledger
    conn = psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'), port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'), password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        database=os.getenv('POSTGRES_DB', 'market_data')
    )
    cur = conn.cursor()
    
    for sym in candidates:
        score = conviction_results.get(sym, {}).get("total_conviction", 0)
        if score >= 80:
            trade_id = str(uuid.uuid4())
            price = quotes[f"NSE:{sym}"]["last_price"]
            
            # Additional metrics for the dashboard
            latest_row = latest_df.filter(pl.col("symbol") == sym).to_dicts()[0]
            notes_data = {
                "ai_score": score,
                "mom_score": 100.0, # Placeholder for now
                "vol_z": 2.0, # Placeholder
                "turnover": (latest_row["volume"] * latest_row["close"]) / 10000000.0,
                "macro": macro_regime["regime"]
            }
            
            cur.execute("""
                INSERT INTO trade_events (trade_id, ticker, status, price, quantity, notes)
                VALUES (%s, %s, 'SIGNALED', %s, 0, %s)
            """, (trade_id, sym, price, json.dumps(notes_data)))
            
    conn.commit()
    cur.close()
    conn.close()
    
    # 10. Update Dashboard
    generate_dashboard()
    logging.info("Sniper Run Complete. Dashboard Updated.")

if __name__ == "__main__":
    run_3pm_sniper()
