import os
import json
import logging
import psycopg2
import pandas as pd
import polars as pl
from datetime import datetime, timedelta, timezone
from pipeline.screener import SovereignScreener
from agents.librarian_agent import SovereignLibrarian
from scripts.weekly_auditor import SovereignAuditor
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

def run_marathon():
    screener = SovereignScreener()
    librarian = SovereignLibrarian()
    auditor = SovereignAuditor()
    
    start_date = datetime.now(timezone.utc).date() - timedelta(days=90)
    end_date = datetime.now(timezone.utc).date()
    
    logging.info(f"🏃 Starting 3-Month Marathon from {start_date} to {end_date}...")
    
    # Load initial config
    with open("config/strategy_weights.json", "r") as f:
        config = json.load(f)
    
    # 1. Fetch full historical dataset
    df = screener.fetch_market_data()
    full_df = screener.apply_stage_2_filter(df)
    
    # Check what days are already in the ledger
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT time::date FROM decision_ledger WHERE mode = 'back_audit'")
    processed_days = {row[0] for row in cur.fetchall()}
    cur.close()
    conn.close()
    
    # Get all trading days in the range
    all_trading_days = sorted(full_df.filter((pl.col('time').dt.date() >= start_date) & (pl.col('time').dt.date() <= end_date))['time'].unique().to_list())
    
    # Filter out already processed days
    trading_days = [dt for dt in all_trading_days if dt.date() not in processed_days]
    
    logging.info(f"⏭️ Skipping {len(all_trading_days) - len(trading_days)} already processed days. Resuming with {len(trading_days)} days remaining.")
    
    day_count = len(all_trading_days) - len(trading_days)
    for current_dt in trading_days:
        day_count += 1
        current_date = current_dt.date()
        logging.info(f"📅 Simulating Day {day_count}: {current_date}")
        
        # Get data available UP TO this day
        daily_snapshot = full_df.filter(pl.col('time').dt.date() == current_date)
        
        # Capture Decisions for the day
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "quant"),
            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
            database=os.getenv("POSTGRES_DB", "market_data")
        )
        cur = conn.cursor()
        
        for row in daily_snapshot.to_dicts():
            symbol = row['symbol']
            
            # Calculate 10-Day Avg Volume for RVOL
            hist_vol = full_df.filter(
                (pl.col('symbol') == symbol) & 
                (pl.col('time').dt.date() < current_date)
            ).tail(10)['volume'].mean()
            
            row['volume_ratio'] = row['volume'] / hist_vol if hist_vol and hist_vol > 0 else 1.0
            
            audit = librarian.audit_setup(symbol, row)
            
            # CONSENSUS CHECK: Must have support from multiple 'categories' of signals
            # Since the Librarian currently holds most rules, we check if the score 
            # is derived from a diverse set of passed checks.
            passed_checks = audit.get('passed', [])
            score = audit.get('score', 0)
            
            # Elite Filter: Score must be high AND diverse
            diversity_score = len(set([c.split('_')[0] for c in passed_checks])) # Count unique prefixes like PRING, INTRADAY, etc.
            
            final_status = audit['status']
            if score < config["thresholds"].get("SIGNAL_PASS_SCORE", 110):
                final_status = "WATCHLIST" 
            
            # Log to Ledger
            cur.execute(
                "INSERT INTO decision_ledger (symbol, mode, score, status, agent_opinions, price_at_scan, time) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (
                    audit['ticker'], 'back_audit', score, final_status,
                    json.dumps({"passed": audit.get('passed', []), "failed": audit.get('failed', []), "diversity": diversity_score}),
                    audit.get('price', 0),
                    current_dt
                )
            )
        conn.commit()
        cur.close()
        conn.close()
        
        # Every 7 days (Friday), run the Post-Mortem and Tuning
        if day_count % 5 == 0:
            logging.info(f"🔬 Weekend reached in simulation. Running Post-Mortem for {current_date}...")
            reflection = auditor.run_post_mortem(target_date=current_date)
            
            # Apply "Smart Tuning"
            suggested_changes = reflection.get("suggested_weight_changes", {})
            if suggested_changes:
                logging.info(f"🧠 AI suggesting {len(suggested_changes)} weight adjustments...")
                with open("config/strategy_weights.json", "r") as f:
                    config = json.load(f)
                
                for rule, new_val in suggested_changes.items():
                    if rule in config["weights"]:
                        try:
                            # Handle case where AI might return a string like "increase to 20"
                            if isinstance(new_val, str):
                                import re
                                digits = re.findall(r'\d+', new_val)
                                if digits:
                                    config["weights"][rule] = int(digits[0])
                            else:
                                config["weights"][rule] = int(new_val)
                        except Exception:
                            logging.warning(f"Could not parse new weight for {rule}: {new_val}")
                
                # Apply Threshold Changes
                new_thresholds = reflection.get("new_thresholds", {})
                if new_thresholds:
                    for key, val in new_thresholds.items():
                        if key in config["thresholds"]:
                            try:
                                config["thresholds"][key] = float(val)
                            except: pass
                
                with open("config/strategy_weights.json", "w") as f:
                    json.dump(config, f, indent=4)
                
                # RE-LOAD Librarian with new weights
                librarian = SovereignLibrarian()
                logging.info(f"💾 Evolution Journal updated. (Score Threshold: {config['thresholds'].get('SIGNAL_PASS_SCORE')})")
                import time
                time.sleep(2)
            
            day_count += 1

    logging.info("🏁 Marathon Complete. The engine has been trained on 3 months of market data.")

if __name__ == "__main__":
    run_marathon()
