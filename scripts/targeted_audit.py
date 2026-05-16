import os
import logging
import psycopg2
import polars as pl
from datetime import datetime
from agents.librarian_agent import SovereignLibrarian
from pipeline.screener import SovereignScreener

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

TARGETS = ["HIRECT", "TRITURBINE", "ARIHANTSUP", "APEX", "RISHABH", "MINDTECK", "AJOONI", "SREEL", "DAVANGERE", "ZYDUSLIFE", "MFSL"]

def run_targeted_audit():
    date = "2026-05-14"
    screener = SovereignScreener()
    librarian = SovereignLibrarian()
    
    # Fetch data up to May 14
    df = screener.fetch_market_data()
    from datetime import timezone, timedelta
    target_dt = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc) + timedelta(days=1)
    df = df.filter(pl.col("time") < target_dt)
    
    # Calculate metrics
    df = screener.apply_stage_2_filter(df)
    latest_df = df.group_by("symbol").tail(1)
    
    print("\n" + "="*80)
    print(f"TARGETED AUDIT: {date} (WHY DID WE BUY THESE?)")
    print("="*80)
    print(f"{'Symbol':<12} | {'Score':<6} | {'Status':<10} | {'Reason/Veto'}")
    print("-" * 80)

    for sym in TARGETS:
        row = latest_df.filter(pl.col("symbol") == sym).to_dicts()
        if not row:
            print(f"{sym:<12} | {'N/A':<6} | {'MISSING':<10} | No price data for this date.")
            continue
        
        # Run Librarian Audit
        audit = librarian.audit_setup(sym, row[0])
        
        reason = audit.get("reason", "N/A")
        if audit['status'] == 'SIGNALED':
            reason = f"Passed: {', '.join(audit['passed'][:3])}"
            
        print(f"{sym:<12} | {audit['score']:<6.1f} | {audit['status']:<10} | {reason}")

if __name__ == "__main__":
    run_targeted_audit()
