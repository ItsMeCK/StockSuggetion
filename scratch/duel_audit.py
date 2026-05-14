import polars as pl
from pipeline.screener import SovereignScreener
from agents.librarian_agent import SovereignLibrarian
import logging

logging.basicConfig(level=logging.INFO)

def run_duel():
    screener = SovereignScreener()
    librarian = SovereignLibrarian()
    
    dates = ['2026-05-08', '2026-05-11', '2026-05-12']
    report = []

    for d in dates:
        print(f"\n--- 🏛️ DUELING ON {d} ---")
        
        # 1. Fetch Market Data
        raw_df = screener.fetch_market_data()
        
        # Calculate metrics on the FULL dataset so rolling windows work
        full_df = screener.apply_stage_2_filter(raw_df)
        
        # Now filter for the target date
        if d:
            # Apply IST offset (+5:30) before casting to date
            full_df = full_df.with_columns(
                (pl.col("time") + pl.duration(hours=5, minutes=30)).cast(pl.Date).alias("date")
            )
            full_df = full_df.filter(pl.col("date") == pl.lit(d).str.to_date())
        
        # Calculate is_stage_2 based on the new Pring/Shannon rules
        stage_2_df = full_df.filter(
            (pl.col("close") > pl.col("sma_50")) &
            ((pl.col("sma_50") >= pl.col("sma_200")) | (pl.col("volume") > 2.0 * pl.col("vol_avg_20"))) &
            (pl.col("sma_50_slope_10d") > -50) &
            (pl.col("sma_10") > pl.col("sma_20"))
        )
        s2_symbols = stage_2_df["symbol"].unique().to_list()
        full_df = full_df.with_columns(pl.col("symbol").is_in(s2_symbols).alias("is_stage_2"))

        # OLD LOGIC: Extension < 5%, Volume > 1.2x
        old_winners = full_df.filter(
            (pl.col("extension_pct") <= 5.0) & 
            (pl.col("volume") >= 1.2 * pl.col("vol_avg_20")) &
            (pl.col("is_stage_2")) # Old logic required stage 2
        )['symbol'].to_list()
        
        # NEW LOGIC: Use Librarian on all potential names
        potential_names = full_df.filter(
            (pl.col("extension_pct") <= 25.0) # Looser net to catch Champions
        )['symbol'].to_list()
        
        new_winners = []
        for sym in potential_names:
            row = full_df.filter(pl.col("symbol") == sym).to_dicts()[0]
            audit = librarian.audit_setup(sym, row)
            
            if sym == "MAPMYINDIA":
                print(f"   [DEBUG] MMI Audit: {audit}")
                
            if audit['status'] == "SIGNALED":
                new_winners.append(sym)

        print(f"   - Old Logic Signal Count: {len(old_winners)}")
        print(f"   - Librarian Signal Count: {len(new_winners)}")
        
        # Check for the Alpha Capture (The names we care about)
        alpha_targets = ['MAPMYINDIA', 'JAINREC', 'FSL', 'OIL', 'MEDANTA']
        caught_old = [s for s in alpha_targets if s in old_winners]
        caught_new = [s for s in alpha_targets if s in new_winners]
        
        print(f"   - Alpha Targets Caught (OLD): {caught_old}")
        print(f"   - Alpha Targets Caught (NEW): {caught_new}")
        
        report.append({
            "date": d,
            "old_count": len(old_winners),
            "new_count": len(new_winners),
            "alpha_improvement": len(caught_new) - len(caught_old)
        })

    print("\n--- 🏛️ FINAL DUEL VERDICT ---")
    for r in report:
        print(f"   {r['date']}: New Era caught {r['alpha_improvement']} more Alpha leaders.")

if __name__ == "__main__":
    run_duel()
