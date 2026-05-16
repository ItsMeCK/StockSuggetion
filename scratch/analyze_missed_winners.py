import os
import logging
import pandas as pd
import polars as pl
from pipeline.screener import SovereignScreener
from agents.librarian_agent import SovereignLibrarian
from datetime import datetime, timedelta, timezone

logging.basicConfig(level=logging.ERROR)

screener = SovereignScreener()
librarian = SovereignLibrarian()

print("Fetching and computing full market data features...")
df = screener.fetch_market_data()
full_df = screener.apply_stage_2_filter(df)

# Calculate next day's return to identify breakouts
# Shift(-1) brings tomorrow's data to today's row
full_df = full_df.with_columns([
    (((pl.col('close').shift(-1).over('symbol')) - pl.col('close')) / pl.col('close') * 100).alias('next_day_return')
])

# Filter for the last 7 trading days
today = datetime.now(timezone.utc).date()
seven_days_ago = today - timedelta(days=10) # 10 days to account for weekends

recent_df = full_df.filter(pl.col('time').dt.date() >= seven_days_ago)
# Remove the very last row for each symbol since next_day_return will be null
recent_df = recent_df.drop_nulls(subset=['next_day_return', 'roc_10', 'sma_50'])

# Find setups where tomorrow's return was > 5.0%
breakouts = recent_df.filter(pl.col('next_day_return') > 5.0)

print(f"\nFound {len(breakouts)} instances in the last 7 days where a stock jumped >5% the next day.")

results = breakouts.to_dicts()

reasons_missed = {}
total_audited = 0
passed_but_low_score = 0

for row in results:
    symbol = row['symbol']
    date = row['time'].date()
    next_return = row['next_day_return']
    
    # Audit today's setup (to see why we didn't buy it at 3:15 PM before the breakout)
    audit = librarian.audit_setup(symbol, row)
    
    total_audited += 1
    
    status = audit['status']
    if status == 'VETOED':
        reason = audit.get('reason', 'UNKNOWN_VETO')
        reasons_missed[reason] = reasons_missed.get(reason, 0) + 1
    elif status == 'REJECTED':
        failed_checks = audit.get('failed', [])
        for f in failed_checks:
            reasons_missed[f] = reasons_missed.get(f, 0) + 1
    elif status == 'SIGNALED':
        passed_but_low_score += 1
        # It passed, but score might have been too low (e.g. 80) to make the Top 3
        # We need a score of like 95+ to guarantee being in Top 3.
        reasons_missed[f"PASSED_BUT_SCORE_{audit['score']}"] = reasons_missed.get(f"PASSED_BUT_SCORE_{audit['score']}", 0) + 1

print("\n=== WHY WE MISSED THE 5%+ BREAKOUTS (EOD Incubator Audit) ===")
# Sort reasons by frequency
sorted_reasons = sorted(reasons_missed.items(), key=lambda x: x[1], reverse=True)
for reason, count in sorted_reasons:
    pct = (count / total_audited) * 100
    print(f"{count:3d} times ({pct:4.1f}%): {reason}")

print("\n=== TOP 5 MISSED MONSTER BREAKOUTS AND THEIR AUDIT ===")
top_monsters = sorted(results, key=lambda x: x['next_day_return'], reverse=True)[:5]
for row in top_monsters:
    audit = librarian.audit_setup(row['symbol'], row)
    print(f"\n{row['symbol']} on {row['time'].date()}")
    print(f"   Next Day Jump: +{row['next_day_return']:.1f}%")
    print(f"   Our Score: {audit['score']} | Status: {audit['status']}")
    if audit['status'] != 'SIGNALED':
        if 'reason' in audit:
            print(f"   Veto: {audit['reason']}")
        elif 'failed' in audit:
            print(f"   Failed: {audit['failed']}")
    print(f"   Metrics: ROC10={row.get('roc_10',0):.1f}, ROC20={row.get('roc_20',0):.1f}, Ext={row.get('extension_pct',0):.1f}%")
