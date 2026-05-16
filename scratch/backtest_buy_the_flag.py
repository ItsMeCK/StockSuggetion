import os
import logging
import pandas as pd
import polars as pl
from pipeline.screener import SovereignScreener
from agents.librarian_agent import SovereignLibrarian
from datetime import datetime, timedelta, timezone
import numpy as np

logging.basicConfig(level=logging.ERROR)

screener = SovereignScreener()

print("Fetching and computing full market data features...")
df = screener.fetch_market_data()
full_df = screener.apply_stage_2_filter(df)

# Calculate Max Gain over the NEXT 2 days
# We use forward rolling max
full_df = full_df.with_columns([
    (((pl.col('close').shift(-2).over('symbol')).max_horizontal(pl.col('close').shift(-1).over('symbol')) - pl.col('close')) / pl.col('close') * 100).alias('max_gain_2d')
])

# Filter for a historical test period (e.g. April 2026) to see forward results
# Let's just use the last 15 days of data we have, excluding the very last 2 days
today = datetime.now(timezone.utc).date()
test_period_start = today - timedelta(days=20)
test_period_end = today - timedelta(days=3)

test_df = full_df.filter((pl.col('time').dt.date() >= test_period_start) & (pl.col('time').dt.date() <= test_period_end))
test_df = test_df.drop_nulls(subset=['max_gain_2d', 'roc_10', 'roc_20'])

# STRATEGY A: CURRENT ACCELERATION (ROC10 > ROC20)
strat_a = test_df.filter(pl.col('roc_10') > pl.col('roc_20'))

# STRATEGY B: BUY THE FLAG (ROC20 > 15% AND -5% <= ROC10 <= 2%)
strat_b = test_df.filter((pl.col('roc_20') > 15.0) & (pl.col('roc_10') >= -5.0) & (pl.col('roc_10') <= 2.0))

def summarize(name, df):
    if df.empty:
        print(f"\n{name}: No trades found.")
        return
    
    total = len(df)
    win_3pct = len(df.filter(pl.col('max_gain_2d') >= 3.0))
    avg_max_gain = df['max_gain_2d'].mean()
    
    print(f"\n=== {name} ===")
    print(f"Total Trades: {total}")
    print(f"Win Rate (>=3% in 2 days): {(win_3pct/total)*100:.1f}%")
    print(f"Avg Max Gain (2 days): {avg_max_gain:.2f}%")

summarize("CURRENT STRATEGY (Acceleration)", strat_a)
summarize("NEW STRATEGY (Buy the Flag)", strat_b)

print("\n--- TOP 5 'BUY THE FLAG' EXAMPLES CAUGHT ---")
if not strat_b.empty:
    top_flags = strat_b.sort_values('max_gain_2d', ascending=False).head(5).to_dicts()
    for row in top_flags:
        print(f"{row['symbol']} on {row['time'].date()}: ROC20={row['roc_20']:.1f}, ROC10={row['roc_10']:.1f} -> Max Gain 2D: +{row['max_gain_2d']:.2f}%")
