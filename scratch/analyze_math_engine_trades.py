"""
scratch/analyze_math_engine_trades.py

Comprehensive analysis of the 8 trades from Aug 11-14, 2026:
Winners: ZYDUSLIFE, OBEROIRLTY, DRREDDY
Losers: PRESTIGE, NATIONALUM, PATANJALI, TRENT, TATAPOWER

Evaluates Math Engine thresholds:
- bbw < 0.22
- vol_surge > 2.5
- close < sma_20 * 1.03 (and close > sma_20)
- close > open
"""

import os
import polars as pl
import pandas as pd
import numpy as np

def run_analysis():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"Error: Parquet file {parquet_path} not found.")
        return

    print("Loading intraday OHLCV dataset...")
    df = pl.read_parquet(parquet_path)
    print(f"Total rows: {len(df)}, Symbols: {df['symbol'].n_unique()}")

    # Ensure sorting
    df = df.sort(["symbol", "time"])

    # Compute Math Engine metrics identical to scripts/live_hourly_job.py
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])

    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20")) * 100).alias("sma20_dist_pct"),
        (((pl.col("close") - pl.col("open")) / pl.col("open")) * 100).alias("candle_pct"),
        (((pl.col("high") - pl.col("low")) / pl.col("close")) * 100).alias("candle_range_pct"),
    ])

    # Filter for the target symbols
    target_symbols = {
        "ZYDUSLIFE": "WINNER",
        "OBEROIRLTY": "WINNER",
        "DRREDDY": "WINNER",
        "PRESTIGE": "LOSER",
        "NATIONALUM": "LOSER",
        "PATANJALI": "LOSER",
        "TRENT": "LOSER",
        "TATAPOWER": "LOSER",
    }

    # Filter for August 10 - 14, 2026
    # UTC timestamps for Aug 10-14
    aug_df = df.filter(
        (pl.col("symbol").is_in(list(target_symbols.keys()))) &
        (pl.col("time") >= pl.datetime(2026, 8, 10, 0, 0, 0, time_zone="UTC")) &
        (pl.col("time") <= pl.datetime(2026, 8, 14, 23, 59, 59, time_zone="UTC"))
    )

    print("\n" + "="*80)
    print("ALL CANDLES FOR TARGET 8 TRADES DURING AUG 10-14")
    print("="*80)

    # Let's find the exact breakout hours where Math Engine conditions met or near met
    # Math Engine: bbw < 0.22 & vol_surge > 2.5 & close > sma_20 & close < sma_20 * 1.03 & close > open
    breakout_candidates = aug_df.filter(
        (pl.col("bbw") < 0.25) &
        (pl.col("vol_surge") >= 2.0) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") > pl.col("open"))
    ).sort(["symbol", "time"])

    print(f"\nFound {len(breakout_candidates)} qualifying/near-qualifying candles for target symbols:")
    for row in breakout_candidates.iter_rows(named=True):
        outcome = target_symbols.get(row['symbol'], "UNKNOWN")
        print(f"[{outcome}] {row['symbol']:<11} | Time: {row['time']} | Close: {row['close']:>8.2f} | BBW: {row['bbw']:>6.4f} | VolSurge: {row['vol_surge']:>6.2f}x | DistSMA20: {row['sma20_dist_pct']:>5.2f}% | Candle: +{row['candle_pct']:>4.2f}%")

    # Let's isolate the specific trigger hours:
    # Based on live logs and strategy triggers:
    # ZYDUSLIFE: Aug 11 04:45 UTC (10:15 IST candle)
    # DRREDDY: Aug 11 05:45 UTC (11:15 IST candle)
    # OBEROIRLTY: Aug 13 04:45 UTC (10:15 IST candle)
    # PATANJALI: Aug 11 04:45 UTC (10:15 IST candle)
    # PRESTIGE: Aug 11 03:45 UTC (09:15 IST candle) / Aug 10-11
    # NATIONALUM: Aug 11-14 breakout
    # TRENT: Aug 14 04:45 UTC (10:15 IST candle)
    # TATAPOWER: Aug 14 05:45 UTC (11:15 IST candle)

    print("\n" + "="*80)
    print("DETAILED TRIGGER CANDLE AUDIT FOR EACH OF THE 8 TRADES")
    print("="*80)

    trigger_data = []

    for sym, outcome in target_symbols.items():
        sym_df = aug_df.filter(pl.col("symbol") == sym).sort("time")
        # Identify the candle with the highest volume surge satisfying the breakout criteria
        qual_df = sym_df.filter((pl.col("vol_surge") > 2.0) & (pl.col("close") > pl.col("sma_20")) & (pl.col("close") > pl.col("open")))
        if len(qual_df) == 0:
            # Fallback to max vol_surge candle
            qual_df = sym_df.sort("vol_surge", descending=True).head(1)
        
        # Take the most prominent trigger candle
        trigger_candle = qual_df.sort("vol_surge", descending=True).head(1).to_dicts()[0] if len(qual_df) > 0 else None
        
        if trigger_candle:
            trigger_candle["outcome"] = outcome
            trigger_data.append(trigger_candle)
            print(f"Trade: {sym:<11} [{outcome:<6}] -> Trigger Time: {trigger_candle['time']}")
            print(f"   Close: {trigger_candle['close']:.2f} | Open: {trigger_candle['open']:.2f} | High: {trigger_candle['high']:.2f} | Low: {trigger_candle['low']:.2f}")
            print(f"   BBW: {trigger_candle['bbw']:.4f} (Limit < 0.22)")
            print(f"   Vol Surge: {trigger_candle['vol_surge']:.2f}x (Limit > 2.5)")
            print(f"   Dist above SMA20: {trigger_candle['sma20_dist_pct']:.2f}% (Limit < 3.0%)")
            print(f"   Candle Body: +{trigger_candle['candle_pct']:.2f}% | Candle Range: {trigger_candle['candle_range_pct']:.2f}%")
            print("-" * 60)

    # Statistical Comparison
    trig_df = pd.DataFrame(trigger_data)
    
    print("\n" + "="*80)
    print("STATISTICAL SUMMARY: WINNERS VS LOSERS AT TRIGGER HOUR")
    print("="*80)

    winners = trig_df[trig_df["outcome"] == "WINNER"]
    losers = trig_df[trig_df["outcome"] == "LOSER"]

    metrics = ["bbw", "vol_surge", "sma20_dist_pct", "candle_pct", "candle_range_pct"]

    summary_rows = []
    for m in metrics:
        w_vals = winners[m].dropna()
        l_vals = losers[m].dropna()
        summary_rows.append({
            "Metric": m,
            "Winner Mean": f"{w_vals.mean():.4f}",
            "Winner Median": f"{w_vals.median():.4f}",
            "Winner Min": f"{w_vals.min():.4f}",
            "Winner Max": f"{w_vals.max():.4f}",
            "Loser Mean": f"{l_vals.mean():.4f}",
            "Loser Median": f"{l_vals.median():.4f}",
            "Loser Min": f"{l_vals.min():.4f}",
            "Loser Max": f"{l_vals.max():.4f}",
        })

    summary_table = pd.DataFrame(summary_rows)
    print(summary_table.to_string(index=False))

    print("\n" + "="*80)
    print("KEY PATTERNS & INSIGHTS")
    print("="*80)
    
    # Check threshold separation
    print("\n1. BOLLINGER BAND WIDTH (BBW):")
    print(f"   - Winners BBW: {list(winners['bbw'].round(4))} (Avg: {winners['bbw'].mean():.4f})")
    print(f"   - Losers BBW:  {list(losers['bbw'].round(4))} (Avg: {losers['bbw'].mean():.4f})")

    print("\n2. VOLUME SURGE (vol_surge):")
    print(f"   - Winners Vol Surge: {list(winners['vol_surge'].round(2))}x (Avg: {winners['vol_surge'].mean():.2f}x)")
    print(f"   - Losers Vol Surge:  {list(losers['vol_surge'].round(2))}x (Avg: {losers['vol_surge'].mean():.2f}x)")

    print("\n3. SMA-20 EXTENSION DISTANCE (sma20_dist_pct):")
    print(f"   - Winners SMA20 Dist: {list(winners['sma20_dist_pct'].round(2))}% (Avg: {winners['sma20_dist_pct'].mean():.2f}%)")
    print(f"   - Losers SMA20 Dist:  {list(losers['sma20_dist_pct'].round(2))}% (Avg: {losers['sma20_dist_pct'].mean():.2f}%)")

    # Generate tightened threshold test
    print("\n" + "="*80)
    print("THRESHOLD TIGHTENING SCENARIO ANALYSIS")
    print("="*80)

    scenarios = [
        {"name": "Current Baseline", "bbw": 0.22, "vol": 2.5, "max_sma": 3.0, "min_sma": 0.0},
        {"name": "Tight Vol Surge (vol > 3.5)", "bbw": 0.22, "vol": 3.5, "max_sma": 3.0, "min_sma": 0.0},
        {"name": "Tight BBW (bbw < 0.15)", "bbw": 0.15, "vol": 2.5, "max_sma": 3.0, "min_sma": 0.0},
        {"name": "Tight Vol (vol > 3.0) & BBW (bbw < 0.18)", "bbw": 0.18, "vol": 3.0, "max_sma": 3.0, "min_sma": 0.0},
        {"name": "Sovereign Elite (vol > 3.5, bbw < 0.16, sma_dist 0.5-2.5%)", "bbw": 0.16, "vol": 3.5, "max_sma": 2.5, "min_sma": 0.5},
    ]

    for sc in scenarios:
        w_passed = []
        l_passed = []
        for r in trigger_data:
            passed = (
                (r['bbw'] < sc['bbw']) and
                (r['vol_surge'] >= sc['vol']) and
                (r['sma20_dist_pct'] <= sc['max_sma']) and
                (r['sma20_dist_pct'] >= sc['min_sma'])
            )
            if passed:
                if r['outcome'] == "WINNER":
                    w_passed.append(r['symbol'])
                else:
                    l_passed.append(r['symbol'])
        
        print(f"\nScenario: {sc['name']}")
        print(f"  -> Winners Kept: {len(w_passed)}/3 ({', '.join(w_passed) if w_passed else 'None'})")
        print(f"  -> Losers Filtered Out: {5 - len(l_passed)}/5 (Remaining Losers: {', '.join(l_passed) if l_passed else 'None'})")

if __name__ == "__main__":
    run_analysis()
