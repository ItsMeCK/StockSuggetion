"""
scratch/analyze_volume_expansion.py

In-depth mathematical analysis of Volume Profile Expansion for August 2026.
Tests whether replacing / augmenting single-hour 'vol_surge > 2.5' with:
1. Cumulative 3-Hour Volume (vol_surge_3h)
2. Intraday VWAP Trend (Close > VWAP & VWAP slope > 0)
3. Steady Accumulation Multi-Candle Profile
captures more winners, enters earlier, and reduces single-candle exhaustion traps.
"""

import os
import polars as pl
import pandas as pd
import numpy as np
from datetime import datetime

def run_analysis():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"Error: Parquet file {parquet_path} not found.")
        return

    print("Loading intraday OHLCV dataset...")
    df = pl.read_parquet(parquet_path)
    print(f"Dataset loaded: {len(df):,} rows across {df['symbol'].n_unique()} symbols.")
    print(f"Time range: {df['time'].min()} to {df['time'].max()}")

    # Sort
    df = df.sort(["symbol", "time"])

    # 1. Base Rolling Indicators
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
        pl.col("volume").rolling_sum(window_size=3).over("symbol").alias("vol_sum_3h"),
        pl.col("volume").rolling_sum(window_size=2).over("symbol").alias("vol_sum_2h"),
    ])

    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge_1h"),
        (pl.col("vol_sum_3h") / (3.0 * pl.col("vol_avg_20"))).alias("vol_surge_3h"),
        (pl.col("vol_sum_2h") / (2.0 * pl.col("vol_avg_20"))).alias("vol_surge_2h"),
        (((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20")) * 100).alias("sma20_dist_pct"),
        (((pl.col("close") - pl.col("open")) / pl.col("open")) * 100).alias("candle_pct"),
        pl.col("close").shift(1).over("symbol").alias("close_prev1"),
    ])

    df = df.with_columns([
        pl.col("vol_surge_1h").shift(1).over("symbol").alias("vol_surge_prev1"),
        pl.col("vol_surge_1h").shift(2).over("symbol").alias("vol_surge_prev2"),
    ])

    # 2. Intraday VWAP & VWAP Slope (Calculated per symbol per day)
    df = df.with_columns([
        pl.col("time").dt.date().alias("date"),
        ((pl.col("high") + pl.col("low") + pl.col("close")) / 3.0).alias("typical_price"),
    ])
    df = df.with_columns([
        (pl.col("typical_price") * pl.col("volume")).alias("pv")
    ])
    df = df.with_columns([
        pl.col("pv").cum_sum().over(["symbol", "date"]).alias("cum_pv"),
        pl.col("volume").cum_sum().over(["symbol", "date"]).alias("cum_vol"),
    ])
    df = df.with_columns([
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap")
    ])
    df = df.with_columns([
        (((pl.col("close") - pl.col("vwap")) / pl.col("vwap")) * 100).alias("vwap_dist_pct"),
        pl.col("vwap").shift(1).over(["symbol", "date"]).alias("vwap_prev"),
    ])
    df = df.with_columns([
        (((pl.col("vwap") - pl.col("vwap_prev")) / pl.col("vwap_prev")) * 100).alias("vwap_slope_pct"),
    ])

    # 3. Calculate Forward Returns (Forward 1-bar, 3-bar, EOD, and Max High over next 7 bars ~ 1 day)
    df = df.with_columns([
        pl.col("close").shift(-1).over("symbol").alias("fwd_close_1h"),
        pl.col("close").shift(-3).over("symbol").alias("fwd_close_3h"),
        pl.col("high").rolling_max(window_size=7).shift(-7).over("symbol").alias("fwd_max_high_7h"),
        pl.col("low").rolling_min(window_size=7).shift(-7).over("symbol").alias("fwd_min_low_7h"),
        pl.col("close").last().over(["symbol", "date"]).alias("day_close"),
    ])

    df = df.with_columns([
        (((pl.col("fwd_close_1h") - pl.col("close")) / pl.col("close")) * 100).alias("ret_1h"),
        (((pl.col("fwd_close_3h") - pl.col("close")) / pl.col("close")) * 100).alias("ret_3h"),
        (((pl.col("day_close") - pl.col("close")) / pl.col("close")) * 100).alias("ret_eod"),
        (((pl.col("fwd_max_high_7h") - pl.col("close")) / pl.col("close")) * 100).alias("max_fwd_gain"),
        (((pl.col("fwd_min_low_7h") - pl.col("close")) / pl.col("close")) * 100).alias("max_fwd_drawdown"),
    ])

    # Filter for August 2026 (Aug 3 to Aug 14)
    aug_df = df.filter(
        (pl.col("time") >= pl.datetime(2026, 8, 3, 0, 0, 0, time_zone="UTC")) &
        (pl.col("time") <= pl.datetime(2026, 8, 14, 23, 59, 59, time_zone="UTC"))
    )

    print(f"\nAugust 2026 dataset: {len(aug_df):,} hourly candles.")

    # 4. Identify Ground Truth Winners across August 2026
    symbol_aug_summary = aug_df.group_by("symbol").agg([
        pl.col("close").first().alias("aug_open_price"),
        pl.col("high").max().alias("aug_max_high"),
        pl.col("close").last().alias("aug_last_price"),
    ]).with_columns([
        (((pl.col("aug_max_high") - pl.col("aug_open_price")) / pl.col("aug_open_price")) * 100).alias("max_aug_gain_pct"),
        (((pl.col("aug_last_price") - pl.col("aug_open_price")) / pl.col("aug_open_price")) * 100).alias("aug_total_return_pct"),
    ]).sort("max_aug_gain_pct", descending=True)

    top_aug_movers = symbol_aug_summary.filter(pl.col("max_aug_gain_pct") >= 7.0)
    print(f"\nFound {len(top_aug_movers)} major movers with >= 7% gain in August 2026:")
    top_symbols_list = top_aug_movers["symbol"].to_list()
    print(", ".join(top_symbols_list[:30]))

    # 5. Define Candidate Screening Models
    models = {
        "Baseline (1h Vol > 2.5)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("vol_surge_1h") > 2.5) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
        "Model A (3H Cum Surge >= 1.6 & 1h >= 1.2)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("vol_surge_3h") >= 1.6) &
            (pl.col("vol_surge_1h") >= 1.2) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
        "Model B (VWAP Trend + 3H Cum >= 1.5)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("close") > pl.col("vwap")) &
            ((pl.col("vwap_slope_pct") >= 0.0) | pl.col("vwap_slope_pct").is_null()) &
            (pl.col("vol_surge_3h") >= 1.5) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
        "Model C (Dual-Track: 1h>2.5 OR [3H>=1.5 & VWAP])": (
            (pl.col("bbw") < 0.25) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open")) &
            (
                (pl.col("vol_surge_1h") >= 2.5) |
                ((pl.col("vol_surge_3h") >= 1.5) & (pl.col("vol_surge_1h") >= 1.1) & (pl.col("close") > pl.col("vwap")))
            )
        ),
        "Model D (Steady Accum 2-Candle: 1h>=1.4 & prev>=1.2 + VWAP)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("vol_surge_1h") >= 1.4) &
            (pl.col("vol_surge_prev1") >= 1.2) &
            (pl.col("close") > pl.col("vwap")) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
        "Model E (Triple Accumulation: 3 consecutive candles > 1.2x vol)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("vol_surge_1h") >= 1.2) &
            (pl.col("vol_surge_prev1") >= 1.2) &
            (pl.col("vol_surge_prev2") >= 1.2) &
            (pl.col("close") > pl.col("vwap")) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
    }

    # Evaluate Each Model
    print("\n" + "="*95)
    print(f"{'MODEL PERFORMANCE COMPARISON ON AUGUST 2026 INTRADAY DATA':^95}")
    print("="*95)

    results_table = []
    signals_by_model = {}

    for name, condition in models.items():
        sig_df = aug_df.filter(condition).sort(["time", "vol_surge_1h"], descending=[False, True])
        signals_by_model[name] = sig_df
        
        n_triggers = len(sig_df)
        unique_symbols = sig_df["symbol"].n_unique()
        
        # How many of the major August movers (top_symbols_list) did this model catch?
        caught_movers = set(sig_df["symbol"].unique().to_list()).intersection(set(top_symbols_list))
        mover_capture_rate = (len(caught_movers) / len(top_symbols_list)) * 100 if top_symbols_list else 0
        
        # Returns
        avg_ret_1h = sig_df["ret_1h"].mean()
        avg_ret_3h = sig_df["ret_3h"].mean()
        avg_ret_eod = sig_df["ret_eod"].mean()
        avg_max_gain = sig_df["max_fwd_gain"].mean()
        avg_max_dd = sig_df["max_fwd_drawdown"].mean()
        
        # Win rate (Max forward gain >= 3.0% within next day)
        win_count_3pct = len(sig_df.filter(pl.col("max_fwd_gain") >= 3.0))
        win_rate_3pct = (win_count_3pct / n_triggers * 100) if n_triggers > 0 else 0
        
        win_count_5pct = len(sig_df.filter(pl.col("max_fwd_gain") >= 5.0))
        win_rate_5pct = (win_count_5pct / n_triggers * 100) if n_triggers > 0 else 0

        # Loss rate (Max drawdown < -2.0% before 2.0% gain)
        loser_count = len(sig_df.filter((pl.col("max_fwd_drawdown") <= -2.0) & (pl.col("max_fwd_gain") < 2.0)))
        loss_rate = (loser_count / n_triggers * 100) if n_triggers > 0 else 0
        
        results_table.append({
            "Model": name,
            "Triggers": n_triggers,
            "Unique Sym": unique_symbols,
            "Movers Caught": f"{len(caught_movers)}/{len(top_symbols_list)} ({mover_capture_rate:.0f}%)",
            "Win >=3%": f"{win_rate_3pct:.1f}%",
            "Win >=5%": f"{win_rate_5pct:.1f}%",
            "Loss Rate": f"{loss_rate:.1f}%",
            "Avg EOD": f"{avg_ret_eod:+.2f}%",
            "Avg Max Gain": f"{avg_max_gain:+.2f}%",
            "Avg Max DD": f"{avg_max_dd:+.2f}%",
        })

    res_df = pd.DataFrame(results_table)
    print(res_df.to_string(index=False))

    # 6. Detailed Symbol-by-Symbol Audit for Key Trades
    print("\n" + "="*95)
    print(f"{'TIMING & CAPTURE AUDIT FOR KEY WINNERS & LOSERS':^95}")
    print("="*95)

    key_case_studies = [
        ("ZYDUSLIFE", "WINNER (+8.7%)"),
        ("OBEROIRLTY", "WINNER (+5.0%)"),
        ("CIPLA", "WINNER (+10.8%)"),
        ("DRREDDY", "WINNER (+5.3%)"),
        ("ADANIENT", "WINNER (+12.9%)"),
        ("SAREGAMA", "WINNER (+24.9%)"),
        ("PRESTIGE", "LOSER (Reversal)"),
        ("NATIONALUM", "LOSER (Reversal)"),
        ("PATANJALI", "LOSER (Reversal)"),
        ("TRENT", "LOSER (Reversal)"),
        ("TATAPOWER", "LOSER (Reversal)"),
    ]

    for sym, category in key_case_studies:
        sym_aug = aug_df.filter(pl.col("symbol") == sym).sort("time")
        if len(sym_aug) == 0:
            continue
            
        print(f"\n>>> {sym} [{category}] - Signal Trigger Timeline:")
        
        # Check first trigger time and price under each model
        for model_name, cond in models.items():
            trigs = sym_aug.filter(cond)
            if len(trigs) > 0:
                first_trig = trigs.head(1).to_dicts()[0]
                t_val = first_trig["time"]
                px = first_trig["close"]
                v1h = first_trig["vol_surge_1h"]
                v3h = first_trig["vol_surge_3h"]
                vwap_d = first_trig["vwap_dist_pct"]
                max_g = first_trig["max_fwd_gain"]
                print(f"   [{model_name[:30]:<30}] -> {t_val} | Px: {px:>8.2f} | 1hVol: {v1h:>4.2f}x | 3hCumVol: {v3h:>4.2f}x | VWAP Dist: {vwap_d:>+5.2f}% | MaxGain: {max_g:>+5.2f}%")
            else:
                print(f"   [{model_name[:30]:<30}] -> NO SIGNAL TRIGGERED")

if __name__ == "__main__":
    run_analysis()
