"""
scripts/analyze_theory3_wick_cap.py

Fast, vectorized quantitative backtest and empirical audit of Theory 3:
Testing the "Upper Wick < 15%" rule vs the `close < sma_20 * 1.03` extension cap.
"""

import os
import polars as pl
import pandas as pd
import numpy as np

def analyze_theory3():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"Error: Parquet file {parquet_path} not found.")
        return

    df = pl.read_parquet(parquet_path)
    df = df.sort(["symbol", "time"])

    # Base indicators
    df = df.with_columns([
        pl.col("time").dt.date().alias("date"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])

    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20")) * 100).alias("dist_sma20_pct"),
        (pl.col("high") - pl.col("close")).alias("upper_wick"),
        (pl.col("close") - pl.col("open")).alias("body"),
        (pl.col("high") - pl.col("low")).alias("candle_range"),
        (((pl.col("close") - pl.col("open")) / pl.col("open")) * 100).alias("body_pct"),
    ])

    df = df.with_columns([
        pl.when(pl.col("candle_range") > 0)
        .then((pl.col("upper_wick") / pl.col("candle_range")) * 100)
        .otherwise(0.0)
        .alias("upper_wick_pct_range"),
        ((pl.col("upper_wick") / pl.col("close")) * 100).alias("upper_wick_pct_price")
    ])

    # Next candle returns & intra-day forward trajectories
    # Leads within symbol
    df = df.with_columns([
        pl.col("close").shift(-1).over("symbol").alias("next_1_close"),
        pl.col("close").shift(-2).over("symbol").alias("next_2_close"),
        pl.col("date").shift(-1).over("symbol").alias("next_1_date"),
        pl.col("date").shift(-2).over("symbol").alias("next_2_date"),
        pl.col("close").last().over(["symbol", "date"]).alias("eod_close"),
    ])

    # Compute rest-of-day max high and min low
    # We can shift high and low by -1..-5 over symbol and filter same day
    for k in range(1, 6):
        df = df.with_columns([
            pl.when(pl.col("date").shift(-k).over("symbol") == pl.col("date"))
            .then(pl.col("high").shift(-k).over("symbol"))
            .otherwise(pl.col("close"))
            .alias(f"lead_high_{k}"),
            pl.when(pl.col("date").shift(-k).over("symbol") == pl.col("date"))
            .then(pl.col("low").shift(-k).over("symbol"))
            .otherwise(pl.col("close"))
            .alias(f"lead_low_{k}")
        ])

    df = df.with_columns([
        pl.max_horizontal([pl.col(f"lead_high_{k}") for k in range(1, 6)]).alias("rest_of_day_high"),
        pl.min_horizontal([pl.col(f"lead_low_{k}") for k in range(1, 6)]).alias("rest_of_day_low"),
    ])

    # Compute forward returns
    df = df.with_columns([
        # 1-hour forward return (only if same day)
        pl.when(pl.col("next_1_date") == pl.col("date"))
        .then(((pl.col("next_1_close") - pl.col("close")) / pl.col("close")) * 100)
        .otherwise(0.0)
        .alias("ret_1h"),
        
        # 2-hour forward return (only if same day)
        pl.when(pl.col("next_2_date") == pl.col("date"))
        .then(((pl.col("next_2_close") - pl.col("close")) / pl.col("close")) * 100)
        .otherwise(((pl.col("eod_close") - pl.col("close")) / pl.col("close")) * 100)
        .alias("ret_2h"),

        # EOD return
        (((pl.col("eod_close") - pl.col("close")) / pl.col("close")) * 100).alias("ret_eod"),

        # Intraday Max High & Min Low
        (((pl.col("rest_of_day_high") - pl.col("close")) / pl.col("close")) * 100).alias("max_high_intra"),
        (((pl.col("rest_of_day_low") - pl.col("close")) / pl.col("close")) * 100).alias("min_low_intra"),
    ])

    # Convert to pandas for easy analysis
    pdf = df.to_pandas()
    pdf['date'] = pd.to_datetime(pdf['date'])

    # Filter for August 2026 breakout candidates:
    # Setup conditions: bbw < 0.22, vol_surge > 2.5, close > sma_20, close > open
    candidates = pdf[
        (pdf['date'] >= pd.to_datetime('2026-08-01')) &
        (pdf['bbw'] < 0.22) &
        (pdf['vol_surge'] > 2.5) &
        (pdf['close'] > pdf['sma_20']) &
        (pdf['close'] > pdf['open'])
    ].copy()

    # Simulated Option Trade P&L:
    # Option PnL = Spot PnL * 10 (Delta 0.5, 5% premium)
    # Target +50% option gain (+5% spot) -> lock profit
    # Stop loss -20% option loss (-2% spot) -> cut
    def calc_trade_pnl(row):
        max_g = row['max_high_intra'] * 10.0
        max_dd = row['min_low_intra'] * 10.0
        eod_opt = row['ret_eod'] * 10.0
        if max_g >= 50.0:
            return max(30.0, eod_opt), "BIG_WINNER"
        elif max_dd <= -20.0 and max_g < 15.0:
            return -20.0, "STOP_LOSS"
        elif eod_opt > 10.0:
            return eod_opt, "WINNER"
        elif eod_opt < -10.0:
            return eod_opt, "LOSER"
        else:
            return eod_opt, "SCRATCH"

    res = candidates.apply(calc_trade_pnl, axis=1)
    candidates['sim_trade_pnl'] = [r[0] for r in res]
    candidates['trade_outcome'] = [r[1] for r in res]
    candidates['option_ret_eod'] = candidates['ret_eod'] * 10.0

    last_week_mask = candidates['date'] >= pd.to_datetime('2026-08-10')
    
    print("=" * 95)
    print("THEORY 3 QUANTITATIVE AUDIT: WICK/PRICE ACTION RELAXATION & EXTENSION CAP")
    print("=" * 95)
    print(f"Total August 2026 Breakout Candles: {len(candidates)}")
    print(f"Total Last Week (Aug 10-14) Breakout Candles: {last_week_mask.sum()}")
    
    # Split by Extension Cap: <= 3% vs > 3%
    cap_pass = candidates[candidates['dist_sma20_pct'] <= 3.0]
    cap_reject = candidates[candidates['dist_sma20_pct'] > 3.0]
    
    lw_pass = candidates[last_week_mask & (candidates['dist_sma20_pct'] <= 3.0)]
    lw_reject = candidates[last_week_mask & (candidates['dist_sma20_pct'] > 3.0)]

    print(f"\n--- POPULATION BREAKDOWN (LAST WEEK: AUG 10-14, 2026) ---")
    print(f"Accepted by Baseline (Close <= SMA20 * 1.03): {len(lw_pass)} setups")
    print(f"Rejected by Baseline (Close > SMA20 * 1.03)  : {len(lw_reject)} setups")

    print(f"\n--- POPULATION BREAKDOWN (FULL AUGUST 2026: AUG 03-14) ---")
    print(f"Accepted by Baseline (Close <= SMA20 * 1.03): {len(cap_pass)} setups")
    print(f"Rejected by Baseline (Close > SMA20 * 1.03)  : {len(cap_reject)} setups")

    # Statistical comparison between Accepted and Rejected
    print("\n" + "=" * 95)
    print("STATISTICAL COMPARISON: ACCEPTED (<= 3%) vs REJECTED (> 3%) TRADES LAST WEEK")
    print("=" * 95)
    
    for label, group in [("ACCEPTED (Close <= SMA20 * 1.03)", lw_pass), ("REJECTED (Close > SMA20 * 1.03)", lw_reject)]:
        print(f"\nGroup: {label} (N={len(group)})")
        print(f"  • Mean Spot EOD Return:       {group['ret_eod'].mean():+.2f}% (Median: {group['ret_eod'].median():+.2f}%)")
        print(f"  • Mean Intraday Max High:     +{group['max_high_intra'].mean():.2f}% (Median: +{group['max_high_intra'].median():.2f}%)")
        print(f"  • Mean Intraday Max Drawdown: {group['min_low_intra'].mean():.2f}% (Median: {group['min_low_intra'].median():.2f}%)")
        print(f"  • Spot Win Rate (EOD > 0):    {(group['ret_eod'] > 0).mean()*100:.1f}%")
        print(f"  • Mean Option EOD P&L:        {group['option_ret_eod'].mean():+.2f}%")
        print(f"  • Simulated Trade P&L:        {group['sim_trade_pnl'].mean():+.2f}% (Total Sum: {group['sim_trade_pnl'].sum():+.0f}%)")
        
        pos_pnl = group[group['sim_trade_pnl'] > 0]['sim_trade_pnl'].sum()
        neg_pnl = abs(group[group['sim_trade_pnl'] < 0]['sim_trade_pnl'].sum())
        pf = (pos_pnl / neg_pnl) if neg_pnl > 0 else 0
        print(f"  • Profit Factor:              {pf:.2f}")
        print(f"  • Explosive Moves (Max High >= +3% spot / +30% option): {(group['max_high_intra'] >= 3.0).sum()} ({(group['max_high_intra'] >= 3.0).mean()*100:.1f}%)")
        print(f"  • Severe Traps (Drawdown <= -2% spot / -20% option)   : {(group['min_low_intra'] <= -2.0).sum()} ({(group['min_low_intra'] <= -2.0).mean()*100:.1f}%)")

    # Detailed Audit of Rejected Trades Last Week
    print("\n" + "=" * 95)
    print("ALL TOP MISSED HIGH-PROFIT TRADES REJECTED BY SMA20 * 1.03 (LAST WEEK)")
    print("=" * 95)
    
    top_missed_lw = lw_reject[lw_reject['max_high_intra'] >= 2.0].sort_values('max_high_intra', ascending=False)
    print(f"Found {len(top_missed_lw)} rejected trades with max gain >= +2.0% (+20% option):")
    cols = ['date', 'time', 'symbol', 'close', 'dist_sma20_pct', 'upper_wick_pct_range', 'vol_surge', 'max_high_intra', 'ret_eod', 'min_low_intra']
    print(top_missed_lw[cols].head(25).to_string(index=False))

    print("\n" + "=" * 95)
    print("WORST BULLET-DODGED TRAPS / BLOW-OFF TOPS REJECTED BY SMA20 * 1.03 (LAST WEEK)")
    print("=" * 95)
    top_traps_lw = lw_reject[lw_reject['min_low_intra'] <= -2.0].sort_values('min_low_intra', ascending=True)
    print(f"Found {len(top_traps_lw)} rejected trades that suffered severe intraday drawdowns (<= -2.0% spot / -20% option):")
    print(top_traps_lw[cols].head(15).to_string(index=False))

    # Evaluate the Upper Wick Distribution on the Rejected vs Accepted population
    print("\n" + "=" * 95)
    print("UPPER WICK ANALYSIS: CAN UPPER WICK FILTER SEPARATE WINNERS FROM TRAPS?")
    print("=" * 95)

    print("\nRejected population categorized by Upper Wick % of Candle Range:")
    for wick_thresh in [5, 10, 15, 20, 30, 50]:
        subset = lw_reject[lw_reject['upper_wick_pct_range'] < wick_thresh]
        if len(subset) == 0:
            continue
        print(f"  • Upper Wick < {wick_thresh:2d}% (N={len(subset):3d}): WinRate={(subset['ret_eod']>0).mean()*100:4.1f}% | Avg Spot EOD={subset['ret_eod'].mean():+5.2f}% | Avg Max Gain=+{subset['max_high_intra'].mean():4.2f}% | Avg Max DD={subset['min_low_intra'].mean():+5.2f}% | Big Wins={(subset['max_high_intra']>=3.0).sum():2d} | Big Traps={(subset['min_low_intra']<=-2.0).sum():2d}")

    # SCENARIOS BACKTESTING ACROSS LAST WEEK & FULL AUGUST
    rules = [
        {
            "id": "1",
            "name": "1. Current Baseline (Dist <= 3.0%)",
            "filter": lambda d: d['dist_sma20_pct'] <= 3.0
        },
        {
            "id": "2",
            "name": "2. Theory 3 Pure Wick (No SMA Cap, Upper Wick < 15%)",
            "filter": lambda d: d['upper_wick_pct_range'] < 15.0
        },
        {
            "id": "3",
            "name": "3. Strict Wick (No SMA Cap, Upper Wick < 10%)",
            "filter": lambda d: d['upper_wick_pct_range'] < 10.0
        },
        {
            "id": "4",
            "name": "4. Relaxed Wick (No SMA Cap, Upper Wick < 20%)",
            "filter": lambda d: d['upper_wick_pct_range'] < 20.0
        },
        {
            "id": "5",
            "name": "5. Hybrid Moderate (Dist <= 5.0% AND Upper Wick < 15%)",
            "filter": lambda d: (d['dist_sma20_pct'] <= 5.0) & (d['upper_wick_pct_range'] < 15.0)
        },
        {
            "id": "6",
            "name": "6. Hybrid Expanded (Dist <= 6.0% AND Upper Wick < 15%)",
            "filter": lambda d: (d['dist_sma20_pct'] <= 6.0) & (d['upper_wick_pct_range'] < 15.0)
        },
        {
            "id": "7",
            "name": "7. Dual Tier (Dist <= 3.0% OR (Dist <= 6.0% & Wick < 10% & Vol > 3.5))",
            "filter": lambda d: (d['dist_sma20_pct'] <= 3.0) | ((d['dist_sma20_pct'] <= 6.0) & (d['upper_wick_pct_range'] < 10.0) & (d['vol_surge'] > 3.5))
        },
        {
            "id": "8",
            "name": "8. Dual Tier + Wick (Baseline + (3-5% Dist & Wick < 15%))",
            "filter": lambda d: (d['dist_sma20_pct'] <= 3.0) | ((d['dist_sma20_pct'] <= 5.0) & (d['upper_wick_pct_range'] < 15.0))
        },
        {
            "id": "9",
            "name": "9. Dual Tier + Strict Wick (Baseline + (3-6% Dist & Wick < 10%))",
            "filter": lambda d: (d['dist_sma20_pct'] <= 3.0) | ((d['dist_sma20_pct'] <= 6.0) & (d['upper_wick_pct_range'] < 10.0))
        }
    ]

    for scope_name, scope_df in [("LAST WEEK (AUG 10-14, 2026)", candidates[last_week_mask]), ("FULL AUGUST 2026 (AUG 03-14, 2026)", candidates)]:
        print("\n" + "=" * 95)
        print(f"COMPREHENSIVE SCENARIO MATRIX: {scope_name}")
        print("=" * 95)
        
        results = []
        for r in rules:
            selected = scope_df[r['filter'](scope_df)]
            n_trades = len(selected)
            if n_trades == 0:
                continue
                
            win_rate_eod = (selected['ret_eod'] > 0).mean() * 100
            big_win_cnt = (selected['max_high_intra'] >= 3.0).sum()
            big_win_pct = (selected['max_high_intra'] >= 3.0).mean() * 100
            big_loss_cnt = (selected['min_low_intra'] <= -2.0).sum()
            big_loss_pct = (selected['min_low_intra'] <= -2.0).mean() * 100
            
            mean_eod_spot = selected['ret_eod'].mean()
            mean_trade_pnl = selected['sim_trade_pnl'].mean()
            total_sim_pnl = selected['sim_trade_pnl'].sum()
            
            # Profit factor
            pos_pnl = selected[selected['sim_trade_pnl'] > 0]['sim_trade_pnl'].sum()
            neg_pnl = abs(selected[selected['sim_trade_pnl'] < 0]['sim_trade_pnl'].sum())
            profit_factor = (pos_pnl / neg_pnl) if neg_pnl > 0 else np.nan
            
            avg_max_gain = selected['max_high_intra'].mean()
            avg_max_dd = selected['min_low_intra'].mean()
            
            results.append({
                "Rule": r['name'],
                "Trades": n_trades,
                "WinRate %": f"{win_rate_eod:.1f}%",
                "Spot EOD": f"{mean_eod_spot:+.2f}%",
                "Avg PnL": f"{mean_trade_pnl:+.2f}%",
                "Total PnL": f"{total_sim_pnl:+.0f}%",
                "ProfitFac": f"{profit_factor:.2f}",
                "Max Gain": f"+{avg_max_gain:.2f}%",
                "Max DD": f"{avg_max_dd:.2f}%",
                "BigWins(>=3%)": f"{big_win_cnt} ({big_win_pct:.0f}%)",
                "Traps(<=-2%)": f"{big_loss_cnt} ({big_loss_pct:.0f}%)"
            })
            
        res_df = pd.DataFrame(results)
        print(res_df.to_string(index=False))

    # Check specific explosive setups and case studies
    print("\n" + "=" * 95)
    print("CASE STUDIES: EXPLOSIVE WINNERS MISSED VS BLOWN-OFF TRAPS SAVED")
    print("=" * 95)
    
    # Let's inspect notable symbols
    notable_symbols = ["PAYTM", "ZYDUSLIFE", "OBEROIRLTY", "DRREDDY", "TRENT", "TATAPOWER", "PATANJALI", "PRESTIGE", "NATIONALUM", "IDEA", "SUZLON", "BHEL", "COALINDIA"]
    found_notable = candidates[candidates['symbol'].isin(notable_symbols)].sort_values(['symbol', 'time'])
    if len(found_notable) > 0:
        print(f"\nNotable Core Symbols in August 2026 dataset:")
        print(found_notable[['date', 'time', 'symbol', 'close', 'dist_sma20_pct', 'upper_wick_pct_range', 'vol_surge', 'ret_eod', 'max_high_intra', 'min_low_intra']].to_string(index=False))

if __name__ == "__main__":
    analyze_theory3()
