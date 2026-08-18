"""
scratch/analyze_winner_expansion_full.py

Comprehensive mathematical analysis of Volume Profile Expansion across all 498 symbols
in data/intraday_ohlcv.parquet for August 2026.

Analyzes:
1. Ground truth top weekly/monthly winners.
2. Direct comparison of:
   - Baseline Single-Hour Surge (vol_surge_1h > 2.5)
   - Cumulative 3-Hour Volume Surge (vol_surge_3h >= 1.5, vol_surge_1h >= 1.1)
   - Cumulative 2-Hour Volume Surge (vol_surge_2h >= 1.4, close > VWAP)
   - Intraday VWAP Trend Rule (close > VWAP & vwap_slope >= 0 & vol_surge_3h >= 1.4)
   - Dual-Track Hybrid Engine (1h > 2.5 OR [3h >= 1.5 & 1h >= 1.1 & close > VWAP])
3. Entry timing advantage (hours earlier) and price improvement (entry price % discount).
4. Full portfolio / universe performance metrics:
   - Total Triggers
   - Unique Winners Captured
   - Capture Rate of Top Movers
   - Win Rate (>= +3.0%, >= +5.0%)
   - Drawdown & Loss Rate (< -2.0%)
   - Profit Factor & Average Forward Gain
"""

import os
import polars as pl
import pandas as pd
import numpy as np

def run_full_analysis():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"Error: Parquet file {parquet_path} not found.")
        return

    print("Loading intraday OHLCV dataset...")
    df = pl.read_parquet(parquet_path)
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
    ])

    df = df.with_columns([
        pl.col("vol_surge_1h").shift(1).over("symbol").alias("vol_surge_prev1"),
        pl.col("vol_surge_1h").shift(2).over("symbol").alias("vol_surge_prev2"),
        pl.col("close").shift(1).over("symbol").alias("close_prev1"),
    ])

    # 2. Intraday VWAP & VWAP Slope (per symbol per day)
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

    # 3. Forward Returns (Forward 1d = 7 bars, 2d = 14 bars)
    df = df.with_columns([
        pl.col("close").shift(-1).over("symbol").alias("fwd_close_1h"),
        pl.col("close").shift(-7).over("symbol").alias("fwd_close_1d"),
        pl.col("close").shift(-14).over("symbol").alias("fwd_close_2d"),
        pl.col("high").rolling_max(window_size=14).shift(-14).over("symbol").alias("fwd_max_high_2d"),
        pl.col("low").rolling_min(window_size=14).shift(-14).over("symbol").alias("fwd_min_low_2d"),
        pl.col("close").last().over(["symbol", "date"]).alias("day_close"),
    ])

    df = df.with_columns([
        (((pl.col("fwd_close_1h") - pl.col("close")) / pl.col("close")) * 100).alias("ret_1h"),
        (((pl.col("day_close") - pl.col("close")) / pl.col("close")) * 100).alias("ret_eod"),
        (((pl.col("fwd_close_1d") - pl.col("close")) / pl.col("close")) * 100).alias("ret_1d"),
        (((pl.col("fwd_close_2d") - pl.col("close")) / pl.col("close")) * 100).alias("ret_2d"),
        (((pl.col("fwd_max_high_2d") - pl.col("close")) / pl.col("close")) * 100).alias("max_fwd_gain_2d"),
        (((pl.col("fwd_min_low_2d") - pl.col("close")) / pl.col("close")) * 100).alias("max_fwd_dd_2d"),
    ])

    # Filter for August 2026 (Aug 3 to Aug 14)
    aug_df = df.filter(
        (pl.col("time") >= pl.datetime(2026, 8, 3, 0, 0, 0, time_zone="UTC")) &
        (pl.col("time") <= pl.datetime(2026, 8, 14, 23, 59, 59, time_zone="UTC"))
    )

    print(f"August 2026 dataset: {len(aug_df):,} hourly candles across {aug_df['symbol'].n_unique()} symbols.")

    # 4. Identify Top Movers in August 2026
    # Let's compute maximum runup from any point in August
    symbol_movers = aug_df.group_by("symbol").agg([
        pl.col("close").first().alias("aug_start_close"),
        pl.col("high").max().alias("aug_max_high"),
        pl.col("close").last().alias("aug_end_close"),
    ]).with_columns([
        (((pl.col("aug_max_high") - pl.col("aug_start_close")) / pl.col("aug_start_close")) * 100).alias("max_gain_pct"),
        (((pl.col("aug_end_close") - pl.col("aug_start_close")) / pl.col("aug_start_close")) * 100).alias("net_gain_pct"),
    ]).sort("max_gain_pct", descending=True)

    top_movers_5pct = symbol_movers.filter(pl.col("max_gain_pct") >= 5.0)["symbol"].to_list()
    top_movers_8pct = symbol_movers.filter(pl.col("max_gain_pct") >= 8.0)["symbol"].to_list()
    top_movers_10pct = symbol_movers.filter(pl.col("max_gain_pct") >= 10.0)["symbol"].to_list()

    print(f"\nAugust 2026 Movers Universe:")
    print(f" - Movers with >= 5% gain: {len(top_movers_5pct)} stocks")
    print(f" - Movers with >= 8% gain: {len(top_movers_8pct)} stocks")
    print(f" - Movers with >= 10% gain: {len(top_movers_10pct)} stocks")

    # 5. Define Candidate Strategies
    strategies = {
        "1. Baseline (1h Vol Surge > 2.5)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("vol_surge_1h") > 2.5) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
        "2. Cumulative 3H Volume (3h >= 1.5, 1h >= 1.1)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("vol_surge_3h") >= 1.5) &
            (pl.col("vol_surge_1h") >= 1.1) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
        "3. Cumulative 2H Volume + VWAP (2h >= 1.4, 1h >= 1.1, close > VWAP)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("vol_surge_2h") >= 1.4) &
            (pl.col("vol_surge_1h") >= 1.1) &
            (pl.col("close") > pl.col("vwap")) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
        "4. VWAP Trend + Cumulative 3H (close > VWAP, slope >= 0, 3h >= 1.4)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("close") > pl.col("vwap")) &
            ((pl.col("vwap_slope_pct") >= 0.0) | pl.col("vwap_slope_pct").is_null()) &
            (pl.col("vol_surge_3h") >= 1.4) &
            (pl.col("vol_surge_1h") >= 1.1) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open"))
        ),
        "5. Dual-Track Hybrid (Spike > 2.5 OR Steady 3H >= 1.5 + VWAP)": (
            (pl.col("bbw") < 0.25) &
            (pl.col("close") > pl.col("sma_20")) &
            (pl.col("close") < pl.col("sma_20") * 1.03) &
            (pl.col("close") > pl.col("open")) &
            (
                (pl.col("vol_surge_1h") >= 2.5) |
                ((pl.col("vol_surge_3h") >= 1.5) & (pl.col("vol_surge_1h") >= 1.1) & (pl.col("close") > pl.col("vwap")))
            )
        ),
    }

    # 6. Overall Performance Comparison
    summary_rows = []
    first_signals_by_strat = {}

    for strat_name, cond in strategies.items():
        matched_df = aug_df.filter(cond).sort(["time", "vol_surge_1h"], descending=[False, True])
        
        # Deduplicate signals to first signal per symbol per day to simulate realistic trade entries
        first_sig = matched_df.group_by(["symbol", "date"]).first().sort(["date", "time"])
        first_signals_by_strat[strat_name] = first_sig
        
        n_triggers = len(first_sig)
        unique_syms = first_sig["symbol"].n_unique()
        
        # Winner capture
        caught_5pct = len(set(first_sig["symbol"].to_list()).intersection(set(top_movers_5pct)))
        caught_8pct = len(set(first_sig["symbol"].to_list()).intersection(set(top_movers_8pct)))
        caught_10pct = len(set(first_sig["symbol"].to_list()).intersection(set(top_movers_10pct)))
        
        cap_rate_5pct = (caught_5pct / len(top_movers_5pct)) * 100 if top_movers_5pct else 0
        cap_rate_8pct = (caught_8pct / len(top_movers_8pct)) * 100 if top_movers_8pct else 0
        cap_rate_10pct = (caught_10pct / len(top_movers_10pct)) * 100 if top_movers_10pct else 0

        # Quality metrics (Trade outcomes)
        valid_trades = first_sig.filter(pl.col("max_fwd_gain_2d").is_not_null())
        n_valid = len(valid_trades)
        
        win_3pct = len(valid_trades.filter(pl.col("max_fwd_gain_2d") >= 3.0))
        win_5pct = len(valid_trades.filter(pl.col("max_fwd_gain_2d") >= 5.0))
        
        loss_2pct = len(valid_trades.filter((pl.col("max_fwd_dd_2d") <= -2.0) & (pl.col("max_fwd_gain_2d") < 2.0)))
        
        win_rate_3 = (win_3pct / n_valid * 100) if n_valid > 0 else 0
        win_rate_5 = (win_5pct / n_valid * 100) if n_valid > 0 else 0
        loss_rate = (loss_2pct / n_valid * 100) if n_valid > 0 else 0

        avg_gain = valid_trades["max_fwd_gain_2d"].mean()
        avg_dd = valid_trades["max_fwd_dd_2d"].mean()
        avg_ret_2d = valid_trades["ret_2d"].mean()
        avg_ret_eod = valid_trades["ret_eod"].mean()

        # Profit factor approx: sum(positive ret_2d) / abs(sum(negative ret_2d))
        pos_rets = valid_trades.filter(pl.col("ret_2d") > 0)["ret_2d"].sum()
        neg_rets = abs(valid_trades.filter(pl.col("ret_2d") < 0)["ret_2d"].sum())
        profit_factor = (pos_rets / neg_rets) if neg_rets > 0 else 0

        summary_rows.append({
            "Strategy": strat_name,
            "Daily Signals": n_triggers,
            "Unique Stocks": unique_syms,
            "Movers >=8% Caught": f"{caught_8pct}/{len(top_movers_8pct)} ({cap_rate_8pct:.1f}%)",
            "Movers >=10% Caught": f"{caught_10pct}/{len(top_movers_10pct)} ({cap_rate_10pct:.1f}%)",
            "Win Rate (>=3%)": f"{win_rate_3:.1f}%",
            "Win Rate (>=5%)": f"{win_rate_5:.1f}%",
            "Loss Rate (<-2%)": f"{loss_rate:.1f}%",
            "Avg 2D Max Gain": f"{avg_gain:+.2f}%",
            "Avg 2D Max DD": f"{avg_dd:+.2f}%",
            "Avg 2D Net Ret": f"{avg_ret_2d:+.2f}%",
            "Profit Factor": f"{profit_factor:.2f}",
        })

    print("\n" + "="*120)
    print(f"{'MATHEMATICAL BACKTEST: STRATEGY COMPARISON ON AUGUST 2026':^120}")
    print("="*120)
    sum_df = pd.DataFrame(summary_rows)
    print(sum_df.to_string(index=False))

    # 7. Head-to-Head Timing & Entry Price Comparison on Top 20 Movers
    print("\n" + "="*120)
    print(f"{'HEAD-TO-HEAD WINNER CAPTURE & TIMING ANALYSIS (TOP 20 AUGUST MOVERS)':^120}")
    print("="*120)

    top_20_symbols = symbol_movers.head(20)["symbol"].to_list()
    
    h2h_rows = []
    base_sig_df = first_signals_by_strat["1. Baseline (1h Vol Surge > 2.5)"]
    cum3_sig_df = first_signals_by_strat["2. Cumulative 3H Volume (3h >= 1.5, 1h >= 1.1)"]
    cum2_sig_df = first_signals_by_strat["3. Cumulative 2H Volume + VWAP (2h >= 1.4, 1h >= 1.1, close > VWAP)"]
    dual_sig_df = first_signals_by_strat["5. Dual-Track Hybrid (Spike > 2.5 OR Steady 3H >= 1.5 + VWAP)"]

    for sym in top_20_symbols:
        mover_info = symbol_movers.filter(pl.col("symbol") == sym).to_dicts()[0]
        max_g = mover_info["max_gain_pct"]

        b_sub = base_sig_df.filter(pl.col("symbol") == sym).sort("time")
        c3_sub = cum3_sig_df.filter(pl.col("symbol") == sym).sort("time")
        c2_sub = cum2_sig_df.filter(pl.col("symbol") == sym).sort("time")
        d_sub = dual_sig_df.filter(pl.col("symbol") == sym).sort("time")

        b_entry = b_sub.head(1).to_dicts()[0] if len(b_sub) > 0 else None
        c3_entry = c3_sub.head(1).to_dicts()[0] if len(c3_sub) > 0 else None
        c2_entry = c2_sub.head(1).to_dicts()[0] if len(c2_sub) > 0 else None
        d_entry = d_sub.head(1).to_dicts()[0] if len(d_sub) > 0 else None

        b_str = f"{b_entry['time'].strftime('%m-%d %H:%M')} (₹{b_entry['close']:.1f})" if b_entry else "MISSED"
        c3_str = f"{c3_entry['time'].strftime('%m-%d %H:%M')} (₹{c3_entry['close']:.1f})" if c3_entry else "MISSED"
        c2_str = f"{c2_entry['time'].strftime('%m-%d %H:%M')} (₹{c2_entry['close']:.1f})" if c2_entry else "MISSED"
        d_str = f"{d_entry['time'].strftime('%m-%d %H:%M')} (₹{d_entry['close']:.1f})" if d_entry else "MISSED"

        # Calculate timing/price difference between Dual Track vs Baseline
        advantage = "-"
        if d_entry and b_entry:
            if d_entry["time"] < b_entry["time"]:
                hrs_diff = (b_entry["time"] - d_entry["time"]).total_seconds() / 3600
                px_diff = ((b_entry["close"] - d_entry["close"]) / d_entry["close"]) * 100
                advantage = f"+{hrs_diff:.0f}h Earlier (-{px_diff:.1f}% cheaper)"
            elif d_entry["time"] == b_entry["time"]:
                advantage = "Same Time"
            else:
                advantage = "Later"
        elif d_entry and not b_entry:
            advantage = "NEW CAPTURE (Baseline Missed)"
        elif not d_entry and b_entry:
            advantage = "Baseline Only"

        h2h_rows.append({
            "Symbol": sym,
            "Aug Peak Gain": f"+{max_g:.1f}%",
            "Baseline Entry": b_str,
            "Cumul 3H Entry": c3_str,
            "Cumul 2H Entry": c2_str,
            "Dual-Track Entry": d_str,
            "Expansion Advantage": advantage,
        })

    h2h_df = pd.DataFrame(h2h_rows)
    print(h2h_df.to_string(index=False))

if __name__ == "__main__":
    run_full_analysis()
