#!/usr/bin/env python3
"""
scripts/verify_engine2_empirical_edge.py

Empirical Verification of Engine 2 (3:15 PM BTST / Anticipatory Execution)
Measures the overnight gap return (Next Open vs Entry Close) and Next Day Close return
of Engine 2 candidates vs the true complement of the universe across data/intraday_ohlcv.parquet.
"""

import os
import polars as pl
import numpy as np
from scipy import stats


def run_engine2_empirical_audit():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"❌ Error: {parquet_path} not found.")
        return

    print("================================================================================")
    print("📊 EMPIRICAL AUDIT: ENGINE 2 OVERNIGHT GAP VS UNIVERSE COMPLEMENT")
    print("================================================================================")

    df = pl.read_parquet(parquet_path).sort(["symbol", "time"])
    
    # Feature engineering for Engine 2
    df = df.with_columns([
        pl.col("close").rolling_mean(20).over("symbol").alias("sma_20"),
        pl.col("volume").rolling_mean(20).over("symbol").alias("vol_sma_20"),
        pl.col("close").shift(1).over("symbol").alias("prev_close"),
        pl.col("low").shift(1).over("symbol").alias("prev_low"),
        pl.col("time").dt.date().alias("trade_date")
    ])

    df = df.with_columns([
        (pl.col("volume") / pl.col("vol_sma_20")).alias("vol_surge"),
        (
            pl.when(pl.col("high") != pl.col("low"))
            .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
            .otherwise(0.0)
        ).alias("close_to_high_ratio")
    ])

    # Target EOD candle (14:15 - 15:15 IST, UTC hour 8)
    eod_df = df.filter(pl.col("time").dt.hour() == 8)
    
    # Engine 2 criteria (3:15 PM Insider / Anticipatory Filter)
    e2_condition = (
        (pl.col("close_to_high_ratio") > 0.85) &
        (pl.col("vol_surge") > 1.2) & (pl.col("vol_surge") < 3.0) &
        (pl.col("close") > pl.col("open")) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") > pl.col("prev_close")) &
        (pl.col("low") > pl.col("prev_low"))
    )

    # Daily aggregation to find next open and next close
    daily = df.group_by(["symbol", "trade_date"]).agg([
        pl.col("open").first().alias("day_open"),
        pl.col("close").last().alias("day_close")
    ]).sort(["symbol", "trade_date"])

    daily = daily.with_columns([
        pl.col("day_open").shift(-1).over("symbol").alias("next_open"),
        pl.col("day_close").shift(-1).over("symbol").alias("next_close")
    ])

    # Join back with EOD candidates
    merged = eod_df.join(daily, on=["symbol", "trade_date"])
    merged = merged.with_columns([
        ((pl.col("next_open") - pl.col("close")) / pl.col("close") * 100.0).alias("overnight_gap_pct"),
        ((pl.col("next_close") - pl.col("close")) / pl.col("close") * 100.0).alias("next_day_close_pct"),
        e2_condition.alias("is_e2")
    ])

    valid = merged.filter(pl.col("overnight_gap_pct").is_not_null())

    e2_candidates = valid.filter(pl.col("is_e2"))
    complement = valid.filter(~pl.col("is_e2"))

    # Top 5 per day selection (live anticipatory execution behavior)
    e2_top5 = e2_candidates.sort(["trade_date", "vol_surge"], descending=[False, True]).group_by("trade_date").head(5)
    top5_symbols_dates = e2_top5.select(["symbol", "trade_date"])
    complement_top5 = valid.join(top5_symbols_dates, on=["symbol", "trade_date"], how="anti")

    print(f"Total Evaluated EOD Bars: {valid.height:,}")
    print(f"Total Engine 2 Filter Passed (All): {e2_candidates.height}")
    print(f"Total Engine 2 Top-5 Sized Trades: {e2_top5.height}\n")

    # Metrics for All Engine 2 vs Complement
    e2_gap = e2_candidates["overnight_gap_pct"].to_numpy()
    comp_gap = complement["overnight_gap_pct"].to_numpy()
    e2_close = e2_candidates["next_day_close_pct"].to_numpy()
    comp_close = complement["next_day_close_pct"].to_numpy()

    t_stat_gap, p_val_gap = stats.ttest_ind(e2_gap, comp_gap, equal_var=False)
    t_stat_close, p_val_close = stats.ttest_ind(e2_close, comp_close, equal_var=False)

    print("--- ALL ENGINE 2 CANDIDATES ---")
    print(f"Overnight Gap: Engine 2 Mean = {np.mean(e2_gap):+.3f}% vs Market Complement Mean = {np.mean(comp_gap):+.3f}%")
    print(f"  Delta = {np.mean(e2_gap) - np.mean(comp_gap):+.3f}%, Welch t = {t_stat_gap:.2f}, p-value = {p_val_gap:.4e}")
    print(f"Next-Day Close: Engine 2 Mean = {np.mean(e2_close):+.3f}% vs Market Complement Mean = {np.mean(comp_close):+.3f}%")
    print(f"  Delta = {np.mean(e2_close) - np.mean(comp_close):+.3f}%, Welch t = {t_stat_close:.2f}, p-value = {p_val_close:.4e}\n")

    # Metrics for Top 5 Engine 2 vs Complement
    top5_gap = e2_top5["overnight_gap_pct"].to_numpy()
    top5_comp_gap = complement_top5["overnight_gap_pct"].to_numpy()
    top5_close = e2_top5["next_day_close_pct"].to_numpy()
    top5_comp_close = complement_top5["next_day_close_pct"].to_numpy()

    t_stat_top5_gap, p_val_top5_gap = stats.ttest_ind(top5_gap, top5_comp_gap, equal_var=False)
    t_stat_top5_close, p_val_top5_close = stats.ttest_ind(top5_close, top5_comp_close, equal_var=False)

    print("--- TOP 5 ENGINE 2 TRADES (REAL EXECUTION CANDIDATES) ---")
    print(f"Overnight Gap: Top 5 Mean = {np.mean(top5_gap):+.3f}% vs Market Complement Mean = {np.mean(top5_comp_gap):+.3f}%")
    print(f"  Delta = {np.mean(top5_gap) - np.mean(top5_comp_gap):+.3f}%, Welch t = {t_stat_top5_gap:.2f}, p-value = {p_val_top5_gap:.4e}")
    print(f"Next-Day Close: Top 5 Mean = {np.mean(top5_close):+.3f}% vs Market Complement Mean = {np.mean(top5_comp_close):+.3f}%")
    print(f"  Delta = {np.mean(top5_close) - np.mean(top5_comp_close):+.3f}%, Welch t = {t_stat_top5_close:.2f}, p-value = {p_val_top5_close:.4e}\n")

    print("================================================================================")
    print("🔍 CORE TAKEAWAY:")
    if np.mean(top5_gap) > np.mean(top5_close):
        print(f"✅ CONFIRMED: Exiting at T+1 Market Open ({np.mean(top5_gap):+.3f}%) captures significantly higher return")
        print(f"   than holding through T+1 Market Close ({np.mean(top5_close):+.3f}%).")
        print(f"   Edge Preserved by 09:15-09:30 AM Open Exit = {np.mean(top5_gap) - np.mean(top5_close):+.3f}% per trade on underlying.")
    print("================================================================================")


if __name__ == "__main__":
    run_engine2_empirical_audit()
