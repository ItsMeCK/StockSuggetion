#!/usr/bin/env python3
"""
scripts/test_compound_hypothesis.py

Formal Pre-Registered Statistical Test: Compound Hypothesis for Progressive Builder
Stage 1 (Coil/Readiness Screen) + Stage 2 (15m/Hourly Ignition Trigger)

Pre-Registered Multiple Testing Family (7 Hypotheses):
  H1: Stage 1 (BBW <= 10th pct, Close > EMA50, Ext <= 1.5%) + Stage 2 (Close > PrevHigh, Vol >= 1.8x) vs Baseline
  H2: Stage 1 (BBW <= 10th pct, Close > EMA50, Ext <= 1.5%) + Stage 2 (Close > PrevHigh, Vol >= 1.5x) vs Baseline
  H3: Stage 1 (BBW <= 20th pct, Close > EMA50, Ext <= 1.5%) + Stage 2 (Close > PrevHigh, Vol >= 1.8x) vs Baseline
  H4: Stage 1 (BBW <= 20th pct, Close > EMA50, Ext <= 1.5%) + Stage 2 (Close > PrevHigh, Vol >= 1.5x) vs Baseline
  H5 (Ablation): Stage 2 Ignition Only (Vol >= 1.8x, no coil requirement) vs Baseline
  H6 (Ablation): Stage 2 Ignition Only (Vol >= 1.5x, no coil requirement) vs Baseline
  H7 (Ablation): Stage 1 Coil Only (BBW <= 10th pct, no breakout requirement) vs Baseline

Multiple Testing Correction:
  Benjamini-Hochberg (BH) False Discovery Rate (FDR) control at alpha = 0.05 across all 7 tests.

Statistical Rigor:
  - Welch's two-sample t-test (unequal variances)
  - Cohen's d effect size
  - 10,000-iteration percentile bootstrap 95% Confidence Interval for mean difference
"""

import os
import polars as pl
import numpy as np
from scipy import stats


def calculate_cohens_d(group1: np.ndarray, group2: np.ndarray) -> float:
    """Computes Cohen's d effect size between two groups."""
    n1, n2 = len(group1), len(group2)
    s1, s2 = np.var(group1, ddof=1), np.var(group2, ddof=1)
    s_pooled = np.sqrt(((n1 - 1) * s1 + (n2 - 1) * s2) / (n1 + n2 - 2))
    if s_pooled == 0:
        return 0.0
    return float((np.mean(group1) - np.mean(group2)) / s_pooled)


def bootstrap_mean_diff_ci(group1: np.ndarray, group2: np.ndarray, n_boot: int = 10000, seed: int = 42) -> tuple:
    """Computes 95% bootstrap confidence interval for difference in means."""
    rng = np.random.default_rng(seed)
    boot1 = rng.choice(group1, size=(n_boot, len(group1)), replace=True).mean(axis=1)
    boot2 = rng.choice(group2, size=(n_boot, len(group2)), replace=True).mean(axis=1)
    diffs = boot1 - boot2
    ci_lower = float(np.percentile(diffs, 2.5))
    ci_upper = float(np.percentile(diffs, 97.5))
    return ci_lower, ci_upper


def benjamini_hochberg(p_values: list) -> list:
    """Computes Benjamini-Hochberg FDR-adjusted p-values."""
    m = len(p_values)
    sorted_indices = np.argsort(p_values)
    sorted_p = np.array(p_values)[sorted_indices]
    
    adjusted = np.zeros(m)
    for i in range(m):
        adjusted[i] = sorted_p[i] * m / (i + 1)
    
    # Enforce monotonicity: p_adj[i] = min(p_adj[i], min_{k > i} p_adj[k])
    for i in range(m - 2, -1, -1):
        adjusted[i] = min(adjusted[i], adjusted[i + 1])
    
    adjusted = np.clip(adjusted, 0.0, 1.0)
    
    # Restore original order
    rev_indices = np.argsort(sorted_indices)
    return list(adjusted[rev_indices])


def run_compound_hypothesis_test():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"❌ Error: {parquet_path} not found.")
        return

    print("================================================================================")
    print("🔬 PRE-REGISTERED COMPOUND HYPOTHESIS TEST: READINESS COIL + IGNITION")
    print("================================================================================")

    df = pl.read_parquet(parquet_path)
    df = df.sort(["symbol", "time"])

    # Feature Engineering
    # 1. 20-bar Bollinger Band Width (BBW)
    # 2. 50-bar SMA
    # 3. 20-bar Volume SMA
    # 4. Day open price & extension
    # 5. Forward return (t+1 close vs t close)
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("close").rolling_mean(window_size=50).over("symbol").alias("sma_50"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20"),
        pl.col("high").shift(1).over("symbol").alias("prev_high"),
        pl.col("close").shift(-1).over("symbol").alias("next_close"),
        pl.col("high").shift(-1).over("symbol").alias("next_high"),
        pl.col("time").dt.date().alias("trade_date")
    ])

    df = df.with_columns([
        (4.0 * pl.col("std_20") / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_sma_20")).alias("vol_surge"),
        ((pl.col("next_close") - pl.col("close")) / pl.col("close") * 100.0).alias("forward_return"),
        ((pl.col("next_high") - pl.col("close")) / pl.col("close") * 100.0).alias("forward_peak")
    ])

    # Day Open price for extension calculation
    df_day_open = df.group_by(["symbol", "trade_date"]).agg(
        pl.col("open").first().alias("day_open")
    )
    df = df.join(df_day_open, on=["symbol", "trade_date"])
    df = df.with_columns([
        ((pl.col("close") - pl.col("day_open")) / pl.col("day_open") * 100.0).alias("day_extension_pct")
    ])

    # 100-bar rolling percentile rank of BBW per symbol
    # Compute using polars window expressions
    # To compute rolling percentile accurately:
    # rank of current bbw within trailing 100 bars / 100
    df = df.with_columns([
        pl.col("bbw").rolling_min(window_size=100).over("symbol").alias("bbw_min_100"),
        pl.col("bbw").rolling_max(window_size=100).over("symbol").alias("bbw_max_100")
    ])

    # Normalize BBW position within 100-bar range as proxy for percentile: (bbw - min) / (max - min)
    df = df.with_columns([
        pl.when(pl.col("bbw_max_100") > pl.col("bbw_min_100"))
        .then((pl.col("bbw") - pl.col("bbw_min_100")) / (pl.col("bbw_max_100") - pl.col("bbw_min_100")) * 100.0)
        .otherwise(50.0)
        .alias("bbw_pct_rank")
    ])

    # Filter valid rows (warmed up indicators and valid forward return)
    valid_df = df.filter(
        pl.col("sma_50").is_not_null() &
        pl.col("bbw_pct_rank").is_not_null() &
        pl.col("forward_return").is_not_null() &
        pl.col("vol_surge").is_not_null() &
        pl.col("prev_high").is_not_null()
    )

    baseline_returns = valid_df["forward_return"].to_numpy()
    baseline_peaks = valid_df["forward_peak"].to_numpy()
    n_baseline = len(baseline_returns)

    print(f"Total evaluated candidate bars in universe: {n_baseline:,}")
    print(f"Unfiltered Baseline Forward Return Mean: {np.mean(baseline_returns):+.4f}% (Std: {np.std(baseline_returns):.4f}%)")
    print(f"Unfiltered Baseline Peak Thrust Mean: {np.mean(baseline_peaks):+.4f}%\n")

    # Define the 7 Pre-Registered Tests
    test_specs = [
        {
            "id": "H1",
            "name": "Compound Ultra-Tight (BBW<=10%, Vol>=1.8x)",
            "filter": (
                (pl.col("bbw_pct_rank") <= 10.0) &
                (pl.col("close") > pl.col("sma_50")) &
                (pl.col("day_extension_pct") <= 1.5) &
                (pl.col("close") > pl.col("prev_high")) &
                (pl.col("vol_surge") >= 1.8)
            )
        },
        {
            "id": "H2",
            "name": "Compound Tight/ModVol (BBW<=10%, Vol>=1.5x)",
            "filter": (
                (pl.col("bbw_pct_rank") <= 10.0) &
                (pl.col("close") > pl.col("sma_50")) &
                (pl.col("day_extension_pct") <= 1.5) &
                (pl.col("close") > pl.col("prev_high")) &
                (pl.col("vol_surge") >= 1.5)
            )
        },
        {
            "id": "H3",
            "name": "Compound ModCoil/HighVol (BBW<=20%, Vol>=1.8x)",
            "filter": (
                (pl.col("bbw_pct_rank") <= 20.0) &
                (pl.col("close") > pl.col("sma_50")) &
                (pl.col("day_extension_pct") <= 1.5) &
                (pl.col("close") > pl.col("prev_high")) &
                (pl.col("vol_surge") >= 1.8)
            )
        },
        {
            "id": "H4",
            "name": "Compound ModCoil/ModVol (BBW<=20%, Vol>=1.5x)",
            "filter": (
                (pl.col("bbw_pct_rank") <= 20.0) &
                (pl.col("close") > pl.col("sma_50")) &
                (pl.col("day_extension_pct") <= 1.5) &
                (pl.col("close") > pl.col("prev_high")) &
                (pl.col("vol_surge") >= 1.5)
            )
        },
        {
            "id": "H5",
            "name": "Ablation: Ignition Only (Vol>=1.8x, No Coil)",
            "filter": (
                (pl.col("close") > pl.col("prev_high")) &
                (pl.col("vol_surge") >= 1.8)
            )
        },
        {
            "id": "H6",
            "name": "Ablation: Ignition Only (Vol>=1.5x, No Coil)",
            "filter": (
                (pl.col("close") > pl.col("prev_high")) &
                (pl.col("vol_surge") >= 1.5)
            )
        },
        {
            "id": "H7",
            "name": "Ablation: Coil Only (BBW<=10%, No Ignition)",
            "filter": (
                (pl.col("bbw_pct_rank") <= 10.0) &
                (pl.col("close") > pl.col("sma_50")) &
                (pl.col("day_extension_pct") <= 1.5)
            )
        }
    ]

    results = []
    raw_p_values = []

    for spec in test_specs:
        subset = valid_df.filter(spec["filter"])
        sample = subset["forward_return"].to_numpy()
        sample_peak = subset["forward_peak"].to_numpy()
        n = len(sample)

        if n < 5:
            print(f"⚠️ Warning: Insufficient samples for {spec['id']} (n={n})")
            continue

        mean_val = float(np.mean(sample))
        baseline_mean = float(np.mean(baseline_returns))
        mean_diff = mean_val - baseline_mean
        t_stat, p_val = stats.ttest_ind(sample, baseline_returns, equal_var=False)
        d = calculate_cohens_d(sample, baseline_returns)
        ci_low, ci_high = bootstrap_mean_diff_ci(sample, baseline_returns, n_boot=10000)
        win_rate = float(np.sum(sample > 0) / n * 100.0)
        peak_thrust = float(np.mean(sample_peak))

        raw_p_values.append(p_val)
        results.append({
            "id": spec["id"],
            "name": spec["name"],
            "n": n,
            "mean": mean_val,
            "mean_diff": mean_diff,
            "win_rate": win_rate,
            "peak_thrust": peak_thrust,
            "cohens_d": d,
            "ci_low": ci_low,
            "ci_high": ci_high,
            "p_raw": p_val
        })

    # Apply Benjamini-Hochberg FDR correction
    bh_p_values = benjamini_hochberg(raw_p_values)
    for r, p_bh in zip(results, bh_p_values):
        r["p_bh"] = p_bh
        r["significant"] = "✅ YES" if p_bh < 0.05 else "❌ NO"

    # Display Empirical Results
    print("📊 EMPIRICAL STATISTICAL TEST RESULTS (Family m=7, Benjamini-Hochberg FDR q=0.05):")
    print("-" * 140)
    print(f"{'Hypothesis':<6} | {'Description':<42} | {'n':>6} | {'Mean Ret':>9} | {'Diff vs Base':>12} | {'Cohen d':>8} | {'95% Bootstrap CI':>20} | {'p (raw)':>9} | {'p (BH)':>9} | {'Signif?':>7}")
    print("-" * 140)

    for r in results:
        ci_str = f"[{r['ci_low']:+.3f}%, {r['ci_high']:+.3f}%]"
        print(f"{r['id']:<6} | {r['name']:<42} | {r['n']:>6} | {r['mean']:>+8.3f}% | {r['mean_diff']:>+11.3f}% | {r['cohens_d']:>+7.3f} | {ci_str:>20} | {r['p_raw']:>9.4e} | {r['p_bh']:>9.4e} | {r['significant']:>7}")
    print("-" * 140)

    print("\n🔍 SCIENTIFIC INTERPRETATION & ACTIONABLE TAKEAWAY:")
    for r in results:
        status = "CONFIRMED STATISTICAL EDGE" if r["p_bh"] < 0.05 and r["mean_diff"] > 0 else "NO SURVIVING EDGE / INSUFFICIENT ADVANTAGE"
        print(f"• {r['id']} ({r['name']}): BH p = {r['p_bh']:.4e}, Diff = {r['mean_diff']:+.3f}%, Cohen's d = {r['cohens_d']:.3f} -> {status}")


if __name__ == "__main__":
    run_compound_hypothesis_test()
