"""
Adds a genuinely independent signal: cross-sectional Relative Strength rank
(the stock's 10-day return percentile-ranked against the WHOLE universe on
that day) - this is Minervini/IBD's core methodology (RS Rating), and it is
NOT just another cut of the same single-stock chart: ignition/breadth/close-
range all describe ONE stock's own price action, while RS-rank describes how
that stock compares to everyone else on the same day. Combining weakly-
correlated signals (ensemble) is how real multi-factor systems get from "55%
on any one factor" to a higher number on the intersection - at the cost of
fewer total trades.

Tests the triple-intersection: ignition (2d-high + volume + close-range) AND
market-breadth gate AND top-decile RS rank, at real sample sizes.
"""
import os
import datetime
import polars as pl
from dotenv import load_dotenv

load_dotenv()
import sys
sys.path.insert(0, "/Users/poonamsalke/Workplace/StockSuggetion")
os.chdir("/Users/poonamsalke/Workplace/StockSuggetion")

from scripts.gate1_widening_lab import (
    load_universe, load_and_compute, baseline_candidates, build_series_cache,
    signal_features_from_series,
)
from scripts.router_threshold_grid import load_breadth, collect_features
from scripts.strategy_lab import sim_option, sim_equity


def compute_rs_rank(df):
    """Cross-sectional percentile rank of roc_10 within each trading day."""
    ranked = df.filter(pl.col("roc_10").is_not_null()).with_columns(
        (pl.col("roc_10").rank(method="average").over("time") /
         pl.col("roc_10").count().over("time") * 100).alias("rs_percentile")
    )
    return {(r["symbol"], r["time"].strftime("%Y-%m-%d")): r["rs_percentile"]
            for r in ranked.select(["symbol", "time", "rs_percentile"]).to_dicts()}


def test_ensemble(feats, bmap, rs_map, vol_min, cr_min, breadth_min, rs_min, target_mult, hard_stop, label):
    pnls = []
    for f in feats:
        if bmap.get(f["date"], 0) < breadth_min:
            continue
        rs = rs_map.get((f["symbol"], f["date"]))
        if rs is None or rs < rs_min:
            continue
        if not (f["broke_2d_high"] and f["vol_ratio"] >= vol_min and f["close_range"] >= cr_min):
            continue
        p = sim_option({"series": f["series"], "idx": f["idx"]}, target_mult=target_mult, hard_stop=hard_stop)
        if p is not None:
            pnls.append(p)
    if not pnls:
        return 0, 0, 0
    wr = 100 * sum(1 for p in pnls if p > 0) / len(pnls)
    avg = sum(pnls) / len(pnls)
    print(f"  {label:90s} n={len(pnls):4d}  WR={wr:5.1f}%  avg={avg:+7.2f}%")
    return len(pnls), wr, avg


def test_ensemble_equity(feats, bmap, rs_map, ret_min, ret_max, vol_max, breadth_min, rs_min, target_pct, stop_pct, label):
    pnls = []
    for f in feats:
        if bmap.get(f["date"], 0) < breadth_min:
            continue
        rs = rs_map.get((f["symbol"], f["date"]))
        if rs is None or rs < rs_min:
            continue
        if not (ret_min <= f["sig_day_ret"] <= ret_max and f["vol_ratio"] < vol_max):
            continue
        p = sim_equity({"series": f["series"], "idx": f["idx"]}, target_pct=target_pct, stop_pct=stop_pct)
        if p is not None:
            pnls.append(p)
    if not pnls:
        return 0, 0, 0
    wr = 100 * sum(1 for p in pnls if p > 0) / len(pnls)
    avg = sum(pnls) / len(pnls)
    print(f"  {label:90s} n={len(pnls):4d}  WR={wr:5.1f}%  avg={avg:+7.2f}%")
    return len(pnls), wr, avg


def main():
    universe = load_universe()
    print("Loading + computing indicators...")
    df = load_and_compute(universe, "2026-04-15", "2026-07-02")
    window_df = df.filter((pl.col("time") >= datetime.date(2026, 5, 1)) & (pl.col("time") <= datetime.date(2026, 6, 30)))
    series_cache = build_series_cache(df)
    base = baseline_candidates(window_df)
    feats = collect_features(base, series_cache)
    print(f"Raw gate-1 candidates with usable features: {len(feats)}")

    bmap = load_breadth()
    print("Computing cross-sectional RS rank...")
    rs_map = compute_rs_rank(window_df)

    print("\n=== OPTIONS: ignition + breadth + RS-rank ensemble (n>=15 shown, prefer n>=25) ===")
    results = []
    for vol_min in (1.5, 2.0):
        for cr_min in (0.6, 0.7, 0.8):
            for breadth_min in (0, 60, 70):
                for rs_min in (0, 70, 80, 90):
                    for target_mult in (1.5, 2.0):
                        for hard_stop in (0.35, 0.50):
                            n, wr, avg = test_ensemble(feats, bmap, rs_map, vol_min, cr_min, breadth_min, rs_min,
                                                        target_mult, hard_stop,
                                                        f"vol>={vol_min} cr>={cr_min} breadth>={breadth_min} RS>={rs_min} T={target_mult}x S={hard_stop}")
                            if n >= 15:
                                results.append((wr, n, avg, vol_min, cr_min, breadth_min, rs_min, target_mult, hard_stop))

    print("\n--- Best OPTIONS ensemble configs (n>=15, sorted by WR then n) ---")
    for wr, n, avg, vol_min, cr_min, breadth_min, rs_min, target_mult, hard_stop in sorted(results, key=lambda x: (-x[0], -x[1]))[:15]:
        print(f"  WR={wr:5.1f}% n={n:4d} avg={avg:+7.2f}%  vol>={vol_min} cr>={cr_min} breadth>={breadth_min} RS>={rs_min} T={target_mult}x S={hard_stop}")

    print("\n=== EQUITY: continuation + breadth + RS-rank ensemble (n>=15 shown) ===")
    eq_results = []
    for ret_min, ret_max in [(0.5, 3.5), (1.0, 3.0), (0.0, 2.0)]:
        for vol_max in (1.5, 2.0, 999):
            for breadth_min in (0, 60):
                for rs_min in (0, 70, 80, 90):
                    for target_pct in (0.05, 0.10):
                        n, wr, avg = test_ensemble_equity(feats, bmap, rs_map, ret_min, ret_max, vol_max, breadth_min, rs_min,
                                                          target_pct, 0.05,
                                                          f"ret[{ret_min},{ret_max}] vol<{vol_max} breadth>={breadth_min} RS>={rs_min} T={target_pct}")
                        if n >= 15:
                            eq_results.append((wr, n, avg, ret_min, ret_max, vol_max, breadth_min, rs_min, target_pct))

    print("\n--- Best EQUITY ensemble configs (n>=15, sorted by WR then n) ---")
    for wr, n, avg, ret_min, ret_max, vol_max, breadth_min, rs_min, target_pct in sorted(eq_results, key=lambda x: (-x[0], -x[1]))[:15]:
        print(f"  WR={wr:5.1f}% n={n:4d} avg={avg:+7.2f}%  ret[{ret_min},{ret_max}] vol<{vol_max} breadth>={breadth_min} RS>={rs_min} T={target_pct}")


if __name__ == "__main__":
    main()
