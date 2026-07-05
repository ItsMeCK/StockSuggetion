"""
Grid-search the Conviction Router's classification thresholds against the FULL
raw gate-1 candidate pool (~1328 candidate-days over May+June), not just the
67 signals that survived the whole expensive agent pipeline. This gives a much
larger, more statistically credible sample (n=100-300 per bucket instead of
n=15-20) to calibrate thresholds without curve-fitting to noise.

Also tests the NIFTY-breadth macro gate (established finding: options-ignition
only has real edge when the broader market is participating) combined with
tighter/looser ignition-profile thresholds.
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
    signal_features_from_series, get_conn,
)
from scripts.strategy_lab import sim_option, sim_equity


def load_breadth():
    universe = load_universe()
    conn = get_conn()
    df = pl.read_database(
        "SELECT time::date as d, symbol, close FROM daily_ohlcv WHERE symbol = ANY(%(s)s) ORDER BY symbol, time",
        conn, execute_options={"parameters": {"s": list(universe)}})
    conn.close()
    df = df.with_columns(pl.col("close").rolling_mean(20).over("symbol").alias("sma20"))
    df = df.with_columns((pl.col("close") > pl.col("sma20")).alias("above"))
    breadth = (df.filter(pl.col("sma20").is_not_null())
                 .group_by("d").agg((pl.col("above").mean() * 100).alias("pct_above")))
    return {r["d"].strftime("%Y-%m-%d"): r["pct_above"] for r in breadth.to_dicts()}


def collect_features(cand_df, series_cache):
    """One row per (symbol, date) candidate with its signal-day features."""
    out = []
    for row in cand_df.to_dicts():
        sym, date = row["symbol"], row["time"].strftime("%Y-%m-%d")
        series = series_cache.get(sym)
        if not series:
            continue
        dl = [b["d"] for b in series]
        if date not in dl:
            continue
        idx = dl.index(date)
        f = signal_features_from_series(series, idx)
        if not f:
            continue
        f["symbol"], f["date"], f["series"], f["idx"] = sym, date, series, idx
        out.append(f)
    return out


def test_ignition(feats, bmap, vol_min, cr_min, breadth_min, target_mult, hard_stop, green_only, label):
    pnls = []
    for f in feats:
        if bmap.get(f["date"], 0) < breadth_min:
            continue
        if not (f["broke_2d_high"] and f["vol_ratio"] >= vol_min and f["close_range"] >= cr_min):
            continue
        if green_only and f["signal_close"] < f["series"][f["idx"]]["o"]:
            continue
        p = sim_option({"series": f["series"], "idx": f["idx"]}, target_mult=target_mult, hard_stop=hard_stop)
        if p is not None:
            pnls.append(p)
    if not pnls:
        print(f"  {label:80s} n=0")
        return 0, 0, 0
    wr = 100 * sum(1 for p in pnls if p > 0) / len(pnls)
    avg = sum(pnls) / len(pnls)
    print(f"  {label:80s} n={len(pnls):4d}  WR={wr:5.1f}%  avg={avg:+7.2f}%")
    return len(pnls), wr, avg


def test_continuation(feats, bmap, ret_min, ret_max, vol_max, breadth_min, target_pct, stop_pct, label):
    pnls = []
    for f in feats:
        if bmap.get(f["date"], 0) < breadth_min:
            continue
        if not (ret_min <= f["sig_day_ret"] <= ret_max and f["vol_ratio"] < vol_max):
            continue
        p = sim_equity({"series": f["series"], "idx": f["idx"]}, target_pct=target_pct, stop_pct=stop_pct)
        if p is not None:
            pnls.append(p)
    if not pnls:
        print(f"  {label:80s} n=0")
        return 0, 0, 0
    wr = 100 * sum(1 for p in pnls if p > 0) / len(pnls)
    avg = sum(pnls) / len(pnls)
    print(f"  {label:80s} n={len(pnls):4d}  WR={wr:5.1f}%  avg={avg:+7.2f}%")
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

    print("\n=== OPTIONS IGNITION grid (n must stay >=30 to trust the number) ===")
    results = []
    for vol_min in (1.5, 2.0, 2.5):
        for cr_min in (0.6, 0.7, 0.8):
            for breadth_min in (0, 50, 60, 70):
                for target_mult in (1.5, 2.0):
                    for hard_stop in (0.35, 0.50):
                        n, wr, avg = test_ignition(feats, bmap, vol_min, cr_min, breadth_min, target_mult, hard_stop, False,
                                                    f"vol>={vol_min} cr>={cr_min} breadth>={breadth_min} target={target_mult}x stop={hard_stop}")
                        if n >= 30:
                            results.append((wr, n, avg, vol_min, cr_min, breadth_min, target_mult, hard_stop))

    print("\n--- Best OPTIONS configs with n>=30 (sorted by WR) ---")
    for wr, n, avg, vol_min, cr_min, breadth_min, target_mult, hard_stop in sorted(results, reverse=True)[:10]:
        print(f"  WR={wr:5.1f}% n={n:4d} avg={avg:+6.2f}%  vol>={vol_min} cr>={cr_min} breadth>={breadth_min} target={target_mult}x stop={hard_stop}")

    print("\n=== EQUITY CONTINUATION grid (n must stay >=30) ===")
    eq_results = []
    for ret_min, ret_max in [(0.5, 3.5), (1.0, 3.0), (0.0, 2.0), (1.0, 2.0), (-1.0, 1.0)]:
        for vol_max in (1.5, 2.0, 2.5, 999):
            for breadth_min in (0, 50, 60):
                for target_pct in (0.05, 0.10):
                    for stop_pct in (0.03, 0.05):
                        n, wr, avg = test_continuation(feats, bmap, ret_min, ret_max, vol_max, breadth_min, target_pct, stop_pct,
                                                       f"ret[{ret_min},{ret_max}] vol<{vol_max} breadth>={breadth_min} T={target_pct} S={stop_pct}")
                        if n >= 30:
                            eq_results.append((wr, n, avg, ret_min, ret_max, vol_max, breadth_min, target_pct, stop_pct))

    print("\n--- Best EQUITY configs with n>=30 (sorted by WR) ---")
    for wr, n, avg, ret_min, ret_max, vol_max, breadth_min, target_pct, stop_pct in sorted(eq_results, reverse=True)[:10]:
        print(f"  WR={wr:5.1f}% n={n:4d} avg={avg:+6.2f}%  ret[{ret_min},{ret_max}] vol<{vol_max} breadth>={breadth_min} T={target_pct} S={stop_pct}")


if __name__ == "__main__":
    main()
