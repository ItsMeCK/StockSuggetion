"""
Gate-1 widening lab: vectorized (polars) test of screener rule variants across
the FULL 40-trading-day window (May + June), measuring actual TRADE win rate
(via the Conviction Router's ignition/continuation classification + the same
Black-Scholes option / equity simulators used everywhere else) - not just raw
mover-capture counts. This matters because catching more 5%+ movers is only
useful if the captured candidates are also good TRADES.

Financial rationale for the widened rule (not arbitrary threshold-hacking):
  Shannon Stage Analysis treats "Stage 1 -> Stage 2" transition as a valid,
  named entry regime (institutional accumulation resolving into markup), not
  just "must already be above the 50-SMA". The current screener requires
  close > sma_50 as a hard AND on every path, which excludes genuine early
  transitions. We test relaxing that specifically for the case where volume
  provides institutional confirmation (thrust >= 2x average) - i.e. the same
  compensating rigor already used elsewhere in this codebase for "Institutional
  Rebirth", just extended to fire slightly before the 50-SMA cross instead of
  only after it.

No app code imported for the scan itself (pure polars filters mirroring
pipeline/screener.py's logic) - only the Conviction Router's pure functions
and the trade simulators are reused, since those are shared, tested logic.
"""
import os
import csv
import json
import datetime
import psycopg2
import polars as pl
from dotenv import load_dotenv

load_dotenv()
import sys
sys.path.insert(0, "/Users/poonamsalke/Workplace/StockSuggetion")
os.chdir("/Users/poonamsalke/Workplace/StockSuggetion")

from agents.conviction_router_agent import classify_route
from scripts.strategy_lab import sim_option, sim_equity


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"))


def load_universe():
    syms = set()
    with open("pipeline/master_universe.csv") as f:
        for row in csv.DictReader(f):
            syms.add(row["Symbol"])
    return syms


def load_and_compute(universe, start, end):
    conn = get_conn()
    df = pl.read_database(
        "SELECT time::date as time, symbol, open, high, low, close, volume FROM daily_ohlcv "
        "WHERE symbol = ANY(%(s)s) ORDER BY symbol, time",
        conn, execute_options={"parameters": {"s": list(universe)}})
    conn.close()

    df = df.with_columns([
        pl.col("close").rolling_mean(10).over("symbol").alias("sma_10"),
        pl.col("close").rolling_mean(20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_mean(50).over("symbol").alias("sma_50"),
        pl.col("close").rolling_mean(200).over("symbol").alias("sma_200"),
        pl.col("close").rolling_mean(100).over("symbol").alias("sma_100"),
        pl.col("volume").rolling_mean(20).over("symbol").alias("vol_avg_20"),
    ])
    df = df.with_columns([pl.col("sma_200").fill_null(pl.col("sma_100")).fill_null(pl.col("sma_50"))])
    df = df.with_columns([
        (pl.col("sma_50") - pl.col("sma_50").shift(10).over("symbol")).alias("sma_50_slope_10d"),
        (((pl.col("close") - pl.col("sma_50")) / pl.col("sma_50")) * 100).alias("extension_pct"),
        (((pl.col("close") - pl.col("close").shift(10).over("symbol")) / pl.col("close").shift(10).over("symbol")) * 100).alias("roc_10"),
        (((pl.col("close") - pl.col("close").shift(20).over("symbol")) / pl.col("close").shift(20).over("symbol")) * 100).alias("roc_20"),
    ])
    start_d = datetime.date.fromisoformat(start) if isinstance(start, str) else start
    end_d = datetime.date.fromisoformat(end) if isinstance(end, str) else end
    return df.filter((pl.col("time") >= start_d) & (pl.col("time") <= end_d) & pl.col("sma_200").is_not_null())


def baseline_candidates(df):
    """Mirrors pipeline/screener.py stage_2_df / flagged_momentum_df (relaxed_window=True)."""
    stage2 = df.filter(
        (pl.col("close") > pl.col("sma_50")) &
        ((pl.col("sma_50") >= pl.col("sma_200")) | (pl.col("volume") > 2.0 * pl.col("vol_avg_20"))) &
        (pl.col("sma_50_slope_10d") > -50) &
        ((pl.col("extension_pct") <= 5.0) |
         ((pl.col("extension_pct") <= 20.0) & (pl.col("roc_10") > pl.col("roc_20")) & (pl.col("volume") > 1.5 * pl.col("vol_avg_20")))) &
        (pl.col("sma_10") > pl.col("sma_20")) &
        ((pl.col("volume") >= 1.2 * pl.col("vol_avg_20")) | (pl.col("volume") <= 0.8 * pl.col("vol_avg_20")))
    )
    flagged = df.filter(
        (pl.col("close") > pl.col("sma_50")) &
        (((pl.col("extension_pct") > 15.0) & (pl.col("extension_pct") <= 25.0) & (pl.col("roc_10") > 5.0) & (pl.col("volume") >= 2.0 * pl.col("vol_avg_20"))))
    )
    return pl.concat([stage2.select(["symbol", "time"]), flagged.select(["symbol", "time"])]).unique()


def widened_candidates(df, below_50sma_tolerance=3.0, min_thrust=2.0):
    """Adds a 'Stage 1->2 Ignition' pattern: within tolerance% below the 50-SMA
    (not yet crossed), accelerating momentum, and strong volume confirmation."""
    ignition = df.filter(
        (pl.col("close") <= pl.col("sma_50")) &
        (pl.col("close") >= pl.col("sma_50") * (1 - below_50sma_tolerance / 100.0)) &
        (pl.col("roc_10") > pl.col("roc_20")) &
        (pl.col("roc_10") > 0) &
        (pl.col("volume") >= min_thrust * pl.col("vol_avg_20")) &
        (pl.col("sma_10") > pl.col("sma_20"))
    )
    base = baseline_candidates(df)
    new = ignition.select(["symbol", "time"]).unique()
    return pl.concat([base, new]).unique(), new


def build_series_cache(df):
    """Uses the short o/h/l/c/v keys expected by scripts/strategy_lab.py's
    sim_equity / sim_option (shared, tested trade simulators)."""
    cache = {}
    for sym_df in df.partition_by("symbol"):
        sym = sym_df["symbol"][0]
        rows = sym_df.sort("time").to_dicts()
        cache[sym] = [{"d": r["time"].strftime("%Y-%m-%d"), "o": r["open"], "h": r["high"],
                       "l": r["low"], "c": r["close"], "v": r["volume"]} for r in rows]
    return cache


def signal_features_from_series(series, idx):
    if idx < 2 or idx + 1 >= len(series):
        return None
    sig = series[idx]
    vol20 = sum(b["v"] for b in series[max(0, idx - 20):idx]) / max(1, min(20, idx))
    rng = sig["h"] - sig["l"]
    prev_c = series[idx - 1]["c"]
    return {
        "sig_day_ret": (sig["c"] - prev_c) / prev_c * 100 if prev_c else 0,
        "vol_ratio": sig["v"] / vol20 if vol20 else 1.0,
        "close_range": (sig["c"] - sig["l"]) / rng if rng > 0 else 1.0,
        "broke_2d_high": sig["c"] >= max(b["c"] for b in series[idx - 2:idx]),
        "signal_close": sig["c"],
        "entry_idx": idx + 1,
    }


def evaluate_candidates(cand_df, series_cache, label):
    results = []
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
        route = classify_route(f)
        if route == "NO_EDGE":
            continue
        f_lab = {"broke_2d_high": f["broke_2d_high"], "vol_ratio": f["vol_ratio"],
                 "close_range": f["close_range"], "series": series, "idx": idx}
        if route == "OPTIONS_IGNITION":
            pnl = sim_option(f_lab)
        else:
            pnl = sim_equity(f_lab)
        if pnl is not None:
            results.append({"symbol": sym, "date": date, "route": route, "pnl": pnl})

    n = len(results)
    if n == 0:
        print(f"{label:60s} n=0")
        return results
    wins = sum(1 for r in results if r["pnl"] > 0)
    wr = 100 * wins / n
    avg = sum(r["pnl"] for r in results) / n
    print(f"{label:60s} n={n:3d}  WR={wr:5.1f}%  avg={avg:+7.2f}%")
    for route in ("OPTIONS_IGNITION", "EQUITY_CONTINUATION"):
        rr = [r for r in results if r["route"] == route]
        if rr:
            w = 100 * sum(1 for r in rr if r["pnl"] > 0) / len(rr)
            print(f"    {route:22s} n={len(rr):3d}  WR={w:5.1f}%  avg={sum(r['pnl'] for r in rr)/len(rr):+7.2f}%")
    return results


def main():
    universe = load_universe()
    print(f"Universe: {len(universe)} symbols. Loading + computing indicators...")
    df = load_and_compute(universe, "2026-04-15", "2026-07-02")  # extra buffer before window for rolling calcs
    window_df = df.filter((pl.col("time") >= datetime.date(2026, 5, 1)) & (pl.col("time") <= datetime.date(2026, 6, 30)))
    print(f"Rows in window: {len(window_df)}")

    series_cache = build_series_cache(df)

    base = baseline_candidates(window_df)
    print(f"\nBaseline gate-1 candidates in window (all days, not just signal days): {len(base)}")
    evaluate_candidates(base, series_cache, "BASELINE gate-1 -> router -> trade sim (all days)")

    print("\n--- Widened variants (Stage 1->2 Ignition addition) ---")
    for tol, thrust in [(2.0, 2.5), (3.0, 2.0), (3.0, 2.5), (5.0, 2.0), (5.0, 3.0)]:
        all_cands, new_only = widened_candidates(window_df, tol, thrust)
        print(f"\n[tolerance={tol}%, min_thrust={thrust}x]  new candidates added: {len(new_only)}")
        evaluate_candidates(new_only, series_cache, f"  NEW candidates ONLY (tol={tol}%,thrust={thrust}x)")
        evaluate_candidates(all_cands, series_cache, f"  ALL candidates (baseline+new, tol={tol}%,thrust={thrust}x)")


if __name__ == "__main__":
    main()
