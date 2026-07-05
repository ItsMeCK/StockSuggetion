"""
Bearish mirror of the bullish ignition/router logic: detects breakdown setups
(Stage 4 decline / 2-day-low break with volume thrust) and simulates LONG PUT
trades (capital-efficient: buy only, pay premium, no margin - directly
answers the user's capital objection to credit spreads) using the same
Black-Scholes proxy methodology already used for the CE side.

Key question this answers: does a long-PE breakdown book rescue the May
corrective regime that killed the CE-only book (41.9% WR)? If bullish
ignition needs HIGH breadth (market participating) to work, bearish
breakdown should need LOW breadth (market NOT participating / risk-off) to
work - i.e. the two books should be regime-complementary, not
regime-correlated, which is the whole point of going bidirectional.

Pure math + real screener-style filters, no LLM. Runs in ~1-2 minutes.
"""
import os
import math
import datetime
import csv
import psycopg2
import polars as pl
from dotenv import load_dotenv

load_dotenv()
import sys
sys.path.insert(0, "/Users/poonamsalke/Workplace/StockSuggetion")
os.chdir("/Users/poonamsalke/Workplace/StockSuggetion")

from scripts.backtest_3pm_options_vs_equity import (
    norm_cdf, nearest_strike, estimate_iv, RISK_FREE_RATE, EXPIRY_DAYS,
    TARGET_MULT, CUSHION, TRAIL_GIVEBACK, HARD_STOP_MULT,
)


def bs_put_price(S, K, t_years, r, sigma):
    """Black-Scholes put via put-call parity relationship, same inputs as bs_call_price."""
    if t_years <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return max(K - S, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * t_years) / (sigma * math.sqrt(t_years))
    d2 = d1 - sigma * math.sqrt(t_years)
    return K * math.exp(-r * t_years) * norm_cdf(-d2) - S * norm_cdf(-d1)


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
        pl.col("volume").rolling_mean(20).over("symbol").alias("vol_avg_20"),
    ])
    df = df.with_columns([
        (pl.col("sma_50") - pl.col("sma_50").shift(10).over("symbol")).alias("sma_50_slope_10d"),
        (((pl.col("close") - pl.col("sma_50")) / pl.col("sma_50")) * 100).alias("extension_pct"),
    ])
    start_d = datetime.date.fromisoformat(start)
    end_d = datetime.date.fromisoformat(end)
    return df.filter((pl.col("time") >= start_d) & (pl.col("time") <= end_d) & pl.col("sma_200").is_not_null())


def breakdown_candidates(df):
    """Mirror of stage_2/flagged_momentum, inverted for breakdowns:
    Stage 4 decline (close<50sma<200sma, negative slope) with a 2-day-low
    break + volume thrust (distribution, not accumulation)."""
    stage4 = df.filter(
        (pl.col("close") < pl.col("sma_50")) &
        (pl.col("sma_50") < pl.col("sma_200")) &
        (pl.col("sma_50_slope_10d") < 0) &
        (pl.col("sma_10") < pl.col("sma_20")) &
        (pl.col("volume") >= 1.5 * pl.col("vol_avg_20"))
    )
    breakdown_ignition = df.filter(
        (pl.col("close") < pl.col("sma_50")) &
        (pl.col("extension_pct") < -5.0) & (pl.col("extension_pct") > -25.0) &
        (pl.col("volume") >= 2.0 * pl.col("vol_avg_20"))
    )
    return pl.concat([stage4.select(["symbol", "time"]), breakdown_ignition.select(["symbol", "time"])]).unique()


def compute_breadth(df):
    b = (df.filter(pl.col("sma_200").is_not_null())
           .with_columns((pl.col("close") > pl.col("sma_200")).alias("above200"))
           .group_by("time").agg((pl.col("above200").mean() * 100).alias("breadth200")))
    return {r["time"].strftime("%Y-%m-%d"): r["breadth200"] for r in b.to_dicts()}


def build_series_cache(df):
    cache = {}
    for sym_df in df.partition_by("symbol"):
        sym = sym_df["symbol"][0]
        rows = sym_df.sort("time").to_dicts()
        cache[sym] = [{"d": r["time"].strftime("%Y-%m-%d"), "o": r["open"], "h": r["high"],
                       "l": r["low"], "c": r["close"], "v": r["volume"]} for r in rows]
    return cache


def signal_features(series, idx):
    if idx < 2 or idx + 1 >= len(series):
        return None
    sig = series[idx]
    vol20 = sum(b["v"] for b in series[max(0, idx - 20):idx]) / max(1, min(20, idx))
    rng = sig["h"] - sig["l"]
    return {
        "vol_ratio": sig["v"] / vol20 if vol20 else 1.0,
        "close_range": (sig["c"] - sig["l"]) / rng if rng > 0 else 1.0,  # LOW = weak close = bearish confirm
        "broke_2d_low": sig["c"] <= min(b["c"] for b in series[idx - 2:idx]),
        "signal_close": sig["c"],
    }


def is_breakdown_ignition(f, vol_min=1.5, close_range_max=0.4):
    """Mirror of bullish ignition: 2-day-low break + volume + WEAK close (bottom of range)."""
    return f["broke_2d_low"] and f["vol_ratio"] >= vol_min and f["close_range"] <= close_range_max


def sim_long_put(series, entry_idx, entry_field="c"):
    """Mirror of sim_option in strategy_lab.py, but for a long PUT: profits as
    price falls. Same exit structure (2x target, cushion+trail, hard stop)."""
    S0 = series[entry_idx][entry_field]
    K = nearest_strike(S0)
    sigma = estimate_iv([b["c"] for b in series[max(0, entry_idx - 20):entry_idx]])
    entry_premium = bs_put_price(S0, K, EXPIRY_DAYS / 365.0, RISK_FREE_RATE, sigma)
    if entry_premium <= 0.05:
        return None
    peak, armed, idx, days = entry_premium, False, entry_idx, 0
    while True:
        idx += 1
        days += 1
        if idx >= len(series):
            return None  # unresolved within data window
        S_t = series[idx]["c"]
        t = max(EXPIRY_DAYS - days, 1)
        p = bs_put_price(S_t, K, t / 365.0, RISK_FREE_RATE, sigma)
        peak = max(peak, p)
        if peak >= entry_premium * (1 + CUSHION):
            armed = True
        pnl = (p - entry_premium) / entry_premium * 100
        if p >= entry_premium * TARGET_MULT:
            return pnl
        if armed and p <= peak * (1 - TRAIL_GIVEBACK):
            return pnl
        if p <= entry_premium * (1 - HARD_STOP_MULT):
            return pnl
        if t <= 1:
            return pnl


def main():
    universe = load_universe()
    print(f"Universe: {len(universe)} symbols. Loading + computing indicators...")
    df = load_and_compute(universe, "2026-04-15", "2026-07-02")
    window_df = df.filter((pl.col("time") >= datetime.date(2026, 5, 1)) & (pl.col("time") <= datetime.date(2026, 6, 30)))
    series_cache = build_series_cache(df)
    breadth = compute_breadth(df)

    cands = breakdown_candidates(window_df)
    print(f"Breakdown candidates (all days, real screener-style filters): {len(cands)}")

    all_rows = []
    for row in cands.to_dicts():
        sym, date = row["symbol"], row["time"].strftime("%Y-%m-%d")
        series = series_cache.get(sym)
        if not series:
            continue
        dl = [b["d"] for b in series]
        if date not in dl:
            continue
        idx = dl.index(date)
        f = signal_features(series, idx)
        if not f or not is_breakdown_ignition(f):
            continue
        pnl = sim_long_put(series, idx, entry_field="c")  # same-day close entry (validated for CE)
        if pnl is not None:
            all_rows.append({"symbol": sym, "date": date, "pnl": pnl, "breadth": breadth.get(date, 50)})

    print(f"\nBreakdown-ignition long-PUT trades: {len(all_rows)}")

    def summarize(rows, label):
        if not rows:
            print(f"{label}: n=0")
            return
        n = len(rows)
        wr = 100 * sum(1 for r in rows if r["pnl"] > 0) / n
        avg = sum(r["pnl"] for r in rows) / n
        print(f"{label}: n={n}  WR={wr:.1f}%  avg={avg:+.1f}%")

    summarize(all_rows, "ALL breakdown-PE trades (no breadth filter)")

    may_rows = [r for r in all_rows if r["date"] < "2026-06-01"]
    june_rows = [r for r in all_rows if r["date"] >= "2026-06-01"]
    summarize(may_rows, "MAY (the regime that killed CE-only book)")
    summarize(june_rows, "JUNE (the regime CE-only book was strong in)")

    print("\n--- Testing breadth-gate variants (bearish setups may need LOW breadth to confirm) ---")
    for breadth_max in (100, 50, 40, 30):
        filtered = [r for r in all_rows if r["breadth"] <= breadth_max]
        summarize(filtered, f"breadth <= {breadth_max}%")

    print("\n--- Combined CE (bull regime, June) + PE (bear regime, May) hypothesis ---")
    if may_rows:
        summarize(may_rows, "MAY PE book (would replace/complement the failed May CE book)")

    print("\n--- Threshold refinement pass (mirror of what tightened CE) ---")
    def resim(vol_min, cr_max, target_mult, hard_stop):
        rows = []
        for row in cands.to_dicts():
            sym, date = row["symbol"], row["time"].strftime("%Y-%m-%d")
            series = series_cache.get(sym)
            if not series:
                continue
            dl = [b["d"] for b in series]
            if date not in dl:
                continue
            idx = dl.index(date)
            f = signal_features(series, idx)
            if not f or not (f["broke_2d_low"] and f["vol_ratio"] >= vol_min and f["close_range"] <= cr_max):
                continue
            pnl = sim_long_put_custom(series, idx, target_mult, hard_stop)
            if pnl is not None:
                rows.append({"date": date, "pnl": pnl})
        return rows

    def sim_long_put_custom(series, entry_idx, target_mult, hard_stop):
        S0 = series[entry_idx]["c"]
        K = nearest_strike(S0)
        sigma = estimate_iv([b["c"] for b in series[max(0, entry_idx - 20):entry_idx]])
        entry_premium = bs_put_price(S0, K, EXPIRY_DAYS / 365.0, RISK_FREE_RATE, sigma)
        if entry_premium <= 0.05:
            return None
        peak, armed, idx, days = entry_premium, False, entry_idx, 0
        while True:
            idx += 1; days += 1
            if idx >= len(series):
                return None
            S_t = series[idx]["c"]
            t = max(EXPIRY_DAYS - days, 1)
            p = bs_put_price(S_t, K, t / 365.0, RISK_FREE_RATE, sigma)
            peak = max(peak, p)
            if peak >= entry_premium * (1 + CUSHION):
                armed = True
            pnl = (p - entry_premium) / entry_premium * 100
            if p >= entry_premium * target_mult:
                return pnl
            if armed and p <= peak * (1 - TRAIL_GIVEBACK):
                return pnl
            if p <= entry_premium * (1 - hard_stop):
                return pnl
            if t <= 1:
                return pnl

    for vol_min, cr_max, tmult, hstop in [(1.5, 0.4, 2.0, 0.5), (2.0, 0.3, 2.0, 0.5),
                                            (2.0, 0.3, 1.5, 0.35), (2.5, 0.25, 1.5, 0.35),
                                            (2.0, 0.4, 1.5, 0.5)]:
        rows = resim(vol_min, cr_max, tmult, hstop)
        may_r = [r for r in rows if r["date"] < "2026-06-01"]
        jun_r = [r for r in rows if r["date"] >= "2026-06-01"]
        label = f"vol>={vol_min} cr<={cr_max} T={tmult}x stop={hstop}"
        summarize(rows, f"ALL {label}")
        summarize(may_r, f"  MAY {label}")
        summarize(jun_r, f"  JUN {label}")


if __name__ == "__main__":
    main()
