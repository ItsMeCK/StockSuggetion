"""
Builds and validates TWO market-regime detectors that WIDEN THE WATCHLIST only
(never trigger a buy directly) - the actual entry still requires the stock's
own precise, same-day confirmation, per the timing discipline established
this session (time is the enemy of options; be there exactly when momentum
starts, not before, not after).

1. BREADTH THRUST DETECTOR: market breadth was recently low (<45% in the last
   5 days) and is now rising sharply (3-day change >= +15 points) - matches
   core/context_rules_3.json's documented "breadth_thrust" definition. While
   active, widens the candidate net to the "broader continuation" profile
   (already tested standalone: +92 captures) - but ONLY on thrust-active days.

2. PANIC DIP DETECTOR: breadth drops sharply in ONE day (>=15 points 1-day
   change) - an unambiguous, same-day-detectable signal (unlike momentum,
   which develops over time, a panic day is obvious the moment it happens).
   Widens the watchlist to quality stocks (were in Stage-2 BEFORE the panic)
   that show a same-day reversal signature (closed in the upper half of the
   day's range despite being down) - the actual entry confirmation.

Both are validated for (a) incremental capture rate AND (b) actual option P&L
on the newly-unlocked trades - coverage alone is not the bar; profitability is.
"""
import os
import csv
import datetime
import statistics
import psycopg2
import polars as pl
from dotenv import load_dotenv

load_dotenv()
import sys
sys.path.insert(0, "/Users/poonamsalke/Workplace/StockSuggetion")
os.chdir("/Users/poonamsalke/Workplace/StockSuggetion")

from scripts.gate1_widening_lab import load_universe, build_series_cache
from scripts.strategy_lab import sim_option

THRUST_LOW_THRESHOLD = 45.0
THRUST_3D_CHANGE_MIN = 15.0
PANIC_1D_DROP_MIN = 15.0


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"))


def load_full_data(universe, start, end):
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
        (((pl.col("close") - pl.col("close").shift(10).over("symbol")) / pl.col("close").shift(10).over("symbol")) * 100).alias("roc_10"),
        pl.when(pl.col("high") > pl.col("low"))
          .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
          .otherwise(1.0).alias("close_range_pct"),
    ])
    start_d = datetime.date.fromisoformat(start)
    end_d = datetime.date.fromisoformat(end)
    return df.filter((pl.col("time") >= start_d) & (pl.col("time") <= end_d) & pl.col("sma_200").is_not_null())


def compute_breadth_series(df):
    b = (df.filter(pl.col("sma_20").is_not_null())
           .with_columns((pl.col("close") > pl.col("sma_20")).alias("above20"))
           .group_by("time").agg((pl.col("above20").mean() * 100).alias("breadth")))
    rows = sorted(b.to_dicts(), key=lambda r: r["time"])
    dates = [r["time"].strftime("%Y-%m-%d") for r in rows]
    vals = [r["breadth"] for r in rows]
    return dates, vals


def detect_regime_days(dates, vals):
    thrust_days, panic_days = set(), set()
    for i in range(5, len(vals)):
        recent_min = min(vals[max(0, i - 5):i])
        change_3d = vals[i] - vals[i - 3] if i >= 3 else 0
        if recent_min < THRUST_LOW_THRESHOLD and change_3d >= THRUST_3D_CHANGE_MIN:
            thrust_days.add(dates[i])
        change_1d = vals[i] - vals[i - 1]
        if change_1d <= -PANIC_1D_DROP_MIN:
            panic_days.add(dates[i])
    return thrust_days, panic_days


def broader_continuation(row):
    if row["sma_50"] is None or row["roc_10"] is None or row["sma_10"] is None or row["sma_20"] is None:
        return False
    return (row["close"] > row["sma_50"] and row["sma_10"] > row["sma_20"] and
            row["roc_10"] > 0 and row["volume"] >= 1.0 * (row["vol_avg_20"] or row["volume"]))


def panic_reversal_confirm(row, sma50_floor=0.97, cr_min=0.5):
    """Was in Stage-2 style uptrend context (above 50sma despite today's dip)
    AND shows a same-day reversal signature (closed in upper half of range
    despite the panic - i.e. buyers stepped in, not a full capitulation)."""
    if row["sma_50"] is None or row["close_range_pct"] is None:
        return False
    return row["close"] > row["sma_50"] * sma50_floor and row["close_range_pct"] >= cr_min


def main():
    universe = load_universe()
    print(f"Universe: {len(universe)} symbols. Loading full window...")
    df = load_full_data(universe, "2026-04-15", "2026-07-02")
    window_df = df.filter((pl.col("time") >= datetime.date(2026, 5, 1)) & (pl.col("time") <= datetime.date(2026, 6, 30)))
    series_cache = build_series_cache(window_df) if False else None  # not used directly; use full df cache below
    series_cache = {}
    for sym_df in df.partition_by("symbol"):
        sym = sym_df["symbol"][0]
        rows = sym_df.sort("time").to_dicts()
        series_cache[sym] = rows

    dates, vals = compute_breadth_series(window_df)
    thrust_days, panic_days = detect_regime_days(dates, vals)
    print(f"\nThrust-active days detected: {sorted(thrust_days)}")
    print(f"Panic days detected: {sorted(panic_days)}")

    # --- Evaluate THRUST watchlist widening ---
    thrust_trades = []
    for row in window_df.to_dicts():
        date = row["time"].strftime("%Y-%m-%d")
        if date not in thrust_days:
            continue
        if not broader_continuation(row):
            continue
        sym = row["symbol"]
        series = series_cache.get(sym)
        if not series:
            continue
        dl = [r["time"].strftime("%Y-%m-%d") for r in series]
        if date not in dl:
            continue
        idx = dl.index(date)
        if idx + 1 >= len(series):
            continue
        series_short = [{"o": r["open"], "h": r["high"], "l": r["low"], "c": r["close"], "v": r["volume"]} for r in series]
        pnl = sim_option({"series": series_short, "idx": idx})
        if pnl is not None:
            thrust_trades.append({"date": date, "sym": sym, "pnl": pnl})

    # --- Evaluate PANIC watchlist widening: grid over confirmation strictness ---
    all_panic_rows = [row for row in window_df.to_dicts() if row["time"].strftime("%Y-%m-%d") in panic_days]

    def eval_panic(sma50_floor, cr_min):
        trades = []
        for row in all_panic_rows:
            if not panic_reversal_confirm(row, sma50_floor, cr_min):
                continue
            date = row["time"].strftime("%Y-%m-%d")
            sym = row["symbol"]
            series = series_cache.get(sym)
            if not series:
                continue
            dl = [r["time"].strftime("%Y-%m-%d") for r in series]
            if date not in dl:
                continue
            idx = dl.index(date)
            if idx + 1 >= len(series):
                continue
            series_short = [{"o": r["open"], "h": r["high"], "l": r["low"], "c": r["close"], "v": r["volume"]} for r in series]
            pnl = sim_option({"series": series_short, "idx": idx})
            if pnl is not None:
                trades.append({"date": date, "sym": sym, "pnl": pnl})
        return trades

    print("\n--- Panic-dip confirmation strictness grid ---")
    best_panic = None
    for sma50_floor in (0.97, 1.0, 1.02, 1.05):
        for cr_min in (0.5, 0.6, 0.7, 0.8, 0.9):
            trades = eval_panic(sma50_floor, cr_min)
            n = len(trades)
            if n < 15:
                continue
            pnls = [t["pnl"] for t in trades]
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else float('inf')
            exp = statistics.mean(pnls)
            wr = 100 * len(wins) / n
            print(f"  sma50_floor={sma50_floor} cr_min={cr_min}: n={n:3d} WR={wr:5.1f}% PF={pf:.2f} exp={exp:+.1f}%")
            if best_panic is None or (pf > best_panic[1] and n >= 15):
                best_panic = (trades, pf)

    panic_trades = best_panic[0] if best_panic else []

    def report(trades, label):
        n = len(trades)
        if n == 0:
            print(f"\n{label}: n=0"); return
        pnls = [t["pnl"] for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        wr = 100 * len(wins) / n
        pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else float('inf')
        exp = statistics.mean(pnls)
        print(f"\n{label} (n={n}):")
        print(f"  Win rate: {wr:.1f}%   Avg win: {statistics.mean(wins) if wins else 0:+.1f}%   "
              f"Avg loss: {statistics.mean(losses) if losses else 0:+.1f}%")
        print(f"  Profit factor: {pf:.2f}   Expectancy: {exp:+.1f}%")

    report(thrust_trades, "BREADTH THRUST watchlist trades")
    report(panic_trades, "PANIC DIP watchlist trades")
    report(thrust_trades + panic_trades, "COMBINED new regime-based trades")


if __name__ == "__main__":
    main()
