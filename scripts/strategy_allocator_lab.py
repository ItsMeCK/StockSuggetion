"""
Strategy Allocator validation: one decision per candidate, per day -

  HIGH MOMENTUM (ignition/breakdown trigger fires) -> LONG OPTION (convexity)
    - bullish ignition  -> long CE  (validated ~55-70% WR, big-winner skew)
    - bearish breakdown -> long PE  (validated ~34-51% WR, big-winner skew)

  STABLE QUALITY UPTREND (Stage-2 confirmed, RS/breadth healthy, but NOT
  currently igniting) -> BULL PUT CREDIT SPREAD (income / win-rate)
    - validated 74-83% WR, positive expectancy, over 2 real years (see
      scripts/credit_spread_backtest.py)

Reuses already-validated screener/candidate logic and simulators - no
reimplementation, no new curve-fitting. Reports the BLENDED portfolio result:
this is the number that matters, since it's what a live book would actually
realize (mix of both sub-strategies, not either alone).
"""
import os
import math
import datetime
import statistics
from collections import defaultdict
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
from scripts.bearish_pe_validation_lab import (
    breakdown_candidates, signal_features as bearish_signal_features,
    is_breakdown_ignition, sim_long_put, bs_put_price,
)
from scripts.strategy_lab import sim_option
from scripts.router_threshold_grid import load_breadth  # 20-SMA breadth: validated for CE/PE ignition gating
from scripts.credit_spread_backtest import bs_put as cs_bs_put, compute_breadth as compute_breadth_200sma

RISK_FREE = 0.07
CS_HOLDING_DAYS = 10   # matches credit_spread_backtest.py default
CS_SHORT_OTM_PCT = 0.07
CS_WIDTH_PCT = 0.05
CS_RS_MIN = 60
CS_BREADTH_MIN = 50


def is_bullish_ignition(f, vol_min=1.5, cr_min=0.6):
    return f["broke_2d_high"] and f["vol_ratio"] >= vol_min and f["close_range"] >= cr_min


def credit_spread_eligible_candidates(df):
    """Matches credit_spread_backtest.py's actual validated Stage-2 definition
    EXACTLY (close > sma_50 > sma_200, no extension override) - NOT the
    noisier gate1_widening_lab.baseline_candidates() which also includes
    already-extended 'flagged_momentum' names via an OR-branch. Selling a put
    under an already-extended, hot-momentum stock is the wrong population for
    an income trade (it's primed for a pullback); credit spreads want calm,
    established uptrends, not stocks already in the middle of a sprint."""
    return df.filter(
        (pl.col("close") > pl.col("sma_50")) &
        (pl.col("sma_50") > pl.col("sma_200"))
    ).select(["symbol", "time"]).unique()


def compute_rs_rank(df):
    ranked = df.filter(pl.col("roc_10").is_not_null()).with_columns(
        (pl.col("roc_10").rank(method="average").over("time") /
         pl.col("roc_10").count().over("time") * 100).alias("rs_percentile")
    ) if "roc_10" in df.columns else None
    if ranked is None:
        return {}
    return {(r["symbol"], r["time"].strftime("%Y-%m-%d")): r["rs_percentile"]
            for r in ranked.select(["symbol", "time", "rs_percentile"]).to_dicts()}


def credit_spread_trade(entry_price, dvol20_annualized, series, entry_idx, holding_days=CS_HOLDING_DAYS):
    """Sells a put at entry_price*(1-7%), buys protection 5% further down.
    WIN classified on REAL underlying path (premium-independent); expectancy
    uses a BS credit estimate (same methodology as credit_spread_backtest.py)."""
    if entry_idx + holding_days >= len(series):
        return None
    k_short = entry_price * (1 - CS_SHORT_OTM_PCT)
    k_long = entry_price * (1 - CS_SHORT_OTM_PCT - CS_WIDTH_PCT)
    path = series[entry_idx + 1: entry_idx + holding_days + 1]
    exit_price = series[entry_idx + holding_days]["c"]
    win = exit_price >= k_short

    t_years = holding_days / 252.0
    credit = cs_bs_put(entry_price, k_short, t_years, RISK_FREE, dvol20_annualized) - \
             cs_bs_put(entry_price, k_long, t_years, RISK_FREE, dvol20_annualized)
    width = k_short - k_long
    short_intr = max(k_short - exit_price, 0.0)
    long_intr = max(k_long - exit_price, 0.0)
    owed = short_intr - long_intr
    pnl_abs = credit - owed
    max_loss = max(width - credit, 1e-9)
    pnl_pct_risk = pnl_abs / max_loss * 100
    return {"win": win, "pnl_pct_risk": pnl_pct_risk}


def main():
    universe = load_universe()
    print(f"Universe: {len(universe)} symbols. Loading + computing indicators...")
    df = load_and_compute(universe, "2026-04-15", "2026-07-02")
    window_df = df.filter((pl.col("time") >= datetime.date(2026, 5, 1)) & (pl.col("time") <= datetime.date(2026, 6, 30)))
    series_cache = build_series_cache(df)
    breadth = load_breadth()  # 20-SMA breadth, for CE/PE ignition gating (validated with this definition)
    # 200-SMA breadth, for credit-spread gating (validated with this definition in credit_spread_backtest.py)
    breadth_200_raw = compute_breadth_200sma(df)
    breadth_200 = {(d.strftime("%Y-%m-%d") if hasattr(d, "strftime") else d): v for d, v in breadth_200_raw.items()}

    # need roc_10 on window_df for RS rank (gate1_widening_lab's load_and_compute already adds roc_10)
    rs_map = compute_rs_rank(window_df)

    bull_cands = baseline_candidates(window_df)          # stage2 + flagged (bullish universe, for ignition only)
    cs_cands = credit_spread_eligible_candidates(window_df)  # clean stage2-only (close>50sma>200sma), for credit spreads
    bear_cands = breakdown_candidates(window_df)          # stage4 + breakdown ignition (bearish universe)
    print(f"Bullish-universe candidate-days: {len(bull_cands)}   "
          f"Credit-spread-eligible candidate-days: {len(cs_cands)}   "
          f"Bearish-universe candidate-days: {len(bear_cands)}")

    trades = {"CE_IGNITION": [], "PE_IGNITION": [], "CREDIT_SPREAD": []}
    ignited_today = set()  # (symbol, date) that already got a CE ignition trade

    # --- Bullish universe: ignition -> long CE ---
    for row in bull_cands.to_dicts():
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

        if is_bullish_ignition(f) and breadth.get(date, 0) >= 60:
            pnl = sim_option({"series": series, "idx": idx})
            if pnl is not None:
                trades["CE_IGNITION"].append({"date": date, "pnl": pnl, "win": pnl > 0})
            ignited_today.add((sym, date))

    # --- Credit spread: clean Stage-2 (close>50sma>200sma) names that did NOT
    # ignite today - calm, established uptrends, not already-extended sprints ---
    for row in cs_cands.to_dicts():
        sym, date = row["symbol"], row["time"].strftime("%Y-%m-%d")
        if (sym, date) in ignited_today:
            continue
        series = series_cache.get(sym)
        if not series:
            continue
        dl = [b["d"] for b in series]
        if date not in dl:
            continue
        idx = dl.index(date)
        rs = rs_map.get((sym, date))
        if rs is None or rs < CS_RS_MIN or breadth_200.get(date, 0) < CS_BREADTH_MIN:
            continue
        entry_price = series[idx]["c"]
        trailing = [b["c"] for b in series[max(0, idx - 20):idx]]
        if len(trailing) < 5:
            continue
        rets = [math.log(trailing[i] / trailing[i - 1]) for i in range(1, len(trailing)) if trailing[i - 1] > 0]
        if len(rets) < 5:
            continue
        dvol_ann = statistics.stdev(rets) * math.sqrt(252)
        r = credit_spread_trade(entry_price, dvol_ann, series, idx)
        if r:
            trades["CREDIT_SPREAD"].append({"date": date, "pnl": r["pnl_pct_risk"], "win": r["win"]})

    # --- Bearish universe: breakdown ignition -> long PE ---
    for row in bear_cands.to_dicts():
        sym, date = row["symbol"], row["time"].strftime("%Y-%m-%d")
        series = series_cache.get(sym)
        if not series:
            continue
        dl = [b["d"] for b in series]
        if date not in dl:
            continue
        idx = dl.index(date)
        f = bearish_signal_features(series, idx)
        if not f or not is_breakdown_ignition(f, vol_min=2.0, close_range_max=0.3):
            continue
        pnl = sim_long_put(series, idx, entry_field="c")
        if pnl is not None:
            trades["PE_IGNITION"].append({"date": date, "pnl": pnl, "win": pnl > 0})

    # --- Report ---
    print("\n" + "=" * 78)
    print("STRATEGY ALLOCATOR: blended portfolio result (May + June combined)")
    print("=" * 78)

    def bucket_stats(rows):
        if not rows:
            return 0, 0, 0
        n = len(rows)
        wr = 100 * sum(1 for r in rows if r["win"]) / n
        avg = sum(r["pnl"] for r in rows) / n
        return n, wr, avg

    all_rows = []
    for name, rows in trades.items():
        n, wr, avg = bucket_stats(rows)
        print(f"\n{name}: n={n}  WR={wr:.1f}%  avg_pnl={avg:+.1f}%(of premium or risk-capital)")
        may = [r for r in rows if r["date"] < "2026-06-01"]
        jun = [r for r in rows if r["date"] >= "2026-06-01"]
        for label, rr in [("  MAY", may), ("  JUN", jun)]:
            if rr:
                n2, wr2, avg2 = bucket_stats(rr)
                print(f"{label}: n={n2}  WR={wr2:.1f}%  avg={avg2:+.1f}%")
        all_rows.extend(rows)

    n, wr, avg = bucket_stats(all_rows)
    print(f"\n--- BLENDED PORTFOLIO (all 3 buckets combined) ---")
    print(f"Total trades: {n}   Blended WIN RATE: {wr:.1f}%")
    print("(Note: 'avg_pnl' is on DIFFERENT bases per bucket - options=%of premium, "
          "spread=%of risk-capital - so a blended avg P&L isn't directly meaningful. "
          "Win rate blends validly since it's dimensionless.)")

    may_all = [r for r in all_rows if r["date"] < "2026-06-01"]
    jun_all = [r for r in all_rows if r["date"] >= "2026-06-01"]
    for label, rr in [("MAY (blended)", may_all), ("JUNE (blended)", jun_all)]:
        n2, wr2, _ = bucket_stats(rr)
        print(f"{label}: n={n2}  WR={wr2:.1f}%")


if __name__ == "__main__":
    main()
