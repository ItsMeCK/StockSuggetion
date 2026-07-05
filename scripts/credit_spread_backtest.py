"""
Put Credit Spread (Bull Put Spread) win-rate backtest on REAL underlying prices,
Sept 2024 - July 2026 (~2 years, 520 trading days, multiple regimes).

WHY THIS IS CREDIBLE WITHOUT HISTORICAL OPTION PREMIUMS:
A put credit spread's WIN/LOSS is decided by the real underlying price vs the
SHORT STRIKE at expiry - data we have exactly. We sell a put at K_short, buy a
cheaper put at K_long (< K_short) to define risk. At expiry:
  - underlying >= K_short  -> both puts expire worthless -> keep full credit = WIN
  - underlying <= K_long   -> max loss (full width - credit)
  - in between             -> partial
This classifies WIN (underlying >= K_short) using ONLY real prices. Only the
win/loss MAGNITUDE needs a premium model; the WIN RATE (the user's north star)
does not. So this is the honest, premium-independent test of "70% WR for years".

Institutional design (tastytrade / premium-selling canon):
  - A ~30-delta short put has ~70% probability of expiring OTM BY CONSTRUCTION;
    that is the structural source of a high win rate. We approximate strike
    placement by realized-vol (1 SD over the holding horizon) and also test
    fixed-% OTM levels, then measure the REAL win rate.
  - Entry filters to bias the distribution up-and-right (raise WR above the raw
    ~70% structural base): only sell puts on stocks in a confirmed UPTREND
    (Stage 2: close>50SMA>200SMA) with high relative strength, and only when
    the broad MARKET REGIME is healthy (breadth) - i.e. never sell puts into a
    crash tail, which is what kills premium sellers.

Read-only. No LLM. Runs in seconds.
"""
import os
import csv
import math
import datetime
import argparse
import statistics
from collections import defaultdict
import psycopg2
import polars as pl
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"))


def _norm_cdf(x):
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_put(S, K, t, r, sigma):
    if t <= 0 or sigma <= 0:
        return max(K - S, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    return K * math.exp(-r * t) * _norm_cdf(-d2) - S * _norm_cdf(-d1)


RISK_FREE = 0.07


def load_universe():
    syms = set()
    with open("pipeline/master_universe.csv") as f:
        for row in csv.DictReader(f):
            syms.add(row["Symbol"])
    return syms


def load_data(universe):
    conn = get_conn()
    df = pl.read_database(
        "SELECT time::date as time, symbol, high, low, close FROM daily_ohlcv WHERE symbol = ANY(%(s)s) ORDER BY symbol, time",
        conn, execute_options={"parameters": {"s": list(universe)}})
    conn.close()
    df = df.with_columns([
        pl.col("close").rolling_mean(50).over("symbol").alias("sma_50"),
        pl.col("close").rolling_mean(200).over("symbol").alias("sma_200"),
        (((pl.col("close") - pl.col("close").shift(20).over("symbol")) / pl.col("close").shift(20).over("symbol")) * 100).alias("roc_20"),
        (pl.col("close") / pl.col("close").shift(1).over("symbol")).log().alias("logret"),
    ])
    df = df.with_columns(pl.col("logret").rolling_std(20).over("symbol").alias("dvol_20"))
    # cross-sectional RS rank per day
    df = df.with_columns(
        pl.when(pl.col("roc_20").is_not_null())
          .then(pl.col("roc_20").rank(method="average").over("time") / pl.col("roc_20").count().over("time") * 100)
          .otherwise(None).alias("rs_pct")
    )
    return df


def compute_breadth(df):
    b = (df.filter(pl.col("sma_200").is_not_null())
           .with_columns((pl.col("close") > pl.col("sma_200")).alias("above200"))
           .group_by("time").agg((pl.col("above200").mean() * 100).alias("breadth200")))
    return {r["time"]: r["breadth200"] for r in b.to_dicts()}


def run(df, breadth, holding_days, short_otm_sd, width_sd, rs_min, breadth_min,
        require_stage2, fixed_short_pct, fixed_width_pct, manage_profit_target=None):
    """One config. Returns per-trade WIN/LOSS records classified on REAL prices."""
    records = []
    for sym_df in df.partition_by("symbol"):
        rows = sym_df.sort("time").to_dicts()
        n = len(rows)
        for i in range(n):
            r = rows[i]
            if r["sma_50"] is None or r["sma_200"] is None or r["dvol_20"] is None or r["rs_pct"] is None:
                continue
            entry = r["close"]
            entry_date = r["time"]
            # --- entry filters ---
            if require_stage2 and not (entry > r["sma_50"] > r["sma_200"]):
                continue
            if r["rs_pct"] < rs_min:
                continue
            if breadth.get(entry_date, 0) < breadth_min:
                continue
            exit_i = i + holding_days
            if exit_i >= n:
                continue
            # --- strike placement ---
            if fixed_short_pct is not None:
                k_short = entry * (1 - fixed_short_pct)
                k_long = entry * (1 - fixed_short_pct - fixed_width_pct)
            else:
                sd_period = r["dvol_20"] * math.sqrt(holding_days)  # 1-SD log-move over horizon
                k_short = entry * math.exp(-short_otm_sd * sd_period)
                k_long = entry * math.exp(-(short_otm_sd + width_sd) * sd_period)
            # --- outcome on REAL underlying path ---
            path = rows[i + 1: exit_i + 1]
            exit_underlying = rows[exit_i]["close"]
            min_low = min(b["low"] for b in path)
            touched_short = min_low <= k_short
            breached_long = min_low <= k_long
            win = exit_underlying >= k_short  # premium-independent WR classification

            # --- P&L / expectancy layer (needs a credit estimate) ---
            # Credit = net premium collected = BS(short put) - BS(long put), using
            # the realized-vol proxy for entry IV. This is the ONE place a model
            # enters; the WIN classification above does not depend on it.
            t_years = holding_days / 252.0
            sigma_ann = r["dvol_20"] * math.sqrt(252)
            credit = bs_put(entry, k_short, t_years, RISK_FREE, sigma_ann) - \
                     bs_put(entry, k_long, t_years, RISK_FREE, sigma_ann)
            width = k_short - k_long
            # Spread value owed at expiry = short_intrinsic - long_intrinsic (exact given real S)
            short_intr = max(k_short - exit_underlying, 0.0)
            long_intr = max(k_long - exit_underlying, 0.0)
            owed = short_intr - long_intr
            pnl_abs = credit - owed
            # Normalize P&L as % of capital-at-risk (max loss = width - credit)
            max_loss = max(width - credit, 1e-9)
            pnl_pct_risk = pnl_abs / max_loss * 100

            records.append({
                "symbol": r["symbol"], "entry_date": entry_date, "win": win,
                "touched_short": touched_short, "breached_long": breached_long,
                "credit_pct_width": credit / width * 100 if width > 0 else 0,
                "pnl_pct_risk": pnl_pct_risk,
                "entry": entry, "breadth": breadth.get(entry_date, 0),
            })
    return records


def regime_of(date, breadth):
    b = breadth.get(date, 50)
    if b >= 60: return "healthy(>=60)"
    if b >= 40: return "mixed(40-60)"
    return "weak(<40)"


def summarize(records, breadth, label):
    if not records:
        print(f"{label}: n=0")
        return 0, 0
    n = len(records)
    w = sum(1 for r in records if r["win"])
    wr = 100 * w / n
    pnls = [r["pnl_pct_risk"] for r in records]
    avg_credit = statistics.mean(r["credit_pct_width"] for r in records)
    expectancy = statistics.mean(pnls)
    wins_pnl = [p for p in pnls if p > 0]
    loss_pnl = [p for p in pnls if p <= 0]
    pf = (sum(wins_pnl) / abs(sum(loss_pnl))) if loss_pnl and sum(loss_pnl) != 0 else float('inf')
    print(f"\n{label}:")
    print(f"    n={n}  WIN RATE={wr:.1f}%  credit={avg_credit:.0f}% of width  "
          f"expectancy={expectancy:+.1f}% of risk/trade  profit_factor={pf:.2f}")
    # by regime
    byreg = defaultdict(list)
    for r in records:
        byreg[regime_of(r["entry_date"], breadth)].append(r)
    for reg in ("healthy(>=60)", "mixed(40-60)", "weak(<40)"):
        if reg in byreg:
            v = byreg[reg]
            rwr = 100 * sum(1 for x in v if x["win"]) / len(v)
            rexp = statistics.mean(x["pnl_pct_risk"] for x in v)
            print(f"    regime {reg:16s} n={len(v):5d}  WR={rwr:.1f}%  exp={rexp:+.1f}%")
    byhy = defaultdict(list)
    for r in records:
        d = r["entry_date"]
        byhy[f"{d.year}H{1 if d.month<=6 else 2}"].append(r["win"])
    print("    WR by half-year:", "  ".join(f"{k}:{100*sum(v)/len(v):.0f}%(n{len(v)})" for k, v in sorted(byhy.items())))
    return n, wr


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--holding", type=int, default=20)
    args = ap.parse_args()

    universe = load_universe()
    print(f"Universe: {len(universe)} symbols. Loading ~2yr underlying data...")
    df = load_data(universe)
    breadth = compute_breadth(df)
    print(f"Loaded. Days with breadth: {len(breadth)}. Holding horizon: {args.holding} trading days.\n")
    print("="*74)
    print("PUT CREDIT SPREAD win rate on REAL underlying prices (Sept 2024 - Jul 2026)")
    print("WIN = underlying >= short strike at expiry (premium-independent, conservative)")
    print("="*74)

    # Grid: strike distance (in SDs) x filters. Wider OTM = higher WR but less credit.
    configs = [
        # (label, short_sd, width_sd, rs_min, breadth_min, stage2, fixed_short%, fixed_width%)
        ("1.0-SD short, no filters",          1.0, 1.0, 0,  0,  False, None, None),
        ("1.0-SD short, Stage2 only",         1.0, 1.0, 0,  0,  True,  None, None),
        ("1.0-SD short, Stage2+RS70",         1.0, 1.0, 70, 0,  True,  None, None),
        ("1.0-SD short, Stage2+RS70+breadth50",1.0,1.0, 70, 50, True,  None, None),
        ("1.5-SD short, Stage2+RS70+breadth50",1.5,1.0, 70, 50, True,  None, None),
        ("1.5-SD short, Stage2+RS60+breadth50",1.5,1.0, 60, 50, True,  None, None),
        ("2.0-SD short, Stage2+RS60+breadth50",2.0,1.0, 60, 50, True,  None, None),
        ("fixed 5% OTM short, Stage2+RS70+br50",None,None,70,50,True, 0.05, 0.05),
        ("fixed 7% OTM short, Stage2+RS60+br50",None,None,60,50,True, 0.07, 0.05),
        ("fixed 10% OTM short, Stage2+RS60+br40",None,None,60,40,True,0.10,0.05),
    ]
    for (label, ssd, wsd, rs, br, s2, fsp, fwp) in configs:
        recs = run(df, breadth, args.holding, ssd, wsd, rs, br, s2, fsp, fwp, manage_profit_target="expiry")
        summarize(recs, breadth, label)


if __name__ == "__main__":
    main()
