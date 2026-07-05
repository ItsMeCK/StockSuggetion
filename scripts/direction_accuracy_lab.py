"""
Tests the user's reframing directly: instead of "predict an explosive
ignition," just predict DIRECTION (up/down) using technical trend + RS +
fundamental + news signals, since a mere +/-2% move in the underlying
already returns ~50-60% on a leveraged option (user's own empirical rule,
validated against real premium math below).

Step 1: confirm the leverage claim itself against our real Black-Scholes
        proxy (does a 2% underlying move really produce ~50-60% option move?)
Step 2: measure DIRECTIONAL HIT RATE - of stocks in a confirmed uptrend
        (Stage-2, our existing bullish universe) or downtrend (Stage-4, our
        bearish universe), what % actually move >=2% in the PREDICTED
        direction within N days? This is the real number that matters now.

No LLM, pure price math + reuse of already-validated candidate logic.
"""
import os
import math
import datetime
import statistics
import polars as pl
from dotenv import load_dotenv

load_dotenv()
import sys
sys.path.insert(0, "/Users/poonamsalke/Workplace/StockSuggetion")
os.chdir("/Users/poonamsalke/Workplace/StockSuggetion")

from scripts.gate1_widening_lab import (
    load_universe, load_and_compute, baseline_candidates, build_series_cache,
)
from scripts.bearish_pe_validation_lab import breakdown_candidates
from scripts.backtest_3pm_options_vs_equity import (
    bs_call_price, nearest_strike, estimate_iv, RISK_FREE_RATE, EXPIRY_DAYS,
)
from scripts.bearish_pe_validation_lab import bs_put_price


def step1_confirm_leverage_claim():
    """For a range of realistic setups, how much does the option move when
    the underlying moves exactly +/-2%?"""
    print("=" * 74)
    print("STEP 1: Does a 2% underlying move really produce ~50-60% option move?")
    print("=" * 74)
    for S0 in (500, 1500, 5000):
        for sigma in (0.25, 0.35, 0.45):
            K = nearest_strike_local(S0)
            t = EXPIRY_DAYS / 365.0
            entry = bs_call_price(S0, K, t, RISK_FREE_RATE, sigma)
            S_up2 = S0 * 1.02
            # one day passes (theta), t reduces slightly
            t2 = max(EXPIRY_DAYS - 1, 1) / 365.0
            new_val = bs_call_price(S_up2, K, t2, RISK_FREE_RATE, sigma)
            pct_move = (new_val - entry) / entry * 100 if entry > 0 else 0
            print(f"  S0={S0:5.0f} sigma={sigma:.2f}: ATM premium={entry:6.1f}  "
                  f"after +2% underlying move -> {new_val:6.1f}  ({pct_move:+.1f}% option move)")


def nearest_strike_local(price):
    if price < 100: return round(price / 2.5) * 2.5
    if price < 250: return round(price / 5) * 5
    if price < 1000: return round(price / 10) * 10
    if price < 2500: return round(price / 20) * 20
    if price < 5000: return round(price / 50) * 50
    return round(price / 100) * 100


def get_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"))


def step2_directional_hit_rate(universe, holding_days_list=(2, 3, 5)):
    print("\n" + "=" * 74)
    print("STEP 2: Directional hit rate - of Stage-2/Stage-4 candidates, how many")
    print("        actually move >=2% in the PREDICTED direction within N days?")
    print("=" * 74)
    df = load_and_compute(universe, "2026-04-15", "2026-07-02")
    window_df = df.filter((pl.col("time") >= datetime.date(2026, 5, 1)) & (pl.col("time") <= datetime.date(2026, 6, 30)))
    series_cache = build_series_cache(df)

    bull_cands = baseline_candidates(window_df)
    bear_cands = breakdown_candidates(window_df)
    print(f"Bullish (predict UP) candidate-days: {len(bull_cands)}")
    print(f"Bearish (predict DOWN) candidate-days: {len(bear_cands)}")

    for holding_days in holding_days_list:
        print(f"\n--- Holding period: {holding_days} trading days ---")
        for label, cands, direction in [("UP (buy CE)", bull_cands, 1), ("DOWN (buy PE)", bear_cands, -1)]:
            hits, total, moves = 0, 0, []
            for row in cands.to_dicts():
                sym, date = row["symbol"], row["time"].strftime("%Y-%m-%d")
                series = series_cache.get(sym)
                if not series:
                    continue
                dl = [b["d"] for b in series]
                if date not in dl:
                    continue
                idx = dl.index(date)
                entry_idx = idx + 1
                exit_idx = entry_idx + holding_days
                if exit_idx >= len(series):
                    continue
                entry_price = series[entry_idx]["o"]
                exit_price = series[exit_idx]["c"]
                if entry_price <= 0:
                    continue
                pct_move = (exit_price - entry_price) / entry_price * 100
                moves.append(pct_move)
                total += 1
                if direction == 1 and pct_move >= 2.0:
                    hits += 1
                elif direction == -1 and pct_move <= -2.0:
                    hits += 1
            if total == 0:
                print(f"  {label}: n=0")
                continue
            hit_rate = 100 * hits / total
            avg_move = statistics.mean(moves)
            print(f"  {label}: n={total}  HIT RATE (>=2% in predicted dir)={hit_rate:.1f}%  "
                  f"avg actual move={avg_move:+.2f}%")


if __name__ == "__main__":
    step1_confirm_leverage_claim()
    universe = load_universe()
    step2_directional_hit_rate(universe)
