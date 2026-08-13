"""
V9 backtest: does a LONG_BUILDUP signal (option price up + OI up) on a
stock's ATM call, on the trading day BEFORE a real 5%+ up-move starts,
fire more often than on random non-mover F&O stock-days in the same window?

Constrained to what's actually testable: Kite only retains OI history for
CURRENTLY LISTED contracts (expired May/June series are gone), so this is
restricted to the 2026-06-10..2026-06-30 window where current contracts'
OI history overlaps our validated mover_categorization_with_capture.csv.
In that window ALL F&O-eligible movers happen to be UP moves (0 down moves -
market was in the confirmed TRENDING_UP regime) - so this backtest can only
validate the bullish (Long Buildup) side. The bearish (Short Buildup) side
that would matter for anticipating a crash like 2026-07-08 has only ONE
anecdotal data point so far (NIFTY/HDFCBANK on 07-07) - not enough for a
real precision/recall test. That side needs live forward data collection.
"""
import os
import csv
import random
import time
from datetime import datetime, timedelta

os.environ["TRADING_MODE"] = "HISTORICAL"
from dotenv import load_dotenv
load_dotenv()

from mesh_v1.oi_features import buildup_signal_for
from mesh_v1.features import conn

WIN_START, WIN_END = "2026-06-10", "2026-06-30"


def load_movers():
    rows = list(csv.DictReader(open("mover_categorization_with_capture.csv")))
    fo = set(r["Symbol"] for r in csv.DictReader(open("pipeline/fo_universe.csv")))
    sub = [r for r in rows if WIN_START <= r["start"] <= WIN_END and r["symbol"] in fo
           and float(r["move_pct"]) > 0]
    return sub, fo


def get_prior_close(symbol, d):
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT time::date, close FROM daily_ohlcv WHERE symbol=%s AND time::date < %s "
                "ORDER BY time DESC LIMIT 1", (symbol, d))
    row = cur.fetchone()
    c.close()
    return row


def get_all_trading_days_and_closes(symbol):
    c = conn()
    cur = c.cursor()
    cur.execute("SELECT time::date, close FROM daily_ohlcv WHERE symbol=%s AND time::date BETWEEN %s AND %s "
                "ORDER BY time", (symbol, WIN_START, WIN_END))
    rows = cur.fetchall()
    c.close()
    return rows


def main():
    movers, fo = load_movers()
    print(f"F&O-eligible up-movers in {WIN_START}..{WIN_END}: {len(movers)}", flush=True)

    mover_keys = set((r["symbol"], r["start"]) for r in movers)

    print("\n--- Checking Long Buildup on movers (treatment group) ---", flush=True)
    treatment_hits = 0
    treatment_checked = 0
    treatment_raw = []
    for i, r in enumerate(movers):
        sym, start = r["symbol"], r["start"]
        d = datetime.strptime(start, "%Y-%m-%d").date()
        prior = get_prior_close(sym, d)
        if not prior:
            continue
        prior_date, prior_close = prior
        sig = buildup_signal_for(sym, float(prior_close), d, opt_type="CE")
        treatment_checked += 1
        if sig:
            treatment_raw.append(sig)
            if sig["buildup"] == "LONG_BUILDUP":
                treatment_hits += 1
        if (i + 1) % 20 == 0:
            print(f"  ...{i+1}/{len(movers)} checked", flush=True)
        time.sleep(0.15)

    print(f"\nTreatment (real up-movers): {treatment_hits}/{treatment_checked} had LONG_BUILDUP on prior day's ATM call "
          f"= {100*treatment_hits/treatment_checked:.1f}%" if treatment_checked else "no data")

    print("\n--- Building control group (random F&O stock-days, non-movers) ---", flush=True)
    random.seed(42)
    control_candidates = []
    for sym in random.sample(sorted(fo), min(60, len(fo))):
        days = get_all_trading_days_and_closes(sym)
        for j in range(1, len(days)):
            d, close = days[j]
            if (sym, d.isoformat()) in mover_keys:
                continue
            control_candidates.append((sym, d, days[j - 1][1]))

    control_sample = random.sample(control_candidates, min(150, len(control_candidates)))
    print(f"Control pool: {len(control_candidates)} non-mover stock-days, sampling {len(control_sample)}", flush=True)

    control_hits = 0
    control_checked = 0
    control_raw = []
    for i, (sym, d, prior_close) in enumerate(control_sample):
        sig = buildup_signal_for(sym, float(prior_close), d, opt_type="CE")
        control_checked += 1
        if sig:
            control_raw.append(sig)
            if sig["buildup"] == "LONG_BUILDUP":
                control_hits += 1
        if (i + 1) % 30 == 0:
            print(f"  ...{i+1}/{len(control_sample)} checked", flush=True)
        time.sleep(0.15)

    print(f"\nControl (random non-mover days): {control_hits}/{control_checked} had LONG_BUILDUP "
          f"= {100*control_hits/control_checked:.1f}%" if control_checked else "no data")

    print("\n" + "=" * 70)
    print("RESULT (naive binary Buildup)")
    print("=" * 70)
    if treatment_checked and control_checked:
        t_rate = 100 * treatment_hits / treatment_checked
        c_rate = 100 * control_hits / control_checked
        lift = t_rate / c_rate if c_rate else float("inf")
        print(f"LONG_BUILDUP rate on real up-movers:  {t_rate:.1f}% (n={treatment_checked})")
        print(f"LONG_BUILDUP rate on random non-movers: {c_rate:.1f}% (n={control_checked})")
        print(f"Lift: {lift:.2f}x")

    print("\n" + "=" * 70)
    print("MAGNITUDE ANALYSIS: is the signal only in the EXTREME tail?")
    print("(mirrors the Pinocchio-bar lesson - naive threshold may drown signal in noise)")
    print("=" * 70)
    import statistics
    t_oi = [s["oi_chg_pct"] for s in treatment_raw]
    c_oi = [s["oi_chg_pct"] for s in control_raw]
    print(f"Treatment OI-change%: mean={statistics.mean(t_oi):+.1f}  median={statistics.median(t_oi):+.1f}  "
          f"(n={len(t_oi)})")
    print(f"Control   OI-change%: mean={statistics.mean(c_oi):+.1f}  median={statistics.median(c_oi):+.1f}  "
          f"(n={len(c_oi)})")
    for thresh in (30, 50, 75, 100):
        t_ext = sum(1 for s in treatment_raw if s["oi_chg_pct"] >= thresh)
        c_ext = sum(1 for s in control_raw if s["oi_chg_pct"] >= thresh)
        t_rate2 = 100 * t_ext / len(treatment_raw) if treatment_raw else 0
        c_rate2 = 100 * c_ext / len(control_raw) if control_raw else 0
        lift2 = t_rate2 / c_rate2 if c_rate2 else float("inf")
        print(f"  OI-change >= {thresh:3d}%:  movers {t_ext}/{len(treatment_raw)} ({t_rate2:.1f}%)  "
              f"vs control {c_ext}/{len(control_raw)} ({c_rate2:.1f}%)  lift={lift2:.2f}x")


if __name__ == "__main__":
    main()
