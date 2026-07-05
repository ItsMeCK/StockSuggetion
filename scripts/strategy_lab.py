"""
Strategy Lab: rapid experimentation over the approved-signal set.

Takes signals (from backtest_3pm_options_vs_equity.json), computes signal-day
features, then simulates rule variants for entry routing + exits. Pure price
math - runs in seconds, no LLM, no cache-staleness risk (reads prices from DB
fresh each run).

Usage: PYTHONPATH=. venv/bin/python3 scripts/strategy_lab.py
"""
import os
import json
import math
import numpy as np
import psycopg2
from dotenv import load_dotenv

load_dotenv()

from scripts.backtest_3pm_options_vs_equity import (
    bs_call_price, nearest_strike, estimate_iv,
    RISK_FREE_RATE, EXPIRY_DAYS,
)


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"))


_series_cache = {}

def series(sym):
    if sym in _series_cache:
        return _series_cache[sym]
    conn = get_conn(); cur = conn.cursor()
    cur.execute("SELECT time::date, open, high, low, close, volume FROM daily_ohlcv WHERE symbol=%s ORDER BY time ASC", (sym,))
    s = [{"d": r[0].strftime("%Y-%m-%d"), "o": float(r[1]), "h": float(r[2]),
          "l": float(r[3]), "c": float(r[4]), "v": float(r[5])} for r in cur.fetchall()]
    cur.close(); conn.close()
    _series_cache[sym] = s
    return s


def signal_features(sym, signal_date):
    s = series(sym)
    dl = [b["d"] for b in s]
    if signal_date not in dl:
        return None
    i = dl.index(signal_date)
    if i < 21 or i + 1 >= len(s):
        return None
    sig, nxt = s[i], s[i + 1]
    vol20 = sum(b["v"] for b in s[i - 20:i]) / 20
    rng = sig["h"] - sig["l"]
    return {
        "sym": sym, "signal_date": signal_date, "idx": i, "series": s,
        "vol_ratio": sig["v"] / vol20 if vol20 else 1.0,
        "close_range": (sig["c"] - sig["l"]) / rng if rng > 0 else 1.0,
        "sig_day_ret": (sig["c"] - s[i - 1]["c"]) / s[i - 1]["c"] * 100,
        "broke_2d_high": sig["c"] >= max(b["c"] for b in s[i - 2:i]),
        "gap_pct": (nxt["o"] - sig["c"]) / sig["c"] * 100,
        "green": sig["c"] >= sig["o"],
    }


# ---------------------------------------------------------------------------
# Exit simulators (parameterized)
# ---------------------------------------------------------------------------
def sim_equity(f, target_pct=0.10, stop_pct=0.05, max_days=2, day1_bail_pct=None):
    """day1_bail_pct: if set, exit at day-1 close when position is red by more than this %."""
    s, i = f["series"], f["idx"]
    e_idx = i + 1
    entry = s[e_idx]["o"]
    target, stop = entry * (1 + target_pct), entry * (1 - stop_pct)
    for off in range(max_days):
        idx = e_idx + off
        if idx >= len(s):
            return None
        bar = s[idx]
        if bar["l"] <= stop:
            return -stop_pct * 100
        if bar["h"] >= target:
            return target_pct * 100
        if day1_bail_pct is not None and off == 0:
            d1 = (bar["c"] - entry) / entry * 100
            if d1 < -day1_bail_pct:
                return d1
    last = s[min(e_idx + max_days - 1, len(s) - 1)]
    return (last["c"] - entry) / entry * 100


def sim_option(f, target_mult=2.0, cushion=0.30, trail=0.10, hard_stop=0.50, theta_cut_days=None):
    """theta_cut_days: if premium below entry after N days, exit (theta bleed cut)."""
    s, i = f["series"], f["idx"]
    e_idx = i + 1
    S0 = s[e_idx]["o"]
    K = nearest_strike(S0)
    sigma = estimate_iv([b["c"] for b in s[max(0, e_idx - 20):e_idx]])
    ep = bs_call_price(S0, K, EXPIRY_DAYS / 365.0, RISK_FREE_RATE, sigma)
    if ep <= 0.05:
        return None
    peak, armed, idx, days = ep, False, e_idx, 0
    while True:
        idx += 1; days += 1
        if idx >= len(s):
            return None  # still open - exclude from WR
        S_t = s[idx]["c"]
        t = max(EXPIRY_DAYS - days, 1)
        p = bs_call_price(S_t, K, t / 365.0, RISK_FREE_RATE, sigma)
        peak = max(peak, p)
        if peak >= ep * (1 + cushion):
            armed = True
        if p >= ep * target_mult:
            return (target_mult - 1) * 100
        if armed and p <= peak * (1 - trail):
            return (p - ep) / ep * 100
        if p <= ep * (1 - hard_stop):
            return -hard_stop * 100
        if theta_cut_days and days >= theta_cut_days and p < ep:
            return (p - ep) / ep * 100
        if t <= 1:
            return (p - ep) / ep * 100


def wr(pnls):
    pnls = [p for p in pnls if p is not None]
    if not pnls:
        return 0.0, 0, 0.0
    wins = sum(1 for p in pnls if p > 0)
    return 100 * wins / len(pnls), len(pnls), sum(pnls) / len(pnls)


def report(label, pnls):
    w, n, avg = wr(pnls)
    print(f"  {label:58s} n={n:3d}  WR={w:5.1f}%  avg={avg:+7.2f}%")
    return w, n


def main():
    data = json.load(open("backtest_3pm_options_vs_equity.json"))
    trades = data["trades"] if isinstance(data, dict) else data
    sigs = []
    seen = set()
    for t in trades:
        key = (t["symbol"], t["signal_date"])
        if key in seen:
            continue
        seen.add(key)
        f = signal_features(t["symbol"], t["signal_date"])
        if f:
            sigs.append(f)
    print(f"Signals with features: {len(sigs)}\n")

    # ---------------- EQUITY EXPERIMENTS ----------------
    print("=== EQUITY leg (baseline exits: 10%T/5%S/2d) ===")
    report("ALL signals, baseline exits", [sim_equity(f) for f in sigs])

    eq_profile = [f for f in sigs if 0.5 <= f["sig_day_ret"] <= 3.5 and f["vol_ratio"] < 2.5]
    report("EQ-profile (sig-day +0.5..3.5%, vol<2.5x)", [sim_equity(f) for f in eq_profile])

    eq_p2 = [f for f in sigs if 1.0 <= f["sig_day_ret"] <= 3.0 and f["vol_ratio"] < 2.0]
    report("EQ-profile tight (+1..3%, vol<2x)", [sim_equity(f) for f in eq_p2])

    report("EQ-profile tight + day1 bail -3%", [sim_equity(f, day1_bail_pct=3.0) for f in eq_p2])
    report("EQ-profile tight + 3-day hold", [sim_equity(f, max_days=3) for f in eq_p2])
    report("EQ-profile tight + 5%T/3%S/2d", [sim_equity(f, 0.05, 0.03) for f in eq_p2])

    # ---------------- OPTIONS EXPERIMENTS ----------------
    print("\n=== OPTIONS leg (baseline exits: 2x/trail30-10/hard50) ===")
    report("ALL signals, baseline exits", [sim_option(f) for f in sigs])

    ign = [f for f in sigs if f["broke_2d_high"] and f["vol_ratio"] >= 1.5 and f["close_range"] >= 0.6]
    report("IGNITION (2d-high+vol1.5x+closeTop40)", [sim_option(f) for f in ign])

    ign_c = [f for f in ign if f["gap_pct"] >= 0]
    report("IGNITION + gap-up confirm", [sim_option(f) for f in ign_c])

    report("IGNITION + confirm + theta-cut 5d", [sim_option(f, theta_cut_days=5) for f in ign_c])
    report("IGNITION + confirm + hard35", [sim_option(f, hard_stop=0.35) for f in ign_c])
    report("IGNITION + confirm + 1.8x target", [sim_option(f, target_mult=1.8) for f in ign_c])
    report("IGNITION + confirm + 1.5x target", [sim_option(f, target_mult=1.5) for f in ign_c])
    report("IGNITION + confirm + 1.5x + theta-cut5", [sim_option(f, target_mult=1.5, theta_cut_days=5) for f in ign_c])

    ign_strict = [f for f in ign_c if f["close_range"] >= 0.75 and f["green"]]
    report("IGNITION strict (closeTop25+green)+confirm", [sim_option(f) for f in ign_strict])


if __name__ == "__main__":
    main()
