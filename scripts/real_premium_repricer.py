"""
S2 of docs/IMPLEMENTATION_PLAN.md V1: re-price June OPTIONS_IGNITION trades
with REAL option data (currently-listed July-expiry NFO contracts have full
daily premium/OI/volume history back to ~May 27 listing) instead of the
Black-Scholes proxy. Also prices the audit-vetoed set from
scripts/audit_veto_forensics.py (S1) to test whether the audit veto helps or
hurts on this route.

Exit rule = production V5: hard stop -50%, trailing stop arms at 2x with 10%
giveback (unlimited upside), 3-day max hold. Entry = real contract close on
signal day (same-day-close discipline). Contracts with zero volume on the
signal day are flagged UNTRADEABLE (can't realistically fill).

Usage: PYTHONPATH=. venv/bin/python3 scripts/real_premium_repricer.py
"""
import os
import json
import time
import statistics

os.environ["TRADING_MODE"] = "HISTORICAL"
from dotenv import load_dotenv
load_dotenv()

from kiteconnect import KiteConnect

from agents.conviction_router_agent import (
    compute_market_context, compute_signal_features, classify_route,
)

HARD_STOP = 0.50
ARM_MULT = 2.0
TRAIL_GIVEBACK = 0.10
MAX_HOLD_DAYS = 3

kite = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
kite.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'"))

print("Loading NFO chain...")
NFO = kite.instruments("NFO")


def nearest_listed_contract(underlying: str, spot: float, opt_type: str = "CE"):
    """Nearest-strike contract for the EARLIEST currently-listed expiry
    (July 2026 - the one whose history covers June)."""
    chain = [i for i in NFO if i["name"] == underlying and i["instrument_type"] == opt_type
             and i["expiry"] is not None]
    if not chain:
        return None
    earliest = sorted(set(i["expiry"] for i in chain))[0]
    chain = [i for i in chain if i["expiry"] == earliest]
    chain.sort(key=lambda i: abs(float(i["strike"]) - spot))
    return chain[0]


_series_cache = {}


def contract_series(token):
    if token in _series_cache:
        return _series_cache[token]
    try:
        bars = kite.historical_data(token, "2026-05-25", "2026-07-03", "day", oi=True)
    except Exception as e:
        bars = []
    time.sleep(0.34)
    _series_cache[token] = bars
    return bars


def sim_real_option(bars, signal_date: str):
    dl = [b["date"].strftime("%Y-%m-%d") for b in bars]
    if signal_date not in dl:
        return {"status": "NO_CONTRACT_DATA", "pnl_pct": None}
    idx = dl.index(signal_date)
    entry_bar = bars[idx]
    if entry_bar["volume"] == 0:
        return {"status": "UNTRADEABLE_ZERO_VOLUME", "pnl_pct": None}
    entry = entry_bar["close"]
    if entry <= 0.05:
        return {"status": "INVALID_PREMIUM", "pnl_pct": None}
    peak, armed, days = entry, False, 0
    i = idx
    while True:
        i += 1
        days += 1
        if i >= len(bars):
            last = bars[-1]["close"]
            return {"status": "DATA_END_OPEN", "pnl_pct": round((last - entry) / entry * 100, 2),
                    "entry_premium": entry, "exit_premium": last, "days_held": days - 1}
        p = bars[i]["close"]
        peak = max(peak, p)
        if peak >= entry * ARM_MULT:
            armed = True
        pnl = (p - entry) / entry * 100
        if armed and p <= peak * (1 - TRAIL_GIVEBACK):
            st = "TRAIL_STOP_AFTER_2X"
        elif p <= entry * (1 - HARD_STOP):
            st = "HARD_STOP_50PCT"
        elif days >= MAX_HOLD_DAYS:
            st = "MAX_HOLD_3DAY"
        else:
            continue
        return {"status": st, "pnl_pct": round(pnl, 2), "entry_premium": entry,
                "exit_premium": p, "days_held": days, "exit_date": bars[i]["date"].strftime("%Y-%m-%d")}


def reprice(signals, label):
    """signals: list of {symbol, signal_date, underlying_close} dicts."""
    results = []
    for s in signals:
        c = nearest_listed_contract(s["symbol"], s["underlying_close"])
        if c is None:
            results.append({**s, "status": "NO_NFO", "pnl_pct": None})
            continue
        bars = contract_series(c["instrument_token"])
        r = sim_real_option(bars, s["signal_date"])
        results.append({**s, "contract": c["tradingsymbol"], "strike": float(c["strike"]),
                        "lot_size": c["lot_size"], **r})

    tradeable = [r for r in results if r["pnl_pct"] is not None]
    print(f"\n=== {label} (REAL premiums) ===")
    print(f"signals={len(signals)}  tradeable={len(tradeable)}  "
          f"untradeable/no-data={len(results) - len(tradeable)}")
    if tradeable:
        pnls = [r["pnl_pct"] for r in tradeable]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        pf = sum(wins) / abs(sum(losses)) if losses and sum(losses) != 0 else float("inf")
        print(f"n={len(pnls)}  wins={len(wins)}  losses={len(losses)}  "
              f"WR={100 * len(wins) / len(pnls):.1f}%  PF={pf:.2f}  avg={statistics.mean(pnls):+.2f}%")
        for r in sorted(tradeable, key=lambda x: -x["pnl_pct"]):
            print(f"  {r['signal_date']} {r['symbol']:12s} {r.get('contract', ''):24s} "
                  f"{r['status']:20s} {r['pnl_pct']:+8.2f}%")
    return results


def main():
    # --- Approved OPTIONS_IGNITION trades from the fresh v6 run ---
    d = json.load(open("backtest_fast_results_v6_audit_forensics.json"))
    approved = []
    for t in d["trades"]:
        if t.get("route") != "OPTIONS_IGNITION":
            continue
        approved.append({"symbol": t["symbol"], "signal_date": t["signal_date"],
                         "underlying_close": (t.get("options") or {}).get("underlying_entry") or 0})
    # underlying_close fallback: fetch from DB if missing
    import psycopg2
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        dbname=os.getenv("POSTGRES_DB", "market_data"))
    cur = conn.cursor()

    def close_on(symbol, date):
        cur.execute("SELECT close FROM daily_ohlcv WHERE symbol=%s AND time::date=%s", (symbol, date))
        row = cur.fetchone()
        return float(row[0]) if row else None

    for s in approved:
        if not s["underlying_close"]:
            s["underlying_close"] = close_on(s["symbol"], s["signal_date"]) or 0

    # --- Audit-vetoed signals (S1 output), filtered to ignition profile ---
    vetoed_raw = json.load(open("audit_veto_log.json"))
    seen = set()
    vetoed = []
    ctx_cache = {}
    for v in vetoed_raw:
        key = (v["symbol"], v["date"])
        if key in seen:
            continue
        seen.add(key)
        date = v["date"]
        if date not in ctx_cache:
            ctx_cache[date] = compute_market_context(date)
        f = compute_signal_features(v["symbol"], date)
        if f is None:
            continue
        route = classify_route(f, ctx_cache[date]["breadth_pct"],
                               ctx_cache[date]["rs_by_symbol"].get(v["symbol"]))
        if route == "OPTIONS_IGNITION":
            vetoed.append({"symbol": v["symbol"], "signal_date": date,
                           "underlying_close": f["signal_close"], "grade": v["grade"]})
    conn.close()

    print(f"Approved ignition signals: {len(approved)}")
    print(f"Audit-vetoed signals with ignition profile: {len(vetoed)}")

    res_a = reprice(approved, "APPROVED (passed audit)")
    res_v = reprice(vetoed, "AUDIT-VETOED (killed by fundamental audit)")

    json.dump({"approved": res_a, "vetoed": res_v}, open("real_premium_repricing.json", "w"),
              indent=2, default=str)
    print("\nSaved -> real_premium_repricing.json")


if __name__ == "__main__":
    main()
