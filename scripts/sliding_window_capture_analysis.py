"""
Sliding-window mover detection + real-screener capture-rate measurement.

Unlike the earlier fixed "2 or 3 day" mover script, this allows the move to
develop over up to N days with chop in between (e.g. +1%, +2%, -1%, +3% ->
cumulative +5% by day 4) - a more realistic definition of "this stock moved
5%" than requiring a clean monotonic run.

For each detected move, checks whether the REAL production screener
(pipeline/screener.py, called directly - not a reimplementation, learned that
lesson already this session) flagged the stock as a candidate on the day
BEFORE the move started (the day we'd need to have seen it to act).

Goal: measure current capture rate, then identify what to loosen to reach
>=50% capture, tracked separately from any NFO/options concern per instruction.
"""
import os
import json
import argparse
from dotenv import load_dotenv

load_dotenv()
os.environ["TRADING_MODE"] = "HISTORICAL"
import sys
sys.path.insert(0, "/Users/poonamsalke/Workplace/StockSuggetion")
os.chdir("/Users/poonamsalke/Workplace/StockSuggetion")

import csv
import psycopg2
from pipeline.screener import SovereignScreener

MIN_MOVE_PCT = 5.0
MAX_WINDOW_DAYS = 10  # allow the move to develop over up to 10 trading days, with chop


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"))


def load_universe():
    syms = set()
    with open("pipeline/master_universe.csv") as f:
        for row in csv.DictReader(f):
            if row.get("Symbol"):
                syms.add(row["Symbol"])
    return syms


def fetch_all_price_data(start_date, end_date, universe=None):
    """universe=None means the old, wider (loose-regex) DB scope; a set
    restricts to exactly the intended tradeable universe (master_universe.csv) -
    the fairer, more actionable measurement (SME/-SM symbols etc. were never
    in scope for this system to begin with)."""
    conn = get_conn()
    cur = conn.cursor()
    if universe:
        cur.execute("""
            SELECT symbol, time::date, close FROM daily_ohlcv
            WHERE symbol = ANY(%s) AND time::date >= %s AND time::date <= %s
            ORDER BY symbol, time ASC
        """, (list(universe), start_date, end_date))
    else:
        cur.execute("""
            SELECT symbol, time::date, close FROM daily_ohlcv
            WHERE symbol !~ '(NIFTY|BOND|INDEX|BEES|-SG\\d|-ND\\d|-GS\\d)'
              AND time::date >= %s AND time::date <= %s
            ORDER BY symbol, time ASC
        """, (start_date, end_date))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    by_symbol = {}
    for sym, d, c in rows:
        if c is None:
            continue
        by_symbol.setdefault(sym, []).append({"date": d.strftime("%Y-%m-%d"), "close": float(c)})
    return by_symbol


def detect_sliding_window_moves(series, min_move_pct, max_window):
    """For each start day, scan forward up to max_window days; if cumulative
    return from start ever crosses +min_move_pct, record the move (first day
    it crosses) and jump past the end of that move to avoid trivial overlap."""
    events = []
    n = len(series)
    i = 0
    while i < n:
        entry_price = series[i]["close"]
        if not entry_price:
            i += 1
            continue
        found = None
        for j in range(i + 1, min(i + max_window + 1, n)):
            cum = (series[j]["close"] - entry_price) / entry_price * 100
            if cum >= min_move_pct:
                found = (j, cum)
                break
        if found:
            j, cum = found
            events.append({
                "start_date": series[i]["date"], "end_date": series[j]["date"],
                "days": j - i, "move_pct": round(cum, 2),
            })
            i = j + 1  # jump past this move, don't re-trigger on overlapping sub-windows
        else:
            i += 1
    return events


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--start", default="2026-05-01")
    ap.add_argument("--end", default="2026-06-30")
    args = ap.parse_args()

    universe = load_universe()
    print(f"Reading price data {args.start} -> {args.end} (sliding window, max {MAX_WINDOW_DAYS} days, allows chop), "
          f"restricted to master_universe.csv ({len(universe)} symbols)...")
    by_symbol = fetch_all_price_data(args.start, args.end, universe=universe)
    print(f"Loaded {len(by_symbol)} symbols.")

    all_moves = []
    for sym, series in by_symbol.items():
        for e in detect_sliding_window_moves(series, MIN_MOVE_PCT, MAX_WINDOW_DAYS):
            all_moves.append({"symbol": sym, **e})

    all_moves.sort(key=lambda x: -x["move_pct"])
    print(f"\nTotal sliding-window 5%+ moves found: {len(all_moves)}")

    # Cross-check against the REAL screener for each move's start date
    print("\nChecking REAL screener output for each move's trigger day "
          "(this calls the actual pipeline/screener.py, cached per date)...")
    screener = SovereignScreener()
    screener_cache = {}  # date -> set of ALL candidates (stage2+incubator+flagged)

    captured, missed = [], []
    for m in all_moves:
        date = m["start_date"]
        if date not in screener_cache:
            try:
                candidates, incubator, flagged, base_scores, macro_regime = screener.run_pipeline(target_date=date)
                screener_cache[date] = set(candidates) | set(incubator) | set(flagged)
            except Exception as e:
                print(f"  screener failed for {date}: {e}")
                screener_cache[date] = set()
        if m["symbol"] in screener_cache[date]:
            captured.append(m)
        else:
            missed.append(m)

    print("\n" + "=" * 78)
    print(f"SLIDING-WINDOW CAPTURE RATE: {len(all_moves)} real moves >= {MIN_MOVE_PCT}% "
          f"({args.start} -> {args.end})")
    print("=" * 78)
    print(f"Captured (real screener flagged the stock as ANY candidate type on trigger day): "
          f"{len(captured)}  ({100*len(captured)/len(all_moves):.1f}%)")
    print(f"Missed: {len(missed)}  ({100*len(missed)/len(all_moves):.1f}%)")
    print(f"\nTARGET: >=50% capture. Current: {100*len(captured)/len(all_moves):.1f}%. "
          f"Gap: need {max(0, round(0.5*len(all_moves)) - len(captured))} more captures.")

    with open("sliding_window_capture_analysis.json", "w") as f:
        json.dump({"captured": captured, "missed": missed}, f, indent=2, default=str)
    print("\nSaved -> sliding_window_capture_analysis.json")

    print("\n--- Top 20 missed moves (by size) ---")
    for m in missed[:20]:
        print(f"  {m['symbol']:12s} {m['start_date']} -> {m['end_date']} ({m['days']}d)  {m['move_pct']:+.1f}%")


if __name__ == "__main__":
    main()
