"""
Downside mirror of scripts/mover_capture_analysis.py: finds every stock that
FELL >= 5% (close-to-close) within any 2 or 3 trading-day window, over both
the 20-day window and the full ~2-year dataset. Standalone, no app code,
direct SQL reads only.

Why this matters: the CE-only (long call) book was long-only and got crushed
in the May corrective regime (41.9% WR) because it was only ever looking for
upside breakouts in a market that was mostly breaking down. This quantifies
how much downside opportunity existed that a long-PE (buy puts, capital-
efficient - no margin, just premium) book could have captured instead.

Usage: python3 scripts/downside_mover_analysis.py [--days 20|full]
"""
import os
import json
import argparse
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

MIN_MOVE_PCT = 5.0


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"))


def fetch_all_price_data(start_date=None):
    conn = get_conn()
    cur = conn.cursor()
    if start_date:
        cur.execute("""
            SELECT symbol, time::date, close FROM daily_ohlcv
            WHERE symbol !~ '(NIFTY|BOND|INDEX|BEES|-SG\\d|-ND\\d|-GS\\d)' AND time::date >= %s
            ORDER BY symbol, time ASC
        """, (start_date,))
    else:
        cur.execute("""
            SELECT symbol, time::date, close FROM daily_ohlcv
            WHERE symbol !~ '(NIFTY|BOND|INDEX|BEES|-SG\\d|-ND\\d|-GS\\d)'
            ORDER BY symbol, time ASC
        """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    by_symbol = {}
    for sym, d, c in rows:
        if c is None:
            continue
        by_symbol.setdefault(sym, []).append({"date": d.strftime("%Y-%m-%d"), "close": float(c)})
    return by_symbol


def last_n_trading_dates(by_symbol, n):
    all_dates = sorted({b["date"] for series in by_symbol.values() for b in series})
    return all_dates[-n:]


def detect_down_move_events(series, min_move_pct):
    """Every 2-day and 3-day close-to-close window with a >= min_move_pct DROP,
    merged into non-overlapping clusters per symbol (keeping the strongest drop)."""
    events = []
    n = len(series)
    for i in range(2, n):
        for lookback in (2, 3):
            j = i - lookback
            if j < 0:
                continue
            s_close = series[j]["close"]
            e_close = series[i]["close"]
            if not s_close:
                continue
            pct = (e_close - s_close) / s_close * 100
            if pct <= -min_move_pct:
                events.append({
                    "start_date": series[j]["date"],
                    "end_date": series[i]["date"],
                    "days": lookback,
                    "move_pct": round(pct, 2),
                })

    events.sort(key=lambda e: (e["start_date"], e["move_pct"]))  # ascending (most negative first)
    merged = []
    for e in events:
        if merged and e["start_date"] <= merged[-1]["end_date"]:
            if e["move_pct"] < merged[-1]["move_pct"]:
                merged[-1]["move_pct"] = e["move_pct"]
                merged[-1]["days"] = e["days"]
            merged[-1]["start_date"] = min(merged[-1]["start_date"], e["start_date"])
            merged[-1]["end_date"] = max(merged[-1]["end_date"], e["end_date"])
        else:
            merged.append(dict(e))
    return merged


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--window", choices=["20day", "full"], default="20day")
    args = ap.parse_args()

    print("Reading price data from daily_ohlcv (no application code imported)...")
    start_date = None
    if args.window == "20day":
        # Get last 20 trading dates first via a quick probe, then filter precisely.
        by_symbol_probe = fetch_all_price_data(start_date="2026-05-01")
        window_dates = last_n_trading_dates(by_symbol_probe, 20)
        by_symbol = by_symbol_probe
        window_start_date, window_end_date = window_dates[0], window_dates[-1]
    else:
        by_symbol = fetch_all_price_data()
        all_dates = sorted({b["date"] for series in by_symbol.values() for b in series})
        window_start_date, window_end_date = all_dates[0], all_dates[-1]

    print(f"Loaded {len(by_symbol)} symbols.")
    print(f"Analysis window: {window_start_date} -> {window_end_date}")

    all_down_movers = []
    for sym, series in by_symbol.items():
        for e in detect_down_move_events(series, MIN_MOVE_PCT):
            if e["end_date"] < window_start_date or e["end_date"] > window_end_date:
                continue
            all_down_movers.append({"symbol": sym, **e})

    all_down_movers.sort(key=lambda x: x["move_pct"])  # most negative first

    print("\n" + "=" * 78)
    print(f"DOWNSIDE MOVER ANALYSIS: {len(all_down_movers)} moves <= -{MIN_MOVE_PCT}% in 2-3 days "
          f"({window_start_date} -> {window_end_date})")
    print("=" * 78)

    print("\n--- Top 25 downside movers ---")
    print("| Symbol | Start | End | Days | Move% |")
    print("| :--- | :--- | :--- | :--- | :--- |")
    for m in all_down_movers[:25]:
        print(f"| {m['symbol']} | {m['start_date']} | {m['end_date']} | {m['days']} | {m['move_pct']:+.1f}% |")

    out_path = f"downside_mover_analysis_{args.window}.json"
    with open(out_path, "w") as f:
        json.dump(all_down_movers, f, indent=2, default=str)
    print(f"\nSaved {len(all_down_movers)} records to {out_path}")

    # Quick comparison vs upside, if the upside file exists for the same window
    if args.window == "20day" and os.path.exists("mover_capture_analysis.json"):
        upside = json.load(open("mover_capture_analysis.json"))
        print(f"\nFor comparison: upside (>=+{MIN_MOVE_PCT}%) movers in the same 20-day window: {len(upside)}")
        print(f"Downside (<=-{MIN_MOVE_PCT}%) movers: {len(all_down_movers)}")


if __name__ == "__main__":
    main()
