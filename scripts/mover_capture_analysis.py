"""
Standalone mover-capture analysis. Does NOT import any application code
(no core/, agents/, pipeline/ modules) - reads only raw data:

  1. daily_ohlcv - to find every stock that moved >=5% (close-to-close)
     within any 2 or 3 trading-day window over the last 20 trading days.
  2. trade_events - to check whether the system ever actually recorded a
     SIGNALED / AMO_PLACED / ACTIVE ledger entry for that ticker at or just
     before the move started (i.e. did we "capture" it).

Also flags misses that fall inside the known ingestion/ledger outage window
(data stopped updating 2026-06-11, last real signal 2026-06-14) so a miss
caused by "the system was offline" isn't confused with a genuine strategy miss.

Usage: python3 scripts/mover_capture_analysis.py
"""
import os
import json
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

MIN_MOVE_PCT = 5.0
LOOKBACK_WINDOW_DAYS = 20
LEDGER_LOOKBACK_DAYS = 3          # how far before the move's start date a signal still "counts"
OUTAGE_DATA_STOP = "2026-06-11"   # daily_ohlcv stopped updating (found earlier this session)
OUTAGE_LEDGER_STOP = "2026-06-14" # last real SIGNALED/AMO_PLACED entry before this analysis


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"),
    )


def fetch_all_price_data():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT symbol, time::date, close
        FROM daily_ohlcv
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


def fetch_ledger_events():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT ticker, status, system_time::date
        FROM trade_events
        WHERE status IN ('SIGNALED', 'AMO_PLACED', 'ACTIVE')
        ORDER BY ticker, system_time ASC
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    by_ticker = {}
    for ticker, status, d in rows:
        by_ticker.setdefault(ticker, []).append({"status": status, "date": d.strftime("%Y-%m-%d")})
    return by_ticker


def last_n_trading_dates(by_symbol, n):
    all_dates = sorted({b["date"] for series in by_symbol.values() for b in series})
    return all_dates[-n:]


def detect_move_events(series, min_move_pct):
    """Every 2-day and 3-day close-to-close window with a >= min_move_pct gain,
    merged into non-overlapping clusters per symbol (keeping the strongest move)."""
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
            if pct >= min_move_pct:
                events.append({
                    "start_date": series[j]["date"],
                    "end_date": series[i]["date"],
                    "days": lookback,
                    "move_pct": round(pct, 2),
                })

    events.sort(key=lambda e: (e["start_date"], -e["move_pct"]))
    merged = []
    for e in events:
        if merged and e["start_date"] <= merged[-1]["end_date"]:
            if e["move_pct"] > merged[-1]["move_pct"]:
                merged[-1]["move_pct"] = e["move_pct"]
                merged[-1]["days"] = e["days"]
            merged[-1]["start_date"] = min(merged[-1]["start_date"], e["start_date"])
            merged[-1]["end_date"] = max(merged[-1]["end_date"], e["end_date"])
        else:
            merged.append(dict(e))
    return merged


def find_ledger_matches(ticker, start_date, ledger_by_ticker, lookback_days):
    events = ledger_by_ticker.get(ticker, [])
    if not events:
        return None
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    window_start = (start_dt - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
    matches = [e for e in events if window_start <= e["date"] <= start_date]
    return matches if matches else None


def main():
    print("Reading price data from daily_ohlcv (no application code imported)...")
    by_symbol = fetch_all_price_data()
    print(f"Loaded {len(by_symbol)} symbols.")

    print("Reading trade_events ledger for SIGNALED/AMO_PLACED/ACTIVE history...")
    ledger = fetch_ledger_events()
    print(f"Ledger has signal history for {len(ledger)} tickers.")

    window_dates = last_n_trading_dates(by_symbol, LOOKBACK_WINDOW_DAYS)
    window_start_date, window_end_date = window_dates[0], window_dates[-1]
    print(f"Analysis window: {window_start_date} -> {window_end_date} ({len(window_dates)} trading days)")

    all_movers = []
    for sym, series in by_symbol.items():
        for e in detect_move_events(series, MIN_MOVE_PCT):
            if e["end_date"] < window_start_date or e["end_date"] > window_end_date:
                continue
            matches = find_ledger_matches(sym, e["start_date"], ledger, LEDGER_LOOKBACK_DAYS)
            all_movers.append({
                "symbol": sym,
                **e,
                "captured": bool(matches),
                "ledger_matches": matches,
                "during_known_outage": e["start_date"] > OUTAGE_LEDGER_STOP,
            })

    all_movers.sort(key=lambda x: -x["move_pct"])

    captured = [m for m in all_movers if m["captured"]]
    missed = [m for m in all_movers if not m["captured"]]
    missed_outage = [m for m in missed if m["during_known_outage"]]
    missed_active = [m for m in missed if not m["during_known_outage"]]

    print("\n" + "=" * 78)
    print(f"MOVER CAPTURE ANALYSIS: {len(all_movers)} moves >= {MIN_MOVE_PCT}% in 2-3 days "
          f"({window_start_date} -> {window_end_date})")
    print("=" * 78)
    print(f"Captured (ledger has SIGNALED/AMO_PLACED/ACTIVE at/near move start): {len(captured)}")
    print(f"Missed entirely:                                                    {len(missed)}")
    print(f"  - during the known data/ledger outage (after {OUTAGE_LEDGER_STOP}):        {len(missed_outage)}")
    print(f"  - during a period the system WAS actively signaling:               {len(missed_active)}")

    print("\n--- Top 25 movers overall ---")
    print("| Symbol | Start | End | Days | Move% | Status |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- |")
    for m in all_movers[:25]:
        status = "CAPTURED" if m["captured"] else ("MISSED (outage)" if m["during_known_outage"] else "MISSED")
        print(f"| {m['symbol']} | {m['start_date']} | {m['end_date']} | {m['days']} | {m['move_pct']:+.1f}% | {status} |")

    print("\n--- Missed movers WHILE the system was actively signaling (most important) ---")
    missed_active.sort(key=lambda x: -x["move_pct"])
    print("| Symbol | Start | End | Days | Move% |")
    print("| :--- | :--- | :--- | :--- | :--- |")
    for m in missed_active[:30]:
        print(f"| {m['symbol']} | {m['start_date']} | {m['end_date']} | {m['days']} | {m['move_pct']:+.1f}% |")

    with open("mover_capture_analysis.json", "w") as f:
        json.dump(all_movers, f, indent=2, default=str)
    print(f"\nSaved {len(all_movers)} records to mover_capture_analysis.json")


if __name__ == "__main__":
    main()
