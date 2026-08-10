"""
For every 5% move (May & June), categorize it by the book-pattern it fit on
the day the move STARTED. Self-contained: detects the moves DIRECTLY from the
current (timezone-correct) DB frame, so dates always match (the old
sliding_window JSON was pre-timezone-fix and one day off -> NO_DATA rows).

Prints: (1) summary counts per category by month, (2) full per-move table to
CSV + printed top-25-by-size for each month.
"""
import os, json, csv
from collections import defaultdict
from scripts.mover_pattern_classifier import build_frame, archetypes, load_universe

MIN_MOVE, MAX_WIN = 5.0, 10
WIN_START, WIN_END = "2026-05-01", "2026-06-30"

PRIORITY = ["base_breakout", "volume_thrust_2x", "ma_stack_rising", "pullback_in_uptrend",
            "near_20d_high", "above_rising_20sma", "momentum_roc10>5", "hh_hl_uptrend",
            "strong_close", "rs_beats_nifty", "gap_up", "stage2_breakout"]


def primary(fired):
    for p in PRIORITY:
        if fired.get(p):
            return p
    return "UNCLASSIFIED"


def detect_moves(series):
    """series sorted by time. Cumulative +5% within MAX_WIN days, jump past."""
    ev, n, i = [], len(series), 0
    while i < n:
        c0 = series[i]["close"]
        if not c0:
            i += 1; continue
        found = None
        for j in range(i + 1, min(i + MAX_WIN + 1, n)):
            if (series[j]["close"] - c0) / c0 * 100 >= MIN_MOVE:
                found = j; break
        if found:
            ev.append((i, found, round((series[found]["close"] - c0) / c0 * 100, 1)))
            i = found + 1
        else:
            i += 1
    return ev


def main():
    universe = load_universe()
    df = build_frame(universe)
    by_sym = defaultdict(list)
    for r in df.sort(["symbol", "time"]).to_dicts():
        by_sym[r["symbol"]].append(r)

    rows = []
    for sym, series in by_sym.items():
        for i, j, mv in detect_moves(series):
            sd = series[i]["time"].strftime("%Y-%m-%d")
            if not (WIN_START <= sd <= WIN_END):
                continue
            r = series[i]
            if r.get("atr14") is None:
                prim, pats = "NEW_LISTING_NO_HIST", ""
            else:
                fired = archetypes(r)
                prim = primary(fired)
                pats = ",".join(k for k in PRIORITY if fired.get(k))
            rows.append({"month": "May" if sd < "2026-06-01" else "June", "symbol": sym,
                         "start": sd, "end": series[j]["time"].strftime("%Y-%m-%d"),
                         "days": j - i, "move_pct": mv, "primary": prim, "all_patterns": pats})

    print("=" * 70)
    print("5% MOVERS by PRIMARY pattern (self-detected, timezone-correct)")
    print("=" * 70)
    print(f"{'Category':22s} {'May':>6s} {'June':>6s} {'Total':>7s}  {'%ofAll':>7s}")
    cats = defaultdict(lambda: [0, 0])
    for r in rows:
        cats[r["primary"]][0 if r["month"] == "May" else 1] += 1
    total = len(rows)
    for cat in PRIORITY + ["UNCLASSIFIED", "NEW_LISTING_NO_HIST"]:
        if cat in cats:
            may, jun = cats[cat]
            print(f"{cat:22s} {may:6d} {jun:6d} {may+jun:7d}  {100*(may+jun)/total:6.1f}%")
    tm = sum(1 for r in rows if r["month"] == "May"); tj = total - tm
    print(f"{'TOTAL':22s} {tm:6d} {tj:6d} {total:7d}")

    with open("mover_categorization.csv", "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["month", "symbol", "start", "end", "days", "move_pct", "primary", "all_patterns"])
        w.writeheader(); w.writerows(sorted(rows, key=lambda x: (x["month"], -x["move_pct"])))
    print("\nFull per-move table -> mover_categorization.csv")

    for mth in ("May", "June"):
        sub = sorted([r for r in rows if r["month"] == mth], key=lambda x: -x["move_pct"])[:25]
        print(f"\n--- {mth}: top 25 movers by size ---")
        print(f"{'Start':11s} {'Symbol':12s} {'Mv%':>5s} {'D':>2s} {'Primary':20s} {'#pat'}")
        for r in sub:
            npat = len(r["all_patterns"].split(",")) if r["all_patterns"] else 0
            print(f"{r['start']:11s} {r['symbol']:12s} {r['move_pct']:5.1f} {r['days']:2d} {r['primary']:20s} {npat}")


if __name__ == "__main__":
    main()
