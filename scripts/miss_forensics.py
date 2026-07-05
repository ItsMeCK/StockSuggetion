"""
Miss forensics: for every >=5% 2-3 day mover in the analysis window, reconstruct
its indicator state on the SIGNAL DAY (the day before/at move start) and test it
against each of the screener's gate conditions to determine exactly WHY the
pipeline never surfaced it.

Read-only. Standalone (no app imports). Mirrors the filter math in
pipeline/screener.py run_pipeline() (stage_2_df / transition_df / flagged_momentum_df).

Usage: PYTHONPATH=. venv/bin/python3 scripts/miss_forensics.py
"""
import os
import json
from collections import Counter
import psycopg2
import polars as pl
from dotenv import load_dotenv

load_dotenv()

WINDOW_START = "2026-06-01"
WINDOW_END = "2026-07-02"
MIN_MOVE = 5.0


def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"),
    )


def load_universe():
    import csv
    syms = set()
    with open("pipeline/master_universe.csv") as f:
        for row in csv.DictReader(f):
            syms.add(row["Symbol"])
    return syms


def load_prices(universe):
    conn = get_conn()
    q = """
        SELECT time, symbol, open, high, low, close, volume FROM daily_ohlcv
        WHERE symbol = ANY(%(syms)s)
        ORDER BY symbol, time ASC
    """
    df = pl.read_database(q, conn, execute_options={"parameters": {"syms": list(universe)}})
    conn.close()
    return df


def compute_indicators(df):
    df = df.with_columns([
        pl.col("close").rolling_mean(10).over("symbol").alias("sma_10"),
        pl.col("close").rolling_mean(20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_mean(50).over("symbol").alias("sma_50"),
        pl.col("close").rolling_mean(200).over("symbol").alias("sma_200"),
        pl.col("close").rolling_mean(100).over("symbol").alias("sma_100"),
        pl.col("volume").rolling_mean(20).over("symbol").alias("vol_avg_20"),
    ])
    df = df.with_columns([pl.col("sma_200").fill_null(pl.col("sma_100")).fill_null(pl.col("sma_50"))])
    df = df.with_columns([
        (pl.col("sma_50") - pl.col("sma_50").shift(10).over("symbol")).alias("sma_50_slope_10d"),
        (((pl.col("close") - pl.col("sma_50")) / pl.col("sma_50")) * 100).alias("extension_pct"),
        (((pl.col("close") - pl.col("close").shift(10).over("symbol")) / pl.col("close").shift(10).over("symbol")) * 100).alias("roc_10"),
        (((pl.col("close") - pl.col("close").shift(20).over("symbol")) / pl.col("close").shift(20).over("symbol")) * 100).alias("roc_20"),
        pl.max_horizontal([
            pl.col("high") - pl.col("low"),
            (pl.col("high") - pl.col("close").shift(1).over("symbol")).abs(),
            (pl.col("low") - pl.col("close").shift(1).over("symbol")).abs(),
        ]).alias("tr"),
        (pl.col("close") * pl.col("volume")).alias("turnover"),
        pl.when(pl.col("high") > pl.col("low"))
          .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
          .otherwise(1.0).alias("close_range_pct"),
    ])
    df = df.with_columns([
        pl.col("tr").rolling_mean(3).over("symbol").alias("atr_3"),
        pl.col("tr").rolling_mean(20).over("symbol").alias("atr_20"),
    ])
    return df


def gate_report(row):
    """Evaluate the screener's actual gate conditions for one signal-day row.
    Returns (passed_any_gate, list_of_failure_reasons)."""
    c = row
    fails = []
    vol_scale = 1.0  # pulse 3 / EOD

    # --- Stage 2 gate (candidates) ---
    s2 = []
    if not (c["close"] > (c["sma_50"] or 1e18)):
        s2.append("below_50sma")
    est = (c["sma_50"] or 0) >= (c["sma_200"] or 1e18)
    rebirth = c["volume"] > 2.0 * vol_scale * (c["vol_avg_20"] or 1e18)
    if not (est or rebirth):
        s2.append("50sma_below_200sma_and_no_2x_volume")
    if not ((c["sma_50_slope_10d"] or -1e18) > -50):
        s2.append("slope_too_negative")
    ext = c["extension_pct"] if c["extension_pct"] is not None else 1e9
    ext_ok = (ext <= 5.0) or (ext <= 20.0 and (c["roc_10"] or 0) > (c["roc_20"] or 0)
                              and c["volume"] > 1.5 * vol_scale * (c["vol_avg_20"] or 1e18))
    if not ext_ok:
        s2.append("extension_fail(>5%_and_no_momentum_accel)" if ext <= 20 else "over_extended(>20%)")
    if not ((c["sma_10"] or 0) > (c["sma_20"] or 1e18)):
        s2.append("sma10_below_sma20")
    va = c["vol_avg_20"] or 1e18
    vol_ok = (c["volume"] >= 1.2 * vol_scale * va) or (c["volume"] <= 0.8 * vol_scale * va)
    if not vol_ok:
        s2.append("volume_in_dead_zone(0.8x-1.2x)")
    stage2_pass = len(s2) == 0

    # --- Transition gate (incubator) ---
    t = []
    if not (c["close"] > (c["sma_10"] or 1e18)): t.append("below_10sma")
    if not (c["close"] > (c["sma_20"] or 1e18)): t.append("below_20sma")
    if not (c["close"] >= 0.98 * (c["sma_50"] or 1e18)): t.append("below_98pct_of_50sma")
    ext_ok_t = (ext <= 5.0) or (ext <= 12.0 and (c["roc_10"] or 0) > (c["roc_20"] or 0))
    if not ext_ok_t: t.append("transition_extension_fail")
    vol_ok_t = (c["volume"] <= 0.8 * va) or (c["volume"] >= 1.5 * vol_scale * va)
    if not vol_ok_t: t.append("transition_volume_dead_zone")
    transition_pass = len(t) == 0

    # --- Flagged momentum gate ---
    fm = []
    if not (c["close"] > (c["sma_50"] or 1e18)):
        fm.append("below_50sma")
    normal = (ext > 15.0 and ext <= 25.0 and (c["roc_10"] or 0) > 5.0
              and c["volume"] >= 2.0 * vol_scale * va)
    titan = ((c["turnover"] or 0) >= 5_000_000_000 and c["volume"] >= 1.5 * vol_scale * va
             and (c["roc_10"] or 0) > 0)
    stealth = (c["volume"] >= 1.5 * vol_scale * va and 1.5 <= (c["roc_10"] or 0) <= 5.0
               and (c["atr_3"] or 1e18) <= (c["atr_20"] or 0) and (c["close_range_pct"] or 0) >= 0.75)
    if not (normal or titan or stealth):
        fm.append("no_momentum_pattern(normal/titan/stealth)")
    flagged_pass = len(fm) == 0

    passed = stage2_pass or transition_pass or flagged_pass
    return passed, {"stage2_fails": s2, "transition_fails": t, "flagged_fails": fm,
                    "stage2_pass": stage2_pass, "transition_pass": transition_pass,
                    "flagged_pass": flagged_pass}


def main():
    universe = load_universe()
    print(f"Universe: {len(universe)} symbols")
    df = load_prices(universe)
    print(f"Loaded {len(df)} rows")
    df = compute_indicators(df)

    # Find movers (close-to-close >=5% in 2-3 trading days) inside the window
    movers = {}
    for sym_df in df.partition_by("symbol"):
        sym = sym_df["symbol"][0]
        rows = sym_df.sort("time").to_dicts()
        for i in range(len(rows)):
            d_end = rows[i]["time"].strftime("%Y-%m-%d")
            if not (WINDOW_START <= d_end <= WINDOW_END):
                continue
            for lb in (2, 3):
                j = i - lb
                if j < 0:
                    continue
                d_start = rows[j]["time"].strftime("%Y-%m-%d")
                if not (WINDOW_START <= d_start <= WINDOW_END):
                    continue
                sc, ec = rows[j]["close"], rows[i]["close"]
                if not sc:
                    continue
                pct = (ec - sc) / sc * 100
                if pct >= MIN_MOVE:
                    prev = movers.get(sym)
                    if prev is None or pct > prev["move_pct"]:
                        movers[sym] = {"signal_row": rows[j], "start": d_start,
                                       "end": rows[i]["time"].strftime("%Y-%m-%d"),
                                       "move_pct": round(pct, 2)}
    print(f"Movers found in window: {len(movers)}")

    # Gate-test each mover on its signal day
    would_pass, would_fail = [], []
    fail_counter = Counter()
    for sym, m in movers.items():
        passed, detail = gate_report(m["signal_row"])
        rec = {"symbol": sym, "start": m["start"], "end": m["end"], "move_pct": m["move_pct"], **detail}
        if passed:
            would_pass.append(rec)
        else:
            would_fail.append(rec)
            # attribute to the gate that came CLOSEST (fewest fails)
            gates = [("stage2", detail["stage2_fails"]), ("transition", detail["transition_fails"]),
                     ("flagged", detail["flagged_fails"])]
            gates.sort(key=lambda g: len(g[1]))
            nearest_gate, nearest_fails = gates[0]
            for f in nearest_fails:
                fail_counter[f"{nearest_gate}:{f}"] += 1

    print("\n" + "=" * 74)
    print(f"GATE-1 FORENSICS on {len(movers)} movers (>= {MIN_MOVE}% in 2-3d, {WINDOW_START}..{WINDOW_END})")
    print("=" * 74)
    print(f"Would have PASSED gate 1 on signal day: {len(would_pass)}")
    print(f"Rejected at gate 1:                     {len(would_fail)}")

    print("\n--- Top rejection reasons (nearest-gate attribution) ---")
    for reason, cnt in fail_counter.most_common(15):
        print(f"  {cnt:4d}  {reason}")

    print("\n--- Movers that PASSED gate 1 (so were lost DOWNSTREAM in agents) ---")
    would_pass.sort(key=lambda x: -x["move_pct"])
    for r in would_pass[:25]:
        gates = [g for g, ok in [("S2", r["stage2_pass"]), ("TR", r["transition_pass"]), ("FM", r["flagged_pass"])] if ok]
        print(f"  {r['symbol']:12s} {r['start']} -> {r['end']}  {r['move_pct']:+6.1f}%  via {','.join(gates)}")

    with open("miss_forensics.json", "w") as f:
        json.dump({"passed_gate1": would_pass, "rejected_gate1": would_fail,
                   "fail_reasons": dict(fail_counter)}, f, indent=2, default=str)
    print(f"\nSaved -> miss_forensics.json")


if __name__ == "__main__":
    main()
