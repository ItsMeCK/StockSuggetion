"""
V4-S1 — MOVER PATTERN CLASSIFIER. For every real 5% move (May-June), compute
the Pring+Shannon feature set on the day the move STARTS, and compare movers
(positive class) vs a CONTROL of random non-mover (symbol,date). For each
book-archetype report:
  capture% = fires on this many % of the 5% movers (how many moves it catches)
  base%    = fires on this many % of control non-movers (false-positive rate)
  lift     = capture / base  (>1 = the pattern really precedes moves)
  precision-ish = movers_fired / (movers_fired + control_fired_scaled)

This tells us WHICH patterns to build detector agents for (high capture AND
high lift), and which are noise.

Vectorized with polars: load the whole universe once, compute all indicators,
then evaluate each archetype per row.

Usage: PYTHONPATH=. venv/bin/python3 scripts/mover_pattern_classifier.py
"""
import os, json, random
import psycopg2, polars as pl
from dotenv import load_dotenv
load_dotenv()

WIN_START, WIN_END = "2026-04-01", "2026-06-30"


def conn():
    return psycopg2.connect(host="localhost", port=5432, user=os.getenv("POSTGRES_USER", "quant"),
                            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"), dbname=os.getenv("POSTGRES_DB", "market_data"))


def load_universe():
    import csv
    with open("pipeline/master_universe.csv") as f:
        return [r["Symbol"] for r in csv.DictReader(f) if r.get("Symbol")]


def build_frame(universe):
    c = conn()
    df = pl.read_database(
        "SELECT time::date as time, symbol, open, high, low, close, volume FROM daily_ohlcv WHERE symbol = ANY(%(s)s) ORDER BY symbol, time",
        c, execute_options={"parameters": {"s": universe}})
    c.close()
    # NIFTY for relative strength
    c = conn()
    nif = pl.read_database("SELECT time::date as time, close as nif_close FROM daily_ohlcv WHERE symbol='NIFTY 50' ORDER BY time", c)
    c.close()
    nif = nif.with_columns((((pl.col("nif_close") - pl.col("nif_close").shift(10)) / pl.col("nif_close").shift(10)) * 100).alias("nif_roc10"))

    o = pl.col("symbol")
    df = df.with_columns([
        pl.col("close").ewm_mean(span=10).over(o).alias("ema10"),
        pl.col("close").ewm_mean(span=20).over(o).alias("ema20"),
        pl.col("close").ewm_mean(span=50).over(o).alias("ema50"),
        pl.col("close").rolling_mean(20).over(o).alias("sma20"),
        pl.col("close").rolling_mean(100).over(o).alias("sma100"),
        pl.col("volume").rolling_mean(20).over(o).alias("vol_avg20"),
        pl.col("high").rolling_max(20).shift(1).over(o).alias("prior_high20"),
        pl.col("high").rolling_max(10).shift(1).over(o).alias("hi10"),
        pl.col("low").rolling_min(10).shift(1).over(o).alias("lo10"),
        pl.col("low").rolling_min(5).shift(1).over(o).alias("lo5"),
        pl.col("close").shift(1).over(o).alias("prev_close"),
        pl.col("close").shift(10).over(o).alias("close_10ago"),
        pl.max_horizontal([pl.col("high") - pl.col("low"),
                           (pl.col("high") - pl.col("close").shift(1).over(o)).abs(),
                           (pl.col("low") - pl.col("close").shift(1).over(o)).abs()]).alias("tr"),
    ])
    df = df.with_columns([
        pl.col("tr").rolling_mean(14).over(o).alias("atr14"),
        pl.col("ema20").shift(5).over(o).alias("ema20_5ago"),
        pl.col("sma20").shift(5).over(o).alias("sma20_5ago"),
        pl.col("sma100").shift(5).over(o).alias("sma100_5ago"),
        (((pl.col("close") - pl.col("close").shift(10).over(o)) / pl.col("close").shift(10).over(o)) * 100).alias("roc10"),
        pl.col("low").rolling_min(3).shift(1).over(o).alias("lo3"),
    ])
    df = df.join(nif.select(["time", "nif_roc10"]), on="time", how="left")
    return df


# --- book archetypes (each a boolean expression evaluated on a row dict) ---
def archetypes(r):
    def g(k): return r.get(k)
    close, opn, high, low, vol = r["close"], r["open"], r["high"], r["low"], r["volume"]
    out = {}
    va = g("vol_avg20") or vol
    # Pring: base/rectangle breakout — tight recent range, break 20d high on volume
    tight = g("hi10") and g("lo10") and g("lo10") > 0 and (g("hi10") / g("lo10") - 1) < 0.10
    out["base_breakout"] = bool(g("prior_high20") and close > g("prior_high20") and tight and vol >= 1.5 * va)
    # Pring: breakaway gap up
    out["gap_up"] = bool(g("prev_close") and opn >= g("prev_close") * 1.03)
    # Pring: volume thrust (institutional footprint)
    out["volume_thrust_2x"] = bool(vol >= 2.0 * va)
    # Shannon: stacked rising EMAs (healthy uptrend)
    stack = g("ema10") and g("ema20") and g("ema50") and g("ema10") > g("ema20") > g("ema50")
    rising = g("ema20_5ago") and g("ema20") > g("ema20_5ago")
    out["ma_stack_rising"] = bool(stack and rising)
    # Shannon: pullback in uptrend — stacked rising + recent pullback to 20ema + green resume
    pulled = g("lo3") and g("ema20") and g("lo3") <= g("ema20") * 1.01
    out["pullback_in_uptrend"] = bool(stack and rising and pulled and close > opn)
    # Weinstein/Shannon Stage 2: cross above 100-sma turning up
    out["stage2_breakout"] = bool(g("sma100") and g("sma100_5ago") and close > g("sma100")
                                  and g("close_10ago") and g("close_10ago") <= (g("sma100_5ago") or 1e18)
                                  and g("sma100") >= g("sma100_5ago"))
    # above rising 20-sma (basic trend)
    out["above_rising_20sma"] = bool(g("sma20") and g("sma20_5ago") and close > g("sma20") and g("sma20") > g("sma20_5ago"))
    # momentum roc
    out["momentum_roc10>5"] = bool(g("roc10") and g("roc10") > 5)
    # higher-high / higher-low uptrend structure
    out["hh_hl_uptrend"] = bool(g("close_10ago") and close > g("close_10ago") and g("lo5") and low > g("lo5"))
    # near 20d breakout zone
    out["near_20d_high"] = bool(g("prior_high20") and close >= 0.985 * g("prior_high20"))
    # relative strength vs NIFTY
    out["rs_beats_nifty"] = bool(g("roc10") is not None and g("nif_roc10") is not None and g("roc10") > g("nif_roc10"))
    # strong close (top of range)
    rng = high - low
    out["strong_close"] = bool(rng > 0 and (close - low) / rng >= 0.7)
    return out


def main():
    universe = load_universe()
    print(f"Universe {len(universe)}. Building frame...")
    df = build_frame(universe)
    lut = {(r["symbol"], r["time"].strftime("%Y-%m-%d")): r for r in df.to_dicts()}

    data = json.load(open("sliding_window_capture_analysis.json"))
    movers = [m for m in data["captured"] + data["missed"]
              if "-SM" not in m["symbol"] and "-SME" not in m["symbol"]]
    mover_keys = set((m["symbol"], m["start_date"]) for m in movers)

    # control = random non-mover (symbol,date) rows in the window with full features
    random.seed(3)
    all_keys = [k for k, r in lut.items()
                if WIN_START <= k[1] <= WIN_END and r.get("atr14") is not None and r.get("sma100") is not None]
    control = [k for k in all_keys if k not in mover_keys]
    random.shuffle(control)
    control = control[:1500]

    mrows = [lut[k] for k in mover_keys if k in lut and lut[k].get("atr14") is not None]
    crows = [lut[k] for k in control]
    print(f"Movers with features: {len(mrows)}   Control: {len(crows)}\n")

    names = list(archetypes(mrows[0]).keys())
    print(f"{'Archetype':22s} {'Capture%':9s} {'Base%':7s} {'Lift':6s} {'signal'}")
    results = []
    for a in names:
        mc = sum(1 for r in mrows if archetypes(r)[a])
        cc = sum(1 for r in crows if archetypes(r)[a])
        cap = 100 * mc / len(mrows)
        base = 100 * cc / len(crows)
        lift = cap / base if base > 0 else float("inf")
        results.append((a, cap, base, lift))
    for a, cap, base, lift in sorted(results, key=lambda x: -x[3]):
        flag = "<<< strong" if lift >= 1.5 and cap >= 20 else ("<< ok" if lift >= 1.25 else "")
        print(f"{a:22s} {cap:7.1f}%  {base:6.1f}% {lift:5.2f}x  {flag}")

    json.dump({"n_movers": len(mrows), "n_control": len(crows),
               "results": [{"archetype": a, "capture_pct": cap, "base_pct": base, "lift": lift} for a, cap, base, lift in results]},
              open("mover_pattern_classifier_results.json", "w"), indent=2)
    print("\nSaved -> mover_pattern_classifier_results.json")


if __name__ == "__main__":
    main()


def combo_test():
    """Test COMBINATIONS of the high-lift archetypes (Pring 'weight of evidence')."""
    universe = load_universe()
    df = build_frame(universe)
    lut = {(r["symbol"], r["time"].strftime("%Y-%m-%d")): r for r in df.to_dicts()}
    data = json.load(open("sliding_window_capture_analysis.json"))
    movers = [m for m in data["captured"] + data["missed"] if "-SM" not in m["symbol"] and "-SME" not in m["symbol"]]
    mover_keys = set((m["symbol"], m["start_date"]) for m in movers)
    random.seed(3)
    all_keys = [k for k, r in lut.items() if WIN_START <= k[1] <= WIN_END and r.get("atr14") is not None and r.get("sma100") is not None]
    control = [k for k in all_keys if k not in mover_keys]
    random.shuffle(control); control = control[:1500]
    mrows = [lut[k] for k in mover_keys if k in lut and lut[k].get("atr14") is not None]
    crows = [lut[k] for k in control]
    A = {r_id: archetypes(r) for r_id, r in enumerate(mrows)}
    Ac = {r_id: archetypes(r) for r_id, r in enumerate(crows)}

    combos = {
        "volthrust & ma_stack": lambda a: a["volume_thrust_2x"] and a["ma_stack_rising"],
        "volthrust & near_high": lambda a: a["volume_thrust_2x"] and a["near_20d_high"],
        "base_breakout OR (volthrust&ma_stack)": lambda a: a["base_breakout"] or (a["volume_thrust_2x"] and a["ma_stack_rising"]),
        "ma_stack & near_high & vol1.5(=volthrust proxy off)": lambda a: a["ma_stack_rising"] and a["near_20d_high"],
        "pullback & volthrust": lambda a: a["pullback_in_uptrend"] and a["volume_thrust_2x"],
        ">=2 of {base,volthrust,ma_stack,pullback,near_high}": lambda a: sum([a["base_breakout"], a["volume_thrust_2x"], a["ma_stack_rising"], a["pullback_in_uptrend"], a["near_20d_high"]]) >= 2,
        ">=3 of {base,volthrust,ma_stack,pullback,near_high}": lambda a: sum([a["base_breakout"], a["volume_thrust_2x"], a["ma_stack_rising"], a["pullback_in_uptrend"], a["near_20d_high"]]) >= 3,
        "ANY of {base,volthrust,pullback}": lambda a: a["base_breakout"] or a["volume_thrust_2x"] or a["pullback_in_uptrend"],
    }
    print(f"\n=== COMBINATIONS (movers={len(mrows)}, control={len(crows)}) ===")
    print(f"{'Combo':52s} {'Capture%':9s} {'Base%':7s} {'Lift':6s}")
    for name, fn in combos.items():
        mc = sum(1 for i in A if fn(A[i])); cc = sum(1 for i in Ac if fn(Ac[i]))
        cap = 100*mc/len(mrows); base = 100*cc/len(crows); lift = cap/base if base > 0 else float("inf")
        print(f"{name:52s} {cap:7.1f}%  {base:6.1f}% {lift:5.2f}x")


if __name__ == "__main__" and os.getenv("COMBO"):
    combo_test()
