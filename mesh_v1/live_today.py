"""
LIVE run: fetch TODAY's partial-day quote for the whole universe, merge with
historical DB data, run the same detector agents + router + basket selection
as mesh_v1/basket_backtest.py, and report today's top-N picks.

⚠️ IMPORTANT CAVEAT: if run while the market is still open, this uses the
CURRENT running price as a stand-in for "today's close" (the validated entry
rule is same-day CLOSE, not intraday). The picks here are PRELIMINARY and can
change by end of day - re-run after 3:30 PM IST close for the final,
actionable basket.

Usage: TRADING_MODE=HISTORICAL PYTHONPATH=. venv/bin/python3 mesh_v1/live_today.py
"""
import os
import sys
import time
import json
import statistics
import datetime
from collections import defaultdict

os.environ["TRADING_MODE"] = "HISTORICAL"
os.environ.setdefault("NEWS_LIVE_FETCH", "1")  # live run: worth the real news check
from dotenv import load_dotenv
load_dotenv()
from kiteconnect import KiteConnect

from mesh_v1.features import build_stock_frame, load_universe, load_company_map, frame_lookup
from mesh_v1.regime_agent import classify_regime
from mesh_v1.technical_agents import (
    trend_continuation_agent, volume_thrust_agent, relative_strength_agent, oversold_reversal_agent,
)
from mesh_v1.news_agents import earnings_calendar_agent, news_catalyst_agent_v2
from mesh_v1.sector_agent import build_sector_move_map, sector_rotation_agent
from mesh_v1.router import combine

TECH_AGENTS = {
    "trend_continuation": trend_continuation_agent,
    "volume_thrust": volume_thrust_agent,
    "relative_strength": relative_strength_agent,
    "oversold_reversal": oversold_reversal_agent,
}
SHORTLIST_MIN_SCORE, SHORTLIST_MIN_AGENTS = 40, 2
TODAY = datetime.datetime.now().strftime("%Y-%m-%d")
NOW_STR = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S IST")
MARKET_CLOSE_HOUR = 15  # 3 PM; after 15:30 the session is over


def get_kite():
    k = KiteConnect(api_key=os.getenv("KITE_API_KEY"))
    k.set_access_token(os.getenv("KITE_ACCESS_TOKEN").strip("'"))
    return k


def fetch_today_quotes(kite, universe):
    """Batch-fetch live quotes for the whole universe (Kite limits ~ a few
    hundred per call - chunk conservatively)."""
    quotes = {}
    CHUNK = 200
    keys = [f"NSE:{s}" for s in universe]
    for i in range(0, len(keys), CHUNK):
        chunk = keys[i:i + CHUNK]
        try:
            q = kite.quote(chunk)
        except Exception as e:
            print(f"  quote batch {i} failed: {e}", file=sys.stderr)
            continue
        for k, v in q.items():
            sym = k.split(":", 1)[1]
            ohlc = v.get("ohlc", {})
            quotes[sym] = {
                "open": ohlc.get("open"), "high": ohlc.get("high"), "low": ohlc.get("low"),
                "close": v.get("last_price"), "volume": v.get("volume"),
            }
        time.sleep(0.34)
    return quotes


def main():
    print(f"=== LIVE RUN for {TODAY} (as of {NOW_STR}) ===")
    market_open_now = datetime.datetime.now().hour < 15 or (datetime.datetime.now().hour == 15 and datetime.datetime.now().minute < 30)
    if market_open_now:
        print("⚠️  MARKET IS STILL OPEN. These are PRELIMINARY picks using the LIVE running")
        print("    price as a stand-in for today's close. Re-run after 3:30 PM IST for the")
        print("    final, actionable basket - the validated entry rule is SAME-DAY CLOSE.\n")

    universe = load_universe()
    company_map = load_company_map()
    kite = get_kite()

    print(f"Fetching live quotes for {len(universe)} stocks...", flush=True)
    quotes = fetch_today_quotes(kite, universe)
    print(f"Got quotes for {len(quotes)}/{len(universe)}", flush=True)

    print("Loading historical frame (through last close) for indicator context...", flush=True)
    # exclude any partial/stale row the DB may already have for TODAY (e.g. from
    # an earlier scheduled ingestion pull mid-session) - we rebuild today's row
    # fresh from the LIVE quote just fetched, not a possibly-stale stored one.
    df = build_stock_frame(universe, start="2026-04-01", end=None)
    import polars as pl
    df = df.filter(pl.col("time") < pl.lit(TODAY).str.to_date())
    by_sym, lut = frame_lookup(df)

    # --- build today's row per symbol by extending each series with the live quote ---
    today_rows = {}
    for sym, series in by_sym.items():
        q = quotes.get(sym)
        if not q or q["open"] is None or q["close"] is None or not series:
            continue
        prev = series[-1]
        if prev["time"].strftime("%Y-%m-%d") >= TODAY:
            continue  # already have today's row somehow (stale test data) - skip
        vol_hist = [r["volume"] for r in series[-20:]]
        vol_avg20 = sum(vol_hist) / len(vol_hist) if vol_hist else q["volume"]
        ema10 = prev.get("ema10"); ema20 = prev.get("ema20"); ema50 = prev.get("ema50")
        alpha10, alpha20, alpha50 = 2 / 11, 2 / 21, 2 / 51
        new_ema10 = q["close"] * alpha10 + (ema10 or q["close"]) * (1 - alpha10) if ema10 else q["close"]
        new_ema20 = q["close"] * alpha20 + (ema20 or q["close"]) * (1 - alpha20) if ema20 else q["close"]
        new_ema50 = q["close"] * alpha50 + (ema50 or q["close"]) * (1 - alpha50) if ema50 else q["close"]
        closes_20 = [r["close"] for r in series[-19:]] + [q["close"]]
        sma20 = sum(closes_20) / len(closes_20) if len(closes_20) >= 5 else None
        closes_100 = [r["close"] for r in series[-99:]] + [q["close"]]
        sma100 = sum(closes_100) / len(closes_100) if len(closes_100) >= 20 else None
        highs10 = [r["high"] for r in series[-10:]]
        lows10 = [r["low"] for r in series[-10:]]
        highs20 = [r["high"] for r in series[-20:]]
        prior_high20 = max(highs20) if highs20 else None
        close_10ago = series[-10]["close"] if len(series) >= 10 else None
        lo5 = min(r["low"] for r in series[-5:]) if len(series) >= 5 else None
        tr_list = [r.get("tr") for r in series[-13:] if r.get("tr") is not None]
        atr14 = (sum(tr_list) + max(q["high"] - q["low"], abs(q["high"] - prev["close"]), abs(q["low"] - prev["close"]))) / (len(tr_list) + 1) if tr_list else None
        roc10 = (q["close"] - close_10ago) / close_10ago * 100 if close_10ago else None
        ema20_5ago = series[-5].get("ema20") if len(series) >= 5 else None
        sma20_5ago = series[-5].get("sma20") if len(series) >= 5 else None
        sma100_5ago = series[-5].get("sma100") if len(series) >= 5 else None

        today_rows[sym] = {
            "symbol": sym, "time": TODAY, "open": q["open"], "high": q["high"], "low": q["low"],
            "close": q["close"], "volume": q["volume"], "vol_avg20": vol_avg20,
            "ema10": new_ema10, "ema20": new_ema20, "ema50": new_ema50,
            "ema20_5ago": ema20_5ago, "sma20": sma20, "sma20_5ago": sma20_5ago,
            "sma100": sma100, "sma100_5ago": sma100_5ago,
            "prior_high20": prior_high20, "hi10": max(highs10) if highs10 else None,
            "lo10": min(lows10) if lows10 else None, "lo5": lo5, "lo3": min(r["low"] for r in series[-2:] + [{"low": q["low"]}]) if len(series) >= 2 else None,
            "close_10ago": close_10ago, "atr14": atr14, "roc10": roc10,
            "prev_close": prev["close"], "nif_roc10": prev.get("nif_roc10"),
        }

    print(f"Computed live indicators for {len(today_rows)} stocks.\n")

    # --- regime: use NIFTY's live quote vs trailing 20-day SMA ---
    try:
        nif_q = kite.quote(["NSE:NIFTY 50"])
        nif_ltp = nif_q.get("NSE:NIFTY 50", {}).get("last_price")
    except Exception:
        nif_ltp = None
    regime = "UNKNOWN"
    if nif_ltp:
        # crude: compare to trailing 20-day NIFTY SMA from DB
        import psycopg2
        conn = psycopg2.connect(host=os.getenv("DB_HOST", "localhost"), port=os.getenv("DB_PORT", "5432"),
                                user=os.getenv("POSTGRES_USER", "quant"), password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
                                dbname=os.getenv("POSTGRES_DB", "market_data"))
        cur = conn.cursor()
        cur.execute("SELECT close FROM daily_ohlcv WHERE symbol='NIFTY 50' ORDER BY time DESC LIMIT 20")
        closes = [float(r[0]) for r in cur.fetchall()]
        conn.close()
        if closes:
            sma20 = sum(closes) / len(closes)
            dist_pct = (nif_ltp - sma20) / sma20 * 100
            regime = "TRENDING_UP" if dist_pct > 0.3 else ("TRENDING_DOWN" if dist_pct < -0.3 else "CHOPPY")
    print(f"Today's regime read: {regime} (NIFTY live={nif_ltp})\n")

    # --- Pass 1: technical agents ---
    candidates = []
    day_returns = []
    for sym, r in today_rows.items():
        if r.get("atr14") is None:
            continue
        prev_c = r.get("prev_close")
        if prev_c:
            day_returns.append((sym, (r["close"] - prev_c) / prev_c * 100))
        tech = {name: fn(r) for name, fn in TECH_AGENTS.items()}
        n_fired = sum(1 for s, _ in tech.values() if s > 0)
        max_score = max((s for s, _ in tech.values()), default=0)
        if max_score >= SHORTLIST_MIN_SCORE or n_fired >= SHORTLIST_MIN_AGENTS:
            candidates.append((sym, tech, r))

    print(f"Shortlisted candidates today: {len(candidates)}\n")
    sector_move_map = build_sector_move_map({TODAY: day_returns}, company_map)

    # --- Pass 2: news/sector + routing ---
    print(f"Scoring candidates (news live-fetch={os.getenv('NEWS_LIVE_FETCH')})...", flush=True)
    scored = []
    for i, (sym, tech, r) in enumerate(candidates):
        earn = earnings_calendar_agent(sym, TODAY)
        news = news_catalyst_agent_v2(sym, TODAY)
        sect = sector_rotation_agent(sym, TODAY, sector_move_map)
        agent_results = {**tech, "earnings_calendar": earn, "news_catalyst_v2": news, "sector_rotation": sect}
        composite, n_fired, reasons = combine(agent_results, regime)
        scored.append((sym, composite, n_fired, reasons, r))
        if (i + 1) % 50 == 0:
            print(f"  scored {i+1}/{len(candidates)}", flush=True)

    ranked = sorted(scored, key=lambda x: -x[1])

    print("\n" + "=" * 90)
    print(f"TODAY'S TOP-10 BASKET ({TODAY}, regime={regime}) — Rs1L / 10 = Rs10,000 each")
    print("=" * 90)
    print(f"{'Rank':4s} {'Symbol':12s} {'Composite':>9s} {'#Agents':>7s} {'LivePrice':>10s} {'Reasons'}")
    for i, (sym, composite, n_fired, reasons, r) in enumerate(ranked[:10], 1):
        print(f"{i:4d} {sym:12s} {composite:9.1f} {n_fired:7d} {r['close']:10.2f} {list(reasons.keys())}")

    json.dump([{"rank": i + 1, "symbol": s, "composite": c, "n_fired": n, "price": r["close"],
               "reasons": list(rs.keys())} for i, (s, c, n, rs, r) in enumerate(ranked[:20])],
              open("live_today_picks.json", "w"), indent=2, default=str)
    print("\nSaved -> live_today_picks.json")
    if market_open_now:
        print("\n⚠️  Re-run this after 3:30 PM IST close for the final, confirmed picks.")


if __name__ == "__main__":
    main()
