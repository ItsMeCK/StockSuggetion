"""
V9: daily OI snapshot ingestion. Must run once per trading day (near close,
~3:25-3:30pm IST) to build up the clean, non-confounded history needed to
actually validate OI-based signals - our backtest of the naive Buildup
hypothesis came back null on a confounded historical sample (contract
ramp-up noise), and the one exciting anecdote (Short Buildup ahead of
2026-07-08's crash) is n=1. This starts accumulating real data going forward
so that hypothesis can be properly tested in a few weeks, on OUR OWN clean
data instead of Kite's now-inaccessible expired-contract history.

Captures:
  - NIFTY + BANKNIFTY: wide strike range (ATM +/- 10 strikes) at nearest
    expiry, for whole-chain PCR / Max Pain / OI-concentration analysis.
  - F&O stock universe (pipeline/fo_universe.csv): ATM CE + ATM PE only, at
    nearest expiry, for the per-stock Buildup signal.

Idempotent: PRIMARY KEY (date, tradingsymbol) with ON CONFLICT DO UPDATE, so
safe to re-run the same day.
"""
import os
import csv
import time
from datetime import date

from dotenv import load_dotenv
load_dotenv()

os.environ["TRADING_MODE"] = "HISTORICAL"
from mesh_v1.features import conn
from mesh_v1.oi_features import kite, _get_option_chain

INDEX_SYMBOLS = ["NIFTY", "BANKNIFTY"]
INDEX_STRIKE_RANGE = 10  # ATM +/- N strikes each side
STOCK_STRIKE_RANGE = 3   # ATM +/- N strikes each side, per Chandrakant's decision
                          # (2026-07-08): enables per-stock PCR/skew, not just a
                          # single ATM contract. Re-fetched fresh each day centered
                          # on that day's spot, so this doubles as both a rolling
                          # "always current ATM" view and (for strikes that stay
                          # within the window across days) a continuous per-contract
                          # history - no separate table needed for both.
# Option-chain underlying "name" field differs from the index spot quote symbol.
INDEX_SPOT_QUOTE_KEY = {"NIFTY": "NSE:NIFTY 50", "BANKNIFTY": "NSE:NIFTY BANK"}


def get_spot_prices(symbols):
    """Live spot via NSE quote for indices + F&O stocks."""
    keys = []
    for s in symbols:
        keys.append(INDEX_SPOT_QUOTE_KEY.get(s, f"NSE:{s}"))
    spots = {}
    reverse_map = {v: k for k, v in INDEX_SPOT_QUOTE_KEY.items()}
    CHUNK = 200
    for i in range(0, len(keys), CHUNK):
        q = kite().quote(keys[i:i + CHUNK])
        for k, v in q.items():
            sym = reverse_map.get(k, k.split(":", 1)[1])
            spots[sym] = v["last_price"]
        time.sleep(0.3)
    return spots


def nearest_expiry_strikes(chain, opt_type, atm_strike, n_each_side):
    same_type = sorted([c for c in chain if c["instrument_type"] == opt_type], key=lambda c: c["strike"])
    strikes = [c["strike"] for c in same_type]
    if atm_strike not in strikes:
        # snap to nearest available strike
        atm_strike = min(strikes, key=lambda s: abs(s - atm_strike))
    idx = strikes.index(atm_strike)
    lo, hi = max(0, idx - n_each_side), min(len(strikes), idx + n_each_side + 1)
    return same_type[lo:hi]


def main():
    fo_symbols = [r["Symbol"] for r in csv.DictReader(open("pipeline/fo_universe.csv"))]
    all_underlyings = INDEX_SYMBOLS + fo_symbols

    print(f"Fetching spot prices for {len(all_underlyings)} underlyings...", flush=True)
    spots = get_spot_prices(all_underlyings)
    print(f"Got {len(spots)} spot prices", flush=True)

    # Use the quote's own timestamp for the trading date, not system date.today() -
    # if this runs after-hours or before market open, date.today() can mislabel a
    # session's data under the wrong calendar day.
    ts_quote = kite().quote(["NSE:NIFTY 50"])["NSE:NIFTY 50"]
    today = ts_quote["timestamp"].date()
    print(f"Trading date resolved from live quote timestamp: {today}", flush=True)

    rows_to_insert = []
    for sym in all_underlyings:
        spot = spots.get(sym)
        if not spot:
            continue
        chain = _get_option_chain(sym)
        if not chain:
            continue
        expiries = sorted(set(i["expiry"] for i in chain if i["expiry"] >= today))
        if not expiries:
            continue
        nearest = expiries[0]
        chain_nearest = [i for i in chain if i["expiry"] == nearest]

        n_each_side = INDEX_STRIKE_RANGE if sym in INDEX_SYMBOLS else STOCK_STRIKE_RANGE
        for opt_type in ("CE", "PE"):
            contracts = nearest_expiry_strikes(chain_nearest, opt_type, spot, n_each_side)
            for c in contracts:
                rows_to_insert.append({
                    "underlying": sym, "strike": c["strike"], "option_type": opt_type,
                    "expiry": nearest, "tradingsymbol": c["tradingsymbol"],
                    "instrument_token": c["instrument_token"], "spot": spot,
                })

    print(f"Fetching live quotes for {len(rows_to_insert)} option contracts...", flush=True)
    tsyms = [r["tradingsymbol"] for r in rows_to_insert]
    keys = [f"NFO:{t}" for t in tsyms]
    quotes = {}
    CHUNK = 200
    for i in range(0, len(keys), CHUNK):
        q = kite().quote(keys[i:i + CHUNK])
        quotes.update(q)
        time.sleep(0.3)

    c = conn()
    cur = c.cursor()
    n_inserted = 0
    for r in rows_to_insert:
        q = quotes.get(f"NFO:{r['tradingsymbol']}")
        if not q:
            continue
        cur.execute("""
            INSERT INTO option_oi_daily (date, underlying, strike, option_type, expiry,
                tradingsymbol, close, oi, volume, spot)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (date, tradingsymbol) DO UPDATE SET
                close=EXCLUDED.close, oi=EXCLUDED.oi, volume=EXCLUDED.volume, spot=EXCLUDED.spot
        """, (today, r["underlying"], r["strike"], r["option_type"], r["expiry"],
              r["tradingsymbol"], q["last_price"], q.get("oi"), q.get("volume"), r["spot"]))
        n_inserted += 1
    c.commit()
    c.close()
    print(f"Ingested {n_inserted} option-day rows for {today}")


if __name__ == "__main__":
    main()
