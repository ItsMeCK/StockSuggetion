"""
V9: backfill full July-to-date OI history for every contract already seeded
in option_oi_daily (today's ATM/near-ATM snapshot). Since these contracts
are currently listed, Kite's historical_data(oi=True) can pull their whole
trading history so far - no need to wait day-by-day for July data we can
get in one shot. (Only works for contracts still listed - May/June's
expired series remain permanently unavailable, this doesn't change that.)

Note: this backfills the SAME fixed contract (e.g. HDFCBANK Jul 850 CE) across
all of July, not a rolling "whichever strike was ATM that day" series - simpler,
avoids stitching different contracts together, but means the contract may not
have been exactly ATM on earlier July days if spot moved since.
"""
import os
import time
from datetime import date

from dotenv import load_dotenv
load_dotenv()

os.environ["TRADING_MODE"] = "HISTORICAL"
from mesh_v1.features import conn
from mesh_v1.oi_features import kite

BACKFILL_START = date(2026, 7, 1)


def main():
    c = conn()
    cur = c.cursor()
    cur.execute("""
        SELECT DISTINCT tradingsymbol, underlying, strike, option_type, expiry
        FROM option_oi_daily WHERE date = (SELECT max(date) FROM option_oi_daily)
    """)
    contracts = cur.fetchall()
    print(f"Backfilling {len(contracts)} contracts from {BACKFILL_START} to today...", flush=True)

    # need instrument_token per tradingsymbol - re-fetch from instruments dump once
    all_nfo = kite().instruments("NFO")
    token_by_tsym = {i["tradingsymbol"]: i["instrument_token"] for i in all_nfo}

    n_rows = 0
    n_contracts_done = 0
    for tsym, underlying, strike, opt_type, expiry in contracts:
        token = token_by_tsym.get(tsym)
        if not token:
            continue
        try:
            hist = kite().historical_data(
                token, BACKFILL_START.strftime("%Y-%m-%d 09:00:00"),
                date.today().strftime("%Y-%m-%d 15:30:00"), "day", oi=True,
            )
        except Exception as e:
            print(f"  {tsym}: fetch failed ({e})")
            time.sleep(0.5)
            continue

        for h in hist:
            d = h["date"].date()
            if d >= date.today():
                continue  # today's row already inserted live by oi_daily_ingest.py
            cur.execute("""
                INSERT INTO option_oi_daily (date, underlying, strike, option_type, expiry,
                    tradingsymbol, close, oi, volume, spot)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NULL)
                ON CONFLICT (date, tradingsymbol) DO UPDATE SET
                    close=EXCLUDED.close, oi=EXCLUDED.oi, volume=EXCLUDED.volume
            """, (d, underlying, strike, opt_type, expiry, tsym, h["close"], h["oi"], h["volume"]))
            n_rows += 1
        n_contracts_done += 1
        if n_contracts_done % 50 == 0:
            print(f"  ...{n_contracts_done}/{len(contracts)} contracts backfilled, {n_rows} rows so far", flush=True)
            c.commit()
        time.sleep(0.12)

    c.commit()
    c.close()
    print(f"\nDone. {n_contracts_done} contracts, {n_rows} historical rows inserted.")


if __name__ == "__main__":
    main()
