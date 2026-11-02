"""
Daily EOD Ingestion Script for Nifty 500 Cash Equities from Zerodha Kite Connect
Ingests continuous historical daily bars from June 2026 to October 2026.
"""

import os
import sys
import time
import json
import datetime
import logging
import pandas as pd
import polars as pl
from dotenv import load_dotenv
from kiteconnect import KiteConnect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("UniverseIngestor")

def ingest_master_universe():
    load_dotenv()
    api_key = (os.getenv("KITE_API_KEY") or "").strip("'\"")
    access_token = (os.getenv("KITE_ACCESS_TOKEN") or "").strip("'\"")

    if not api_key or not access_token:
        logger.error("KITE_API_KEY or KITE_ACCESS_TOKEN not set in .env")
        sys.exit(1)

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    # 1. Load active instruments
    inst_path = "data/nse_cash_instruments.json"
    if not os.path.exists(inst_path):
        logger.info("Fetching instruments from Kite...")
        all_insts = kite.instruments("NSE")
        clean_eqs = [
            {
                "symbol": i["tradingsymbol"],
                "name": i["name"],
                "instrument_token": i["instrument_token"],
                "tick_size": i["tick_size"],
                "lot_size": i["lot_size"]
            }
            for i in all_insts
            if i.get("instrument_type") == "EQ" and i.get("segment") == "NSE"
        ]
        with open(inst_path, "w") as f:
            json.dump(clean_eqs, f, indent=2)
    else:
        with open(inst_path) as f:
            clean_eqs = json.load(f)

    token_map = {i["symbol"]: i["instrument_token"] for i in clean_eqs}

    # 2. Load symbols from pipeline/master_universe.csv
    universe_path = "pipeline/master_universe.csv"
    if os.path.exists(universe_path):
        df_master = pd.read_csv(universe_path)
        symbols = df_master["Symbol"].dropna().unique().tolist()
    else:
        symbols = list(token_map.keys())[:500]

    logger.info(f"Master universe contains {len(symbols)} symbols.")

    # 3. Date range: last 120 days (June 29 to Oct 24, 2026)
    to_date = datetime.date.today()
    from_date = to_date - datetime.timedelta(days=120)
    from_str = from_date.strftime("%Y-%m-%d")
    to_str = to_date.strftime("%Y-%m-%d")

    logger.info(f"Ingesting daily historical candles from {from_str} to {to_str}...")

    all_records = []
    success_count = 0
    failed_symbols = []

    t0 = time.time()
    for idx, sym in enumerate(symbols, 1):
        tok = token_map.get(sym)
        if not tok:
            # Try symbol variations
            for alt in [f"{sym}-EQ", sym.replace("-", "")]:
                if alt in token_map:
                    tok = token_map[alt]
                    break

        if not tok:
            failed_symbols.append((sym, "Token not found"))
            continue

        try:
            bars = kite.historical_data(tok, from_str, to_str, interval="day")
            if bars:
                for b in bars:
                    b_date = b["date"]
                    # Convert to naive or IST date
                    if hasattr(b_date, "date"):
                        d_val = b_date.date()
                    else:
                        d_val = datetime.datetime.strptime(str(b_date)[:10], "%Y-%m-%d").date()

                    all_records.append({
                        "symbol": sym,
                        "time": datetime.datetime.combine(d_val, datetime.time(15, 30)),
                        "trade_date": d_val,
                        "open": float(b["open"]),
                        "high": float(b["high"]),
                        "low": float(b["low"]),
                        "close": float(b["close"]),
                        "volume": int(b.get("volume", 0)),
                        "traded_value": float(b["close"]) * float(b.get("volume", 0))
                    })
                success_count += 1
        except Exception as e:
            logger.warning(f"Failed to fetch {sym} ({tok}): {e}")
            failed_symbols.append((sym, str(e)))

        time.sleep(0.18) # ~5 req/sec (well within Kite limits)

        if idx % 50 == 0 or idx == len(symbols):
            elapsed = time.time() - t0
            logger.info(f"Ingested {idx}/{len(symbols)} symbols ({success_count} successful, {len(all_records)} daily bars) in {elapsed:.1f}s")

    if not all_records:
        logger.error("No daily records retrieved.")
        return

    # 4. Save to Parquet
    out_path = "data/equity_universe_daily.parquet"
    df_out = pl.DataFrame(all_records)
    df_out = df_out.sort(["symbol", "trade_date"])
    df_out.write_parquet(out_path)
    logger.info(f"Successfully saved {len(df_out)} rows for {df_out['symbol'].n_unique()} symbols to {out_path}!")

if __name__ == "__main__":
    ingest_master_universe()
