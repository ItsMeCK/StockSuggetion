"""
scratch/analyze_sector_index_momentum.py

Task:
Analyze the 8 trades from last week (Aug 11-14, 2026):
- Winners: ZYDUSLIFE, OBEROIRLTY, DRREDDY
- Losers: PRESTIGE, NATIONALUM, PATANJALI, TRENT, TATAPOWER

Theory:
The system ignores broader sector and index momentum. The losers failed because
their sector or the NIFTY 50 was dumping intraday, whereas the winners succeeded
because they had macro tailwinds.
"""

import os
import sys
import json
import logging
from datetime import datetime, date
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

try:
    from kiteconnect import KiteConnect
except ImportError:
    logging.error("kiteconnect module not found. Please install it using pip install kiteconnect")
    sys.exit(1)

load_dotenv()

# Strategy / Trade Definitions from Aug 11-14
TRADES = [
    # Winners
    {"symbol": "ZYDUSLIFE", "sector": "NIFTY PHARMA", "outcome": "WINNER", "entry_date": "2026-08-11", "type": "LONG"},
    {"symbol": "OBEROIRLTY", "sector": "NIFTY REALTY", "outcome": "WINNER", "entry_date": "2026-08-12", "type": "LONG"},
    {"symbol": "DRREDDY", "sector": "NIFTY PHARMA", "outcome": "WINNER", "entry_date": "2026-08-13", "type": "LONG"},
    # Losers
    {"symbol": "PRESTIGE", "sector": "NIFTY REALTY", "outcome": "LOSER", "entry_date": "2026-08-11", "type": "LONG"},
    {"symbol": "NATIONALUM", "sector": "NIFTY METAL", "outcome": "LOSER", "entry_date": "2026-08-12", "type": "LONG"},
    {"symbol": "PATANJALI", "sector": "NIFTY FMCG", "outcome": "LOSER", "entry_date": "2026-08-12", "type": "LONG"},
    {"symbol": "TRENT", "sector": "NIFTY CONSUMPTION", "outcome": "LOSER", "entry_date": "2026-08-13", "type": "LONG"},
    {"symbol": "TATAPOWER", "sector": "NIFTY ENERGY", "outcome": "LOSER", "entry_date": "2026-08-14", "type": "LONG"},
]

# Known Zerodha instrument tokens
# NIFTY 50: 256265
NIFTY_TOKEN = 256265

SECTOR_MAP = {
    "ZYDUSLIFE": "NIFTY PHARMA",
    "DRREDDY": "NIFTY PHARMA",
    "OBEROIRLTY": "NIFTY REALTY",
    "PRESTIGE": "NIFTY REALTY",
    "NATIONALUM": "NIFTY METAL",
    "PATANJALI": "NIFTY FMCG",
    "TRENT": "NIFTY CONSUMPTION",
    "TATAPOWER": "NIFTY ENERGY"
}

def get_kite_client():
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    
    if not api_key or not access_token:
        # Fallback to execution account credentials
        api_key = os.getenv("EXEC_KITE_API_KEY")
        access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN")

    if not api_key or not access_token:
        raise ValueError("Missing KITE_API_KEY or KITE_ACCESS_TOKEN in .env")

    api_key = api_key.strip("'\"")
    access_token = access_token.strip("'\"")

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def resolve_tokens(kite):
    """Fetch NSE instruments and build tradingsymbol to instrument_token mapping."""
    logging.info("Fetching instruments list from Zerodha...")
    instruments = kite.instruments("NSE")
    
    symbol_to_token = {}
    
    # Target symbols & sector index names
    target_names = {
        "NIFTY 50", "NIFTY PHARMA", "NIFTY REALTY", "NIFTY METAL",
        "NIFTY FMCG", "NIFTY ENERGY", "NIFTY CONSUMPTION", "NIFTY 500",
        "ZYDUSLIFE", "OBEROIRLTY", "DRREDDY", "PRESTIGE", "NATIONALUM",
        "PATANJALI", "TRENT", "TATAPOWER"
    }
    
    for inst in instruments:
        ts = inst.get("tradingsymbol", "")
        name = inst.get("name", "")
        if ts in target_names or name in target_names:
            symbol_to_token[ts] = inst["instrument_token"]
            if name:
                symbol_to_token[name] = inst["instrument_token"]

    # Explicit override for NIFTY 50
    symbol_to_token["NIFTY 50"] = NIFTY_TOKEN
    
    return symbol_to_token

def fetch_intraday_ohlc(kite, token, from_date, to_date):
    """Fetch daily or 60m historical data from Kite."""
    try:
        data = kite.historical_data(
            instrument_token=token,
            from_date=from_date,
            to_date=to_date,
            interval="day"
        )
        return data
    except Exception as e:
        logging.warning(f"Failed to fetch daily data for token {token}: {e}")
        return []

def analyze():
    kite = get_kite_client()
    tokens = resolve_tokens(kite)
    
    print("\n" + "="*80)
    print("🚀 SECTOR & INDEX MOMENTUM CORRELATION ANALYSIS (AUG 11-14, 2026)")
    print("="*80)
    
    start_date = "2026-08-10"
    end_date = "2026-08-14"
    
    # 1. Fetch NIFTY 50 data
    nifty_records = fetch_intraday_ohlc(kite, NIFTY_TOKEN, start_date, end_date)
    nifty_by_date = {}
    for r in nifty_records:
        d_str = r["date"].strftime("%Y-%m-%d") if hasattr(r["date"], "strftime") else str(r["date"])[:10]
        o, c, h, l = r["open"], r["close"], r["high"], r["low"]
        pct = ((c - o) / o) * 100
        nifty_by_date[d_str] = {
            "open": o, "close": c, "high": h, "low": l, "pnl_pct": pct
        }

    # 2. Analyze Sector & Stock Performance per trade
    results = []
    
    for t in TRADES:
        sym = t["symbol"]
        sector = t["sector"]
        dt = t["entry_date"]
        outcome = t["outcome"]
        
        # Stock token
        stock_token = tokens.get(sym)
        # Sector token
        sector_token = tokens.get(sector)
        
        stock_pnl = None
        sector_pnl = None
        nifty_pnl = nifty_by_date.get(dt, {}).get("pnl_pct", None)
        
        if stock_token:
            s_data = fetch_intraday_ohlc(kite, stock_token, dt, dt)
            if s_data:
                so, sc = s_data[0]["open"], s_data[0]["close"]
                stock_pnl = ((sc - so) / so) * 100
        
        if sector_token:
            sec_data = fetch_intraday_ohlc(kite, sector_token, dt, dt)
            if sec_data:
                seco, secc = sec_data[0]["open"], sec_data[0]["close"]
                sector_pnl = ((secc - seco) / seco) * 100

        results.append({
            "symbol": sym,
            "outcome": outcome,
            "entry_date": dt,
            "sector": sector,
            "stock_intraday_pnl": stock_pnl,
            "sector_intraday_pnl": sector_pnl,
            "nifty_intraday_pnl": nifty_pnl
        })
        
    print(f"\n{'Symbol':<12} | {'Outcome':<8} | {'Entry Date':<10} | {'Stock O-to-C %':<15} | {'Sector Index':<18} | {'Sector O-to-C %':<16} | {'NIFTY 50 O-to-C %':<18}")
    print("-" * 110)
    
    for r in results:
        stk_str = f"{r['stock_intraday_pnl']:+.2f}%" if r['stock_intraday_pnl'] is not None else "N/A"
        sec_str = f"{r['sector_intraday_pnl']:+.2f}%" if r['sector_intraday_pnl'] is not None else "N/A"
        nif_str = f"{r['nifty_intraday_pnl']:+.2f}%" if r['nifty_intraday_pnl'] is not None else "N/A"
        print(f"{r['symbol']:<12} | {r['outcome']:<8} | {r['entry_date']:<10} | {stk_str:<15} | {r['sector']:<18} | {sec_str:<16} | {nif_str:<18}")

    print("\n" + "="*80)
    print("📊 MACRO CORRELATION SUMMARY & INSIGHTS")
    print("="*80)
    
    winners = [r for r in results if r["outcome"] == "WINNER"]
    losers = [r for r in results if r["outcome"] == "LOSER"]
    
    def avg(lst, key):
        vals = [x[key] for x in lst if x[key] is not None]
        return sum(vals) / len(vals) if vals else 0.0

    print(f"Winners Count: {len(winners)}")
    print(f"  - Avg Stock Intraday Return:  {avg(winners, 'stock_intraday_pnl'):+.2f}%")
    print(f"  - Avg Sector Intraday Return: {avg(winners, 'sector_intraday_pnl'):+.2f}%")
    print(f"  - Avg NIFTY Intraday Return:  {avg(winners, 'nifty_intraday_pnl'):+.2f}%")
    
    print(f"\nLosers Count: {len(losers)}")
    print(f"  - Avg Stock Intraday Return:  {avg(losers, 'stock_intraday_pnl'):+.2f}%")
    print(f"  - Avg Sector Intraday Return: {avg(losers, 'sector_intraday_pnl'):+.2f}%")
    print(f"  - Avg NIFTY Intraday Return:  {avg(losers, 'nifty_intraday_pnl'):+.2f}%")
    
    return results

if __name__ == "__main__":
    analyze()
