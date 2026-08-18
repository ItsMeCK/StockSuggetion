#!/usr/bin/env python3
"""
backtest_macro_dynamic_thresholds.py

Comprehensive Backtest & Mathematical Proof for Theory 2:
Macro-Adjusted Dynamic Thresholds in August 2026.

Evaluates whether lowering mathematical standards (e.g. bbw < 0.35, vol_surge > 1.5)
specifically when parent Sector Index > +1.0% intraday captures more profitable winners
without sacrificing system win rate and expectancy.
"""

import os
import sys
import json
import math
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Tuple
import polars as pl
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from kiteconnect import KiteConnect

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

load_dotenv()

# Complete Sector Mapping for FNO Universe
SECTOR_MAP = {
    # NIFTY AUTO
    'ASHOKLEY': 'NIFTY AUTO', 'BAJAJ-AUTO': 'NIFTY AUTO', 'BHARATFORG': 'NIFTY AUTO', 'BOSCHLTD': 'NIFTY AUTO',
    'EICHERMOT': 'NIFTY AUTO', 'FORCEMOT': 'NIFTY AUTO', 'HEROMOTOCO': 'NIFTY AUTO', 'HYUNDAI': 'NIFTY AUTO',
    'M&M': 'NIFTY AUTO', 'MARUTI': 'NIFTY AUTO', 'MOTHERSON': 'NIFTY AUTO', 'SONACOMS': 'NIFTY AUTO',
    'TATAMOTORS': 'NIFTY AUTO', 'TIINDIA': 'NIFTY AUTO', 'TMPV': 'NIFTY AUTO', 'TVSMOTOR': 'NIFTY AUTO',
    'UNOMINDA': 'NIFTY AUTO',

    # NIFTY BANK / PSU BANK
    'AUBANK': 'NIFTY BANK', 'AXISBANK': 'NIFTY BANK', 'BANDHANBNK': 'NIFTY BANK', 'BANKBARODA': 'NIFTY PSU BANK',
    'BANKINDIA': 'NIFTY PSU BANK', 'CANBK': 'NIFTY PSU BANK', 'FEDERALBNK': 'NIFTY BANK', 'HDFCBANK': 'NIFTY BANK',
    'ICICIBANK': 'NIFTY BANK', 'IDFCFIRSTB': 'NIFTY BANK', 'INDIANB': 'NIFTY PSU BANK', 'INDUSINDBK': 'NIFTY BANK',
    'KOTAKBANK': 'NIFTY BANK', 'PNB': 'NIFTY PSU BANK', 'RBLBANK': 'NIFTY BANK', 'SBIN': 'NIFTY PSU BANK',
    'UNIONBANK': 'NIFTY PSU BANK', 'YESBANK': 'NIFTY BANK',

    # NIFTY FIN SERVICE
    '360ONE': 'NIFTY FIN SERVICE', 'ABCAPITAL': 'NIFTY FIN SERVICE', 'ANGELONE': 'NIFTY FIN SERVICE',
    'BAJFINANCE': 'NIFTY FIN SERVICE', 'BAJAJFINSV': 'NIFTY FIN SERVICE', 'BAJAJHLDNG': 'NIFTY FIN SERVICE',
    'BSE': 'NIFTY FIN SERVICE', 'CAMS': 'NIFTY FIN SERVICE', 'CDSL': 'NIFTY FIN SERVICE', 'CHOLAFIN': 'NIFTY FIN SERVICE',
    'HDFCAMC': 'NIFTY FIN SERVICE', 'HDFCLIFE': 'NIFTY FIN SERVICE', 'ICICIGI': 'NIFTY FIN SERVICE',
    'ICICIPRULI': 'NIFTY FIN SERVICE', 'IEX': 'NIFTY FIN SERVICE', 'IREDA': 'NIFTY FIN SERVICE',
    'IRFC': 'NIFTY FIN SERVICE', 'JIOFIN': 'NIFTY FIN SERVICE', 'KFINTECH': 'NIFTY FIN SERVICE',
    'LICHSGFIN': 'NIFTY FIN SERVICE', 'LICI': 'NIFTY FIN SERVICE', 'LTF': 'NIFTY FIN SERVICE',
    'MANAPPURAM': 'NIFTY FIN SERVICE', 'MCX': 'NIFTY FIN SERVICE', 'MFSL': 'NIFTY FIN SERVICE',
    'MOTILALOFS': 'NIFTY FIN SERVICE', 'MUTHOOTFIN': 'NIFTY FIN SERVICE', 'NAM-INDIA': 'NIFTY FIN SERVICE',
    'PAYTM': 'NIFTY FIN SERVICE', 'PFC': 'NIFTY FIN SERVICE', 'PNBHOUSING': 'NIFTY FIN SERVICE',
    'POLICYBZR': 'NIFTY FIN SERVICE', 'RECLTD': 'NIFTY FIN SERVICE', 'SBICARD': 'NIFTY FIN SERVICE',
    'SBILIFE': 'NIFTY FIN SERVICE', 'SHRIRAMFIN': 'NIFTY FIN SERVICE',

    # NIFTY IT
    'COFORGE': 'NIFTY IT', 'HCLTECH': 'NIFTY IT', 'INFY': 'NIFTY IT', 'KPITTECH': 'NIFTY IT',
    'LTM': 'NIFTY IT', 'MPHASIS': 'NIFTY IT', 'NAUKRI': 'NIFTY IT', 'OFSS': 'NIFTY IT',
    'PERSISTENT': 'NIFTY IT', 'TATAELXSI': 'NIFTY IT', 'TCS': 'NIFTY IT', 'TECHM': 'NIFTY IT',
    'WIPRO': 'NIFTY IT',

    # NIFTY PHARMA & HEALTHCARE
    'ALKEM': 'NIFTY PHARMA', 'APOLLOHOSP': 'NIFTY PHARMA', 'AUROPHARMA': 'NIFTY PHARMA', 'BIOCON': 'NIFTY PHARMA',
    'CIPLA': 'NIFTY PHARMA', 'DIVISLAB': 'NIFTY PHARMA', 'DRREDDY': 'NIFTY PHARMA', 'FORTIS': 'NIFTY PHARMA',
    'GLENMARK': 'NIFTY PHARMA', 'LAURUSLABS': 'NIFTY PHARMA', 'LUPIN': 'NIFTY PHARMA', 'MANKIND': 'NIFTY PHARMA',
    'MAXHEALTH': 'NIFTY PHARMA', 'SUNPHARMA': 'NIFTY PHARMA', 'TORNTPHARM': 'NIFTY PHARMA', 'ZYDUSLIFE': 'NIFTY PHARMA',

    # NIFTY REALTY
    'DLF': 'NIFTY REALTY', 'GODREJPROP': 'NIFTY REALTY', 'LODHA': 'NIFTY REALTY', 'OBEROIRLTY': 'NIFTY REALTY',
    'PHOENIXLTD': 'NIFTY REALTY', 'PRESTIGE': 'NIFTY REALTY', 'NBCC': 'NIFTY REALTY',

    # NIFTY METAL
    'APLAPOLLO': 'NIFTY METAL', 'HINDALCO': 'NIFTY METAL', 'HINDZINC': 'NIFTY METAL', 'JINDALSTEL': 'NIFTY METAL',
    'JSWSTEEL': 'NIFTY METAL', 'NATIONALUM': 'NIFTY METAL', 'NMDC': 'NIFTY METAL', 'SAIL': 'NIFTY METAL',
    'TATASTEEL': 'NIFTY METAL', 'VEDL': 'NIFTY METAL',

    # NIFTY FMCG & CONSUMPTION
    'BRITANNIA': 'NIFTY FMCG', 'COLPAL': 'NIFTY FMCG', 'DABUR': 'NIFTY FMCG', 'GODFRYPHLP': 'NIFTY FMCG',
    'GODREJCP': 'NIFTY FMCG', 'HINDUNILVR': 'NIFTY FMCG', 'ITC': 'NIFTY FMCG', 'MARICO': 'NIFTY FMCG',
    'NESTLEIND': 'NIFTY FMCG', 'PATANJALI': 'NIFTY FMCG', 'RADICO': 'NIFTY FMCG', 'TATACONSUM': 'NIFTY FMCG',
    'UNITDSPR': 'NIFTY FMCG', 'VBL': 'NIFTY FMCG', 'ASIANPAINT': 'NIFTY CONSUMPTION', 'DMART': 'NIFTY CONSUMPTION',
    'INDHOTEL': 'NIFTY CONSUMPTION', 'JUBLFOOD': 'NIFTY CONSUMPTION', 'KALYANKJIL': 'NIFTY CONSUMPTION',
    'PAGEIND': 'NIFTY CONSUMPTION', 'TITAN': 'NIFTY CONSUMPTION', 'TRENT': 'NIFTY CONSUMPTION',
    'NYKAA': 'NIFTY CONSUMPTION', 'SWIGGY': 'NIFTY CONSUMPTION', 'ETERNAL': 'NIFTY CONSUMPTION',
    'VMM': 'NIFTY CONSUMPTION',

    # NIFTY ENERGY / OIL & GAS / POWER / PSE
    'ADANIENSOL': 'NIFTY ENERGY', 'ADANIGREEN': 'NIFTY ENERGY', 'ADANIPOWER': 'NIFTY ENERGY',
    'BPCL': 'NIFTY ENERGY', 'COALINDIA': 'NIFTY ENERGY', 'GAIL': 'NIFTY ENERGY', 'HINDPETRO': 'NIFTY ENERGY',
    'IOC': 'NIFTY ENERGY', 'INOXWIND': 'NIFTY ENERGY', 'JSWENERGY': 'NIFTY ENERGY', 'NHPC': 'NIFTY ENERGY',
    'NTPC': 'NIFTY ENERGY', 'OIL': 'NIFTY ENERGY', 'ONGC': 'NIFTY ENERGY', 'PETRONET': 'NIFTY ENERGY',
    'POWERGRID': 'NIFTY ENERGY', 'PREMIERENE': 'NIFTY ENERGY', 'RELIANCE': 'NIFTY ENERGY',
    'SUZLON': 'NIFTY ENERGY', 'TATAPOWER': 'NIFTY ENERGY', 'WAAREEENER': 'NIFTY ENERGY',

    # NIFTY INFRA & CAPITAL GOODS & DEFENSE
    'ABB': 'NIFTY INFRA', 'ADANIENT': 'NIFTY INFRA', 'ADANIPORTS': 'NIFTY INFRA', 'AMBER': 'NIFTY INFRA',
    'AMBUJACEM': 'NIFTY INFRA', 'ASTRAL': 'NIFTY INFRA', 'BDL': 'NIFTY INFRA', 'BEL': 'NIFTY INFRA',
    'BHEL': 'NIFTY INFRA', 'BHARTIARTL': 'NIFTY INFRA', 'BLUESTARCO': 'NIFTY INFRA', 'CGPOWER': 'NIFTY INFRA',
    'COCHINSHIP': 'NIFTY INFRA', 'CONCOR': 'NIFTY INFRA', 'CROMPTON': 'NIFTY INFRA', 'CUMMINSIND': 'NIFTY INFRA',
    'DALBHARAT': 'NIFTY INFRA', 'DELHIVERY': 'NIFTY INFRA', 'DIXON': 'NIFTY INFRA', 'GMRAIRPORT': 'NIFTY INFRA',
    'GRASIM': 'NIFTY INFRA', 'GVT&D': 'NIFTY INFRA', 'HAL': 'NIFTY INFRA', 'HAVELLS': 'NIFTY INFRA',
    'INDIGO': 'NIFTY INFRA', 'INDUSTOWER': 'NIFTY INFRA', 'KAYNES': 'NIFTY INFRA', 'KEI': 'NIFTY INFRA',
    'LT': 'NIFTY INFRA', 'MAZDOCK': 'NIFTY INFRA', 'PGEL': 'NIFTY INFRA', 'PIDILITIND': 'NIFTY INFRA',
    'PIIND': 'NIFTY INFRA', 'POLYCAB': 'NIFTY INFRA', 'POWERINDIA': 'NIFTY INFRA', 'RVNL': 'NIFTY INFRA',
    'SHREECEM': 'NIFTY INFRA', 'SIEMENS': 'NIFTY INFRA', 'SOLARINDS': 'NIFTY INFRA', 'SRF': 'NIFTY INFRA',
    'SUPREMEIND': 'NIFTY INFRA', 'ULTRACEMCO': 'NIFTY INFRA', 'UPL': 'NIFTY INFRA', 'VOLTAS': 'NIFTY INFRA',
    'IDEA': 'NIFTY INFRA',
}

SECTOR_TOKENS = {
    'NIFTY 50': 256265,
    'NIFTY BANK': 260105,
    'NIFTY IT': 259849,
    'NIFTY PHARMA': 262409,
    'NIFTY REALTY': 261129,
    'NIFTY METAL': 263689,
    'NIFTY FMCG': 261897,
    'NIFTY AUTO': 263433,
    'NIFTY ENERGY': 261641,
    'NIFTY INFRA': 261385,
    'NIFTY MEDIA': 263945,
    'NIFTY FIN SERVICE': 257801,
    'NIFTY PSE': 262665,
    'NIFTY PSU BANK': 262921,
    'NIFTY HEALTHCARE': 288521,
    'NIFTY CONSUMPTION': 257545,
    'NIFTY COMMODITIES': 257289
}

def get_kite_client():
    api_key = (os.getenv("KITE_API_KEY") or os.getenv("EXEC_KITE_API_KEY", "")).strip("'\"")
    access_token = (os.getenv("KITE_ACCESS_TOKEN") or os.getenv("EXEC_KITE_ACCESS_TOKEN", "")).strip("'\"")
    if not api_key or not access_token:
        logging.error("Missing Kite credentials in .env")
        return None
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def fetch_sector_historical_data(kite) -> pd.DataFrame:
    """Fetch 60m historical candles for all sector indices."""
    cache_file = "scratch/sector_60m_august_2026.parquet"
    if os.path.exists(cache_file):
        logging.info(f"Loading cached sector 60m data from {cache_file}")
        return pd.read_parquet(cache_file)
        
    logging.info("Fetching fresh 60m historical data for sector indices from Kite...")
    records = []
    for name, token in SECTOR_TOKENS.items():
        try:
            hist = kite.historical_data(token, "2026-07-28", "2026-08-14", interval="60minute")
            for bar in hist:
                records.append({
                    "sector": name,
                    "time": bar["date"],
                    "open": bar["open"],
                    "high": bar["high"],
                    "low": bar["low"],
                    "close": bar["close"],
                    "volume": bar["volume"]
                })
        except Exception as e:
            logging.warning(f"Error fetching historical data for {name} ({token}): {e}")
            
    df_sec = pd.DataFrame(records)
    df_sec["time"] = pd.to_datetime(df_sec["time"], utc=True)
    df_sec["date"] = df_sec["time"].dt.date
    
    # Calculate intraday open for each sector on each day
    day_opens = df_sec.groupby(["sector", "date"])["open"].transform("first")
    df_sec["day_open"] = day_opens
    df_sec["sector_intraday_ret"] = ((df_sec["close"] - df_sec["day_open"]) / df_sec["day_open"]) * 100
    
    # Previous day close calculation
    daily_closes = df_sec.groupby(["sector", "date"])["close"].last().reset_index()
    daily_closes["prev_close"] = daily_closes.groupby("sector")["close"].shift(1)
    df_sec = df_sec.merge(daily_closes[["sector", "date", "prev_close"]], on=["sector", "date"], how="left")
    df_sec["sector_prev_close_ret"] = ((df_sec["close"] - df_sec["prev_close"]) / df_sec["prev_close"]) * 100
    
    os.makedirs(os.path.dirname(cache_file), exist_ok=True)
    df_sec.to_parquet(cache_file)
    return df_sec

def simulate_trade_outcomes(stock_df: pl.DataFrame, trigger_symbol: str, trigger_time: datetime, entry_price: float):
    """
    Computes accurate outcome metrics for a triggered trade:
    - Same-day Close Return (Intraday PnL %)
    - Same-day Max High (Peak Fwd Return %)
    - Same-day Min Low (Max Adverse Drawdown %)
    - 1-Day Forward Return % (Next day close)
    - 3-Day Forward Return %
    - Option Return % (ATM CE simulation with Delta ~0.50 and 50% SL cap)
    """
    sym_df = stock_df.filter(pl.col("symbol") == trigger_symbol).sort("time")
    
    # Trigger date
    trig_date = trigger_time.date()
    
    # Filter candles on the same day AT or AFTER the trigger
    same_day_candles = sym_df.filter(
        (pl.col("time").dt.date() == trig_date) & 
        (pl.col("time") >= trigger_time)
    )
    
    if len(same_day_candles) == 0:
        return None
        
    day_close_price = same_day_candles.select(pl.col("close").last())[0, 0]
    day_max_high = same_day_candles.select(pl.col("high").max())[0, 0]
    day_min_low = same_day_candles.select(pl.col("low").min())[0, 0]
    
    intraday_ret_pct = ((day_close_price - entry_price) / entry_price) * 100
    max_high_pct = ((day_max_high - entry_price) / entry_price) * 100
    max_drawdown_pct = ((day_min_low - entry_price) / entry_price) * 100
    
    # Future dates for 1D / 3D follow-through
    future_candles = sym_df.filter(pl.col("time").dt.date() > trig_date).sort("time")
    future_dates = future_candles.select(pl.col("time").dt.date().unique().sort())["time"].to_list()
    
    fwd_1d_ret = None
    fwd_3d_ret = None
    
    if len(future_dates) >= 1:
        d1_candles = future_candles.filter(pl.col("time").dt.date() == future_dates[0])
        d1_close = d1_candles.select(pl.col("close").last())[0, 0]
        fwd_1d_ret = ((d1_close - entry_price) / entry_price) * 100
        
    if len(future_dates) >= 3:
        d3_candles = future_candles.filter(pl.col("time").dt.date() == future_dates[2])
        d3_close = d3_candles.select(pl.col("close").last())[0, 0]
        fwd_3d_ret = ((d3_close - entry_price) / entry_price) * 100
    elif len(future_dates) > 0:
        last_close = future_candles.select(pl.col("close").last())[0, 0]
        fwd_3d_ret = ((last_close - entry_price) / entry_price) * 100

    # Simulated Option PnL: Delta ~0.50, Gearing ~5-6x, with -50% Stop Loss cap
    # If stock moves +2%, option moves roughly +10% to +12%
    # If stock moves -2%, option loses ~-12%
    # With a realistic trailing/momentum stop or end-of-day exit:
    opt_multiplier = 5.0
    raw_opt_pnl = intraday_ret_pct * opt_multiplier
    opt_pnl = max(-50.0, min(200.0, raw_opt_pnl))
    
    is_winner = (intraday_ret_pct > 0.3) or (max_high_pct > 1.2 and intraday_ret_pct >= -0.2)
    
    return {
        "entry_price": entry_price,
        "day_close_price": day_close_price,
        "intraday_ret_pct": intraday_ret_pct,
        "max_high_pct": max_high_pct,
        "max_drawdown_pct": max_drawdown_pct,
        "fwd_1d_ret": fwd_1d_ret,
        "fwd_3d_ret": fwd_3d_ret,
        "opt_pnl": opt_pnl,
        "is_winner": is_winner
    }

def run_backtest():
    parquet_path = "data/intraday_ohlcv.parquet"
    if not os.path.exists(parquet_path):
        print(f"Error: {parquet_path} not found.")
        return

    print("="*100)
    print("🔬 THEORY 2 BACKTEST: MACRO-ADJUSTED DYNAMIC THRESHOLDS")
    print("   Evaluating Dynamic Mathematical Standards for August 2026")
    print("="*100)

    kite = get_kite_client()
    sector_df = fetch_sector_historical_data(kite)

    print("\nLoading Stock Parquet Data...")
    df = pl.read_parquet(parquet_path)
    df = df.sort(["symbol", "time"])

    # Rolling 20 technicals
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    ])

    df = df.with_columns([
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20")) * 100).alias("sma20_dist_pct"),
        (((pl.col("close") - pl.col("open")) / pl.col("open")) * 100).alias("candle_body_pct")
    ])

    # Convert to pandas for easy sector merging
    pdf = df.to_pandas()
    pdf["time"] = pd.to_datetime(pdf["time"], utc=True)
    pdf["date"] = pdf["time"].dt.date
    
    # Map each stock to its sector
    pdf["sector"] = pdf["symbol"].map(lambda s: SECTOR_MAP.get(s, "NIFTY 50"))
    
    # Merge sector hourly data
    # Note: Sector hourly timestamps align with stock hourly timestamps
    pdf = pdf.merge(
        sector_df[["sector", "time", "sector_intraday_ret", "sector_prev_close_ret", "day_open", "close"]].rename(
            columns={"day_open": "sec_day_open", "close": "sec_close"}
        ),
        on=["sector", "time"],
        how="left"
    )
    
    # Backfill sector returns if slightly offset
    pdf["sector_intraday_ret"] = pdf["sector_intraday_ret"].fillna(0.0)
    pdf["sector_prev_close_ret"] = pdf["sector_prev_close_ret"].fillna(0.0)

    # Filter for FNO universe stocks
    fno_symbols = set(SECTOR_MAP.keys())
    pdf_fno = pdf[pdf["symbol"].isin(fno_symbols)].copy()

    # Define the simulation timeframes:
    # 1. Last Week: Aug 10 - Aug 14, 2026
    # 2. Full Month: Aug 3 - Aug 14, 2026
    
    last_week_start = pd.to_datetime("2026-08-10").date()
    last_week_end = pd.to_datetime("2026-08-14").date()
    
    full_month_start = pd.to_datetime("2026-08-03").date()
    full_month_end = pd.to_datetime("2026-08-14").date()

    # Define Filter Configurations to test:
    configurations = [
        {
            "id": "config_baseline",
            "name": "1. Baseline (Fixed Thresholds)",
            "desc": "Standard: bbw < 0.22, vol_surge > 2.5 (No Sector Adjustment)",
            "bbw_std": 0.22, "vol_std": 2.5,
            "bbw_loosened": 0.22, "vol_loosened": 2.5,
            "sec_threshold": 999.0 # Never triggers loosened
        },
        {
            "id": "config_theory2_modest",
            "name": "2. Theory 2 (Modest Loosening: BBW < 0.28, Vol > 2.0 when Sector > 1.0%)",
            "desc": "Sector > +1.0% -> BBW < 0.28, Vol > 2.0x; Else BBW < 0.22, Vol > 2.5x",
            "bbw_std": 0.22, "vol_std": 2.5,
            "bbw_loosened": 0.28, "vol_loosened": 2.0,
            "sec_threshold": 1.0
        },
        {
            "id": "config_theory2_vol_only",
            "name": "3. Theory 2 (Vol Loosening Only: Vol > 1.5 when Sector > 1.0%)",
            "desc": "Sector > +1.0% -> BBW < 0.22, Vol > 1.5x; Else BBW < 0.22, Vol > 2.5x",
            "bbw_std": 0.22, "vol_std": 2.5,
            "bbw_loosened": 0.22, "vol_loosened": 1.5,
            "sec_threshold": 1.0
        },
        {
            "id": "config_theory2_bbw_only",
            "name": "4. Theory 2 (BBW Loosening Only: BBW < 0.35 when Sector > 1.0%)",
            "desc": "Sector > +1.0% -> BBW < 0.35, Vol > 2.5x; Else BBW < 0.22, Vol > 2.5x",
            "bbw_std": 0.22, "vol_std": 2.5,
            "bbw_loosened": 0.35, "vol_loosened": 2.5,
            "sec_threshold": 1.0
        },
        {
            "id": "config_theory2_full",
            "name": "5. Theory 2 (Full Loosening: BBW < 0.35, Vol > 1.5 when Sector > 1.0%)",
            "desc": "Sector > +1.0% -> BBW < 0.35, Vol > 1.5x; Else BBW < 0.22, Vol > 2.5x",
            "bbw_std": 0.22, "vol_std": 2.5,
            "bbw_loosened": 0.35, "vol_loosened": 1.5,
            "sec_threshold": 1.0
        },
        {
            "id": "config_theory2_sensitive",
            "name": "6. Theory 2 (Sensitive Sector Trigger: Sector > +0.75% -> BBW < 0.30, Vol > 1.8)",
            "desc": "Sector > +0.75% -> BBW < 0.30, Vol > 1.8x; Else BBW < 0.22, Vol > 2.5x",
            "bbw_std": 0.22, "vol_std": 2.5,
            "bbw_loosened": 0.30, "vol_loosened": 1.8,
            "sec_threshold": 0.75
        }
    ]

    def evaluate_configuration(cfg, target_df, period_name):
        triggered_trades = []
        
        # Group by symbol & day to avoid taking multiple entries on the same symbol on the same day
        # (Take first qualifying trigger candle of the day)
        for (sym, dt), day_candles in target_df.groupby(["symbol", "date"]):
            day_candles = day_candles.sort_values("time")
            for _, row in day_candles.iterrows():
                sec_ret = row["sector_intraday_ret"]
                is_sector_strong = sec_ret >= cfg["sec_threshold"]
                
                req_bbw = cfg["bbw_loosened"] if is_sector_strong else cfg["bbw_std"]
                req_vol = cfg["vol_loosened"] if is_sector_strong else cfg["vol_std"]
                
                # Math breakout conditions
                passes_bbw = row["bbw"] < req_bbw
                passes_vol = row["vol_surge"] >= req_vol
                passes_trend = row["close"] > row["sma_20"]
                passes_candle = row["close"] > row["open"]
                passes_extension = row["sma20_dist_pct"] <= 3.0 # Within 3% of SMA20
                
                if passes_bbw and passes_vol and passes_trend and passes_candle and passes_extension:
                    # Trigger trade!
                    outcomes = simulate_trade_outcomes(df, sym, row["time"], row["close"])
                    if outcomes:
                        triggered_trades.append({
                            "symbol": sym,
                            "date": dt,
                            "time": row["time"],
                            "sector": row["sector"],
                            "sector_ret": sec_ret,
                            "is_loosened_trigger": is_sector_strong and (row["bbw"] >= cfg["bbw_std"] or row["vol_surge"] < cfg["vol_std"]),
                            "bbw": row["bbw"],
                            "vol_surge": row["vol_surge"],
                            "sma20_dist_pct": row["sma20_dist_pct"],
                            "candle_body_pct": row["candle_body_pct"],
                            **outcomes
                        })
                    break # Only 1 trade per symbol per day
                    
        return triggered_trades

    # Run for Last Week (Aug 10 - Aug 14)
    last_week_df = pdf_fno[(pdf_fno["date"] >= last_week_start) & (pdf_fno["date"] <= last_week_end)]
    
    print("\n" + "="*100)
    print("📊 COMPARATIVE BACKTEST RESULTS: LAST WEEK (AUG 10 - AUG 14, 2026)")
    print("="*100)

    results_table = []
    detailed_trades_by_cfg = {}

    for cfg in configurations:
        trades = evaluate_configuration(cfg, last_week_df, "Last Week")
        detailed_trades_by_cfg[cfg["id"]] = trades
        
        n_trades = len(trades)
        winners = [t for t in trades if t["is_winner"]]
        losers = [t for t in trades if not t["is_winner"]]
        n_win = len(winners)
        n_loss = len(losers)
        win_rate = (n_win / n_trades * 100) if n_trades > 0 else 0.0
        
        avg_pnl = np.mean([t["intraday_ret_pct"] for t in trades]) if n_trades > 0 else 0.0
        total_pnl = sum([t["intraday_ret_pct"] for t in trades])
        
        avg_opt_pnl = np.mean([t["opt_pnl"] for t in trades]) if n_trades > 0 else 0.0
        total_opt_pnl = sum([t["opt_pnl"] for t in trades])
        
        gross_profit = sum([t["intraday_ret_pct"] for t in winners]) if winners else 0.0
        gross_loss = abs(sum([t["intraday_ret_pct"] for t in losers])) if losers else 0.001
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 999.0
        
        loosened_trades = [t for t in trades if t["is_loosened_trigger"]]
        loosened_winners = [t for t in loosened_trades if t["is_winner"]]
        loosened_losers = [t for t in loosened_trades if not t["is_winner"]]
        
        results_table.append({
            "Configuration": cfg["name"],
            "Total Trades": n_trades,
            "Winners": n_win,
            "Losers": n_loss,
            "Win Rate %": f"{win_rate:.1f}%",
            "Avg Stock PnL": f"{avg_pnl:+.2f}%",
            "Total Stock PnL": f"{total_pnl:+.2f}%",
            "Total Opt PnL": f"{total_opt_pnl:+.1f}%",
            "Profit Factor": f"{profit_factor:.2f}",
            "New Loosened Caught": f"+{len(loosened_trades)} ({len(loosened_winners)}W / {len(loosened_losers)}L)"
        })

    res_df = pd.DataFrame(results_table)
    print(res_df.to_string(index=False))

    # Detailed Inspection of the Extra Trades Caught
    print("\n" + "="*100)
    print("🔍 AUDIT OF TRADES CAUGHT BY FULL THEORY 2 (CONFIG 5) LAST WEEK")
    print("="*100)

    cfg5_trades = detailed_trades_by_cfg["config_theory2_full"]
    baseline_trades = detailed_trades_by_cfg["config_baseline"]
    baseline_symbols_dates = set((t["symbol"], t["date"]) for t in baseline_trades)

    print(f"\nTotal Baseline Trades: {len(baseline_trades)}")
    print(f"Total Theory 2 Trades: {len(cfg5_trades)}")
    print(f"Net Additional Trades: {len(cfg5_trades) - len(baseline_trades)}\n")

    header = f"{'Symbol':<12} | {'Date':<10} | {'Trigger Time':<18} | {'Sector':<16} | {'Sec Ret%':<9} | {'BBW':<6} | {'VolSurge':<8} | {'Stock PnL%':<11} | {'Max High%':<10} | {'Opt PnL%':<9} | {'Source':<16} | {'Outcome':<7}"
    print(header)
    print("-" * len(header))

    for t in cfg5_trades:
        is_extra = (t["symbol"], t["date"]) not in baseline_symbols_dates
        src = "🟢 EXTRA (THEORY 2)" if is_extra else "⚪ BASELINE"
        outcome = "WINNER" if t["is_winner"] else "LOSER"
        t_time_str = t["time"].strftime("%Y-%m-%d %H:%M")
        print(f"{t['symbol']:<12} | {str(t['date']):<10} | {t_time_str:<18} | {t['sector']:<16} | {t['sector_ret']:>+7.2f}% | {t['bbw']:>6.4f} | {t['vol_surge']:>6.2f}x | {t['intraday_ret_pct']:>+9.2f}% | {t['max_high_pct']:>+8.2f}% | {t['opt_pnl']:>+7.1f}% | {src:<16} | {outcome:<7}")

    # Full August 2026 Backtest
    print("\n" + "="*100)
    print("📈 EXTENDED MULTI-WEEK BACKTEST: FULL AUGUST 2026 (AUG 3 - AUG 14, 2026)")
    print("="*100)

    aug_full_df = pdf_fno[(pdf_fno["date"] >= full_month_start) & (pdf_fno["date"] <= full_month_end)]
    full_month_results = []
    
    for cfg in configurations:
        trades = evaluate_configuration(cfg, aug_full_df, "Full August")
        n_trades = len(trades)
        winners = [t for t in trades if t["is_winner"]]
        losers = [t for t in trades if not t["is_winner"]]
        n_win = len(winners)
        n_loss = len(losers)
        win_rate = (n_win / n_trades * 100) if n_trades > 0 else 0.0
        
        avg_pnl = np.mean([t["intraday_ret_pct"] for t in trades]) if n_trades > 0 else 0.0
        total_pnl = sum([t["intraday_ret_pct"] for t in trades])
        
        gross_profit = sum([t["intraday_ret_pct"] for t in winners]) if winners else 0.0
        gross_loss = abs(sum([t["intraday_ret_pct"] for t in losers])) if losers else 0.001
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else 999.0
        
        loosened_trades = [t for t in trades if t["is_loosened_trigger"]]
        loosened_winners = [t for t in loosened_trades if t["is_winner"]]
        loosened_losers = [t for t in loosened_trades if not t["is_winner"]]
        
        full_month_results.append({
            "Configuration": cfg["name"],
            "Total Trades": n_trades,
            "Winners": n_win,
            "Losers": n_loss,
            "Win Rate %": f"{win_rate:.1f}%",
            "Avg Stock PnL": f"{avg_pnl:+.2f}%",
            "Total Stock PnL": f"{total_pnl:+.2f}%",
            "Profit Factor": f"{profit_factor:.2f}",
            "Loosened Incremental": f"+{len(loosened_trades)} ({len(loosened_winners)}W / {len(loosened_losers)}L)"
        })

    res_full_df = pd.DataFrame(full_month_results)
    print(res_full_df.to_string(index=False))

    return detailed_trades_by_cfg

if __name__ == "__main__":
    run_backtest()
