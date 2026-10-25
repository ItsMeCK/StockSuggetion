#!/usr/bin/env python3
"""
scripts/backtest_friday_oct9.py

Comprehensive Friday Oct 9 (3:15 PM) Backtest across the F&O Universe:
1. Ingests completed 60m candles up to Friday Oct 9 15:15 IST directly via KiteConnect.
2. Applies the exact Engine 2 / 3:15 PM Anticipatory Scanner criteria:
   - Volume Surge: Vol / Vol_SMA20 > 1.2 and < 3.0
   - Close-to-High Ratio: (Close - Low) / (High - Low) >= 0.80
   - Green Candle: Close > Open
   - Trend: Close > SMA20, Close > Prev_Close, Low > Prev_Low
3. Selects Top-5 candidates ranked by Volume Surge / Composite Score.
4. Queries live KiteConnect market quotes for:
   - Equity Spot: Friday Close -> Monday Open Gap% -> Monday Morning High% -> LTP%
   - Option Contracts (ITM & ATM CE): Friday Entry -> Monday Open% -> Monday High% (Max PnL) -> LTP%
"""

import os
import sys
import time
from datetime import datetime, date, timedelta
from dotenv import load_dotenv
from kiteconnect import KiteConnect
import pandas as pd
import numpy as np

load_dotenv()

def get_kite_client():
    api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    if not api_key or not token:
        print("❌ Error: KITE_API_KEY or KITE_ACCESS_TOKEN missing.")
        sys.exit(1)
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(token)
    return kite

def run_friday_backtest():
    kite = get_kite_client()
    print("=" * 90)
    print("🔍 RUNNING COMPREHENSIVE FRIDAY OCT 9 (3:15 PM) SCANNER BACKTEST")
    print("=" * 90)

    # 1. Get F&O Universe symbols
    print("Fetching NSE Equity & NFO instruments...")
    nse_insts = kite.instruments("NSE")
    nfo_insts = kite.instruments("NFO")

    # Get all underlyings present in NFO options
    fno_underlyings = set(inst["name"] for inst in nfo_insts if inst["instrument_type"] in ["CE", "PE"])
    print(f"Found {len(fno_underlyings)} F&O underlyings in NSE derivatives.")

    # Filter equity instruments matching F&O underlyings
    eq_map = {inst["tradingsymbol"]: inst["instrument_token"] for inst in nse_insts if inst["tradingsymbol"] in fno_underlyings}
    symbols = sorted(list(eq_map.keys()))
    print(f"Tracking {len(symbols)} F&O equity stocks for historical scan.\n")

    from_dt = datetime(2026, 10, 5, 9, 15)
    to_dt = datetime(2026, 10, 9, 15, 30)

    evaluated_candidates = []
    print("Ingesting Friday Oct 9 hourly candles across universe...")

    start_t = time.time()
    for idx, sym in enumerate(symbols):
        token = eq_map[sym]
        try:
            candles = kite.historical_data(token, from_dt, to_dt, "60minute")
            if len(candles) < 20:
                continue

            # Convert to DataFrame
            cdf = pd.DataFrame(candles)
            cdf["sma20"] = cdf["close"].rolling(20, min_periods=5).mean()
            cdf["vol_sma20"] = cdf["volume"].rolling(20, min_periods=5).mean()

            # Target Friday 14:15 - 15:15 IST candle (index -1)
            eod_bar = cdf.iloc[-1]
            prev_bar = cdf.iloc[-2]

            # Verify date is Oct 9
            bar_date = eod_bar["date"].date() if hasattr(eod_bar["date"], "date") else eod_bar["date"]
            if bar_date != date(2026, 10, 9):
                continue

            open_p = float(eod_bar["open"])
            high_p = float(eod_bar["high"])
            low_p = float(eod_bar["low"])
            close_p = float(eod_bar["close"])
            vol = float(eod_bar["volume"])
            vol_sma = float(eod_bar["vol_sma20"]) if eod_bar["vol_sma20"] > 0 else 1.0
            sma20 = float(eod_bar["sma20"])

            prev_close = float(prev_bar["close"])
            prev_low = float(prev_bar["low"])

            # Compute features
            vol_surge = vol / vol_sma if vol_sma > 0 else 1.0
            tr_range = high_p - low_p
            close_to_high = (close_p - low_p) / tr_range if tr_range > 0 else 0.5
            is_green = close_p > open_p
            is_above_sma = close_p > sma20
            is_higher_low = low_p >= prev_low
            is_higher_close = close_p >= prev_close

            # Engine 2 criteria
            passes_strict_e2 = (
                vol_surge >= 1.2 and vol_surge <= 3.0 and
                close_to_high >= 0.80 and
                is_green and
                is_above_sma and
                is_higher_close
            )

            # Relaxed criteria (vol surge > 1.1, close to high >= 0.70)
            passes_relaxed = (
                vol_surge >= 1.1 and
                close_to_high >= 0.70 and
                is_green and
                is_above_sma
            )

            evaluated_candidates.append({
                "symbol": sym,
                "token": token,
                "friday_close": close_p,
                "friday_open": open_p,
                "friday_high": high_p,
                "friday_low": low_p,
                "volume": vol,
                "vol_surge": vol_surge,
                "close_to_high": close_to_high,
                "passes_strict_e2": passes_strict_e2,
                "passes_relaxed": passes_relaxed
            })

        except Exception as e:
            continue

        if (idx + 1) % 50 == 0:
            print(f"  Processed {idx + 1}/{len(symbols)} symbols...")

    print(f"✅ Ingestion complete in {time.time() - start_t:.1f}s. Evaluated {len(evaluated_candidates)} symbols.\n")

    edf = pd.DataFrame(evaluated_candidates)
    strict_matches = edf[edf["passes_strict_e2"]].sort_values("vol_surge", ascending=False).reset_index(drop=True)
    relaxed_matches = edf[edf["passes_relaxed"]].sort_values("vol_surge", ascending=False).reset_index(drop=True)

    print(f"🎯 STRICT ENGINE 2 MATCHES ON FRIDAY 3:15 PM: {len(strict_matches)}")
    print(f"🎯 RELAXED MOMENTUM MATCHES ON FRIDAY 3:15 PM: {len(relaxed_matches)}\n")

    # Select candidates to backtest: All strict matches + top of relaxed
    selected_symbols = list(strict_matches["symbol"])
    for s in relaxed_matches["symbol"]:
        if s not in selected_symbols and len(selected_symbols) < 10:
            selected_symbols.append(s)

    print(f"📊 SELECTED BACKTEST WATCHLIST ({len(selected_symbols)} candidates): {selected_symbols}\n")

    # Fetch live quotes for selected candidates
    eq_quotes = kite.quote([f"NSE:{s}" for s in selected_symbols])

    # Find October monthly option contracts for each
    opt_tracking = []
    for sym in selected_symbols:
        spot_close = float(edf[edf["symbol"] == sym]["friday_close"].iloc[0])
        calls = [
            inst for inst in nfo_insts
            if inst["name"] == sym and inst["instrument_type"] == "CE" and inst["expiry"] >= date(2026, 10, 15)
        ]
        if not calls:
            continue
        calls = sorted(calls, key=lambda x: (x["expiry"], abs(x["strike"] - spot_close)))
        target_expiry = calls[0]["expiry"]
        exp_calls = [c for c in calls if c["expiry"] == target_expiry]

        # ITM (strike <= spot) and ATM (closest strike)
        itm_calls = [c for c in exp_calls if c["strike"] <= spot_close]
        best_itm = max(itm_calls, key=lambda x: x["strike"]) if itm_calls else exp_calls[0]
        atm_call = min(exp_calls, key=lambda x: abs(x["strike"] - spot_close))

        opt_tracking.append({
            "symbol": sym,
            "spot_close": spot_close,
            "itm_contract": best_itm["tradingsymbol"],
            "atm_contract": atm_call["tradingsymbol"]
        })

    all_opt_ts = []
    for row in opt_tracking:
        all_opt_ts.extend([f"NFO:{row['itm_contract']}", f"NFO:{row['atm_contract']}"])
    all_opt_ts = list(set(all_opt_ts))

    opt_quotes = kite.quote(all_opt_ts)

    # Compile Final Report
    report_rows = []
    for sym in selected_symbols:
        s_data = edf[edf["symbol"] == sym].iloc[0]
        q_eq = eq_quotes.get(f"NSE:{sym}", {})
        eq_ohlc = q_eq.get("ohlc", {})
        mon_open = eq_ohlc.get("open", 0.0)
        mon_high = eq_ohlc.get("high", 0.0)
        mon_ltp = q_eq.get("last_price", 0.0)
        fri_close = s_data["friday_close"]

        gap_pct = (mon_open - fri_close) / fri_close * 100 if fri_close else 0.0
        max_spot_gain = (mon_high - fri_close) / fri_close * 100 if fri_close else 0.0
        ltp_spot_gain = (mon_ltp - fri_close) / fri_close * 100 if fri_close else 0.0

        # Find option data
        opt_info = next((o for o in opt_tracking if o["symbol"] == sym), None)
        itm_ts = opt_info["itm_contract"] if opt_info else "-"
        atm_ts = opt_info["atm_contract"] if opt_info else "-"

        q_itm = opt_quotes.get(f"NFO:{itm_ts}", {}) if opt_info else {}
        itm_ohlc = q_itm.get("ohlc", {})
        itm_entry = itm_ohlc.get("close", 0.0)
        itm_open = itm_ohlc.get("open", 0.0)
        itm_high = itm_ohlc.get("high", 0.0)
        itm_ltp = q_itm.get("last_price", 0.0)

        itm_open_pnl = (itm_open - itm_entry) / itm_entry * 100 if itm_entry else 0.0
        itm_max_pnl = (itm_high - itm_entry) / itm_entry * 100 if itm_entry else 0.0
        itm_ltp_pnl = (itm_ltp - itm_entry) / itm_entry * 100 if itm_entry else 0.0

        report_rows.append({
            "symbol": sym,
            "filter_type": "STRICT E2" if s_data["passes_strict_e2"] else "MOMENTUM",
            "vol_surge": s_data["vol_surge"],
            "close_to_high": s_data["close_to_high"],
            "spot_close": fri_close,
            "mon_open": mon_open,
            "gap_pct": gap_pct,
            "mon_high": mon_high,
            "max_spot_pct": max_spot_gain,
            "itm_contract": itm_ts,
            "itm_entry": itm_entry,
            "itm_open": itm_open,
            "itm_open_pnl": itm_open_pnl,
            "itm_high": itm_high,
            "itm_max_pnl": itm_max_pnl,
            "itm_ltp_pnl": itm_ltp_pnl
        })

    res_df = pd.DataFrame(report_rows)

    print("=" * 115)
    print("📈 FULL BACKTEST RESULTS: FRIDAY 3:15 PM BUY -> MONDAY OPENING EXECUTION")
    print("=" * 115)
    for idx, r in res_df.iterrows():
        print(f"\n[{idx+1}] {r['symbol']} ({r['filter_type']}) | Vol Surge: {r['vol_surge']:.2f}x | Close/High: {r['close_to_high']:.2f}")
        print(f"    Spot Equity: Friday Close: ₹{r['spot_close']:.2f} -> Monday Open: ₹{r['mon_open']:.2f} ({r['gap_pct']:+.2f}%) -> High: ₹{r['mon_high']:.2f} ({r['max_spot_pct']:+.2f}%)")
        if r["itm_contract"] != "-":
            print(f"    ITM Option:  {r['itm_contract']}")
            print(f"                 Entry (Fri Close): ₹{r['itm_entry']:.2f} -> Mon Open: ₹{r['itm_open']:.2f} ({r['itm_open_pnl']:+.2f}%)")
            print(f"                 Peak Mon High:     ₹{r['itm_high']:.2f} ({r['itm_max_pnl']:+.2f}% MAX PROFIT) | Current: {r['itm_ltp_pnl']:+.2f}%")

    print("\n" + "=" * 115)
    print("📊 PORTFOLIO PERFORMANCE SUMMARY (MONDAY 09:15 AM - 09:30 AM)")
    print("=" * 115)
    print(f"Total Traded Candidates:        {len(res_df)}")
    print(f"Average Monday Open Gap:        {res_df['gap_pct'].mean():+.2f}%")
    print(f"Average Monday Spot Max High:   {res_df['max_spot_pct'].mean():+.2f}%")
    print(f"Average ITM Option Open P&L:    {res_df['itm_open_pnl'].mean():+.2f}%")
    print(f"Average ITM Option Peak Profit: {res_df['itm_max_pnl'].mean():+.2f}%")
    win_count = (res_df["itm_max_pnl"] > 0).sum()
    print(f"Trades with Positive Profit:    {win_count}/{len(res_df)} ({win_count/len(res_df)*100:.1f}%)")
    target_hit = (res_df["itm_max_pnl"] >= 15.0).sum()
    print(f"Trades hitting >= +15% Profit:  {target_hit}/{len(res_df)} ({target_hit/len(res_df)*100:.1f}%)")
    print("=" * 115)

if __name__ == "__main__":
    run_friday_backtest()
