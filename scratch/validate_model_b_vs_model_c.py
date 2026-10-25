import os
import csv
import math
import numpy as np
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from scipy.stats import norm
from core.dynamic_exit_engine import calculate_statutory_friction

def bs_call_price(s, k, t, r=0.07, sigma=0.35):
    if t <= 0: return max(0.0, s - k)
    d1 = (math.log(s / k) + (r + 0.5 * sigma ** 2) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    return s * norm.cdf(d1) - k * math.exp(-r * t) * norm.cdf(d2)

def bs_put_price(s, k, t, r=0.07, sigma=0.35):
    if t <= 0: return max(0.0, k - s)
    d1 = (math.log(s / k) + (r + 0.5 * sigma ** 2) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    return k * math.exp(-r * t) * norm.cdf(-d2) - s * norm.cdf(-d1)

def run_baseline_improvement_backtest():
    print("=" * 90)
    print("🔬 EMPIRICAL BACKTEST: MODEL B (43.7% BASELINE) VS. MODEL C (INSTITUTIONAL REFINED)")
    print("Dataset: data/intraday_ohlcv.parquet (52,380 candles across 215 F&O Stocks)")
    print("=" * 90)

    # 1. Load F&O Universe
    fo_symbols = set()
    with open('pipeline/fo_universe.csv', 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r.get('Symbol'): fo_symbols.add(r['Symbol'])
    print(f"Loaded {len(fo_symbols)} F&O underlyings.")

    # 2. Load Hourly Data
    df = pl.read_parquet('data/intraday_ohlcv.parquet')
    df = df.filter(pl.col('symbol').is_in(list(fo_symbols))).sort(['symbol', 'time'])
    
    # Timezone conversion
    df = df.with_columns(
        pl.col("time").dt.convert_time_zone("Asia/Kolkata").alias("ist_time")
    )
    df = df.with_columns([
        pl.col("ist_time").dt.strftime("%H:%M").alias("ist_time_str"),
        pl.col("ist_time").dt.date().alias("trade_date")
    ])

    # Compute Indicators
    df = df.with_columns([
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
    ])

    # Compute Cross-Sectional Market Breadth per timestamp
    breadth = df.group_by('time').agg([
        (pl.col('close') > pl.col('open')).sum().alias('advances'),
        (pl.col('close') < pl.col('open')).sum().alias('declines')
    ]).with_columns(
        (pl.col('advances') / pl.when(pl.col('declines') > 0).then(pl.col('declines')).otherwise(1.0)).alias('ad_ratio')
    )
    df = df.join(breadth.select(['time', 'ad_ratio']), on='time', how='left')

    df = df.with_columns([
        (pl.col("volume") / pl.when(pl.col("vol_sma_20") > 0).then(pl.col("vol_sma_20")).otherwise(1.0)).alias("vol_surge"),
        pl.when(pl.col("high") != pl.col("low"))
          .then((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low")))
          .otherwise(0.5).alias("close_to_high"),
        pl.when(pl.col("high") != pl.col("low"))
          .then((pl.col("high") - pl.col("close")) / (pl.col("high") - pl.col("low")))
          .otherwise(0.5).alias("close_to_low"),
        ((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20") * 100).alias("sma_ext_pct")
    ])

    pdf = df.to_pandas()
    symbols = pdf['symbol'].unique()

    results_model_b = []
    results_model_c = []

    dte_years = 14.0 / 365.0
    r = 0.07
    sigma = 0.35
    lot_size = 500  # Representative lot size for single stock options

    for sym in symbols:
        sym_df = pdf[pdf['symbol'] == sym].reset_index(drop=True)
        n = len(sym_df)
        if n < 30: continue

        for i in range(20, n - 4):
            row = sym_df.iloc[i]
            t_str = row['ist_time_str']
            spot_entry = row['close']
            vol_surge = row['vol_surge']
            close_to_high = row['close_to_high']
            close_to_low = row['close_to_low']
            sma_ext = row['sma_ext_pct']

            # =====================================================================
            # 1. MODEL B: QUANT-HARDENED BASELINE (Our 43.7% Win Rate Model)
            # =====================================================================
            # Criteria: 1.4 <= vol_surge <= 3.2, close near high/low > 0.85, sma_ext <= 2.5%
            # Long Call Setup
            if (1.4 <= vol_surge <= 3.2 and row['close'] > row['open'] and 
                close_to_high > 0.85 and 0.0 < sma_ext <= 2.5):
                k_itm = round(spot_entry * 0.98, 1) # ITM Call (Delta ~0.65)
                entry_opt = bs_call_price(spot_entry, k_itm, dte_years, r, sigma)
                if entry_opt > 0.1:
                    pnl_pct = 0.0
                    ratchet = False
                    profit_locked = False
                    for h in range(1, 4):
                        next_row = sym_df.iloc[i + h]
                        t_dec = dte_years - (h * 1.0 / (252.0 * 6.0))
                        curr_opt = bs_call_price(next_row['close'], k_itm, t_dec, r, sigma)
                        ret = (curr_opt - entry_opt) / entry_opt * 100.0
                        if ret >= 25.0: profit_locked = True
                        if ret >= 15.0: ratchet = True
                        if ret <= -15.0:
                            pnl_pct = -15.0
                            break
                        if profit_locked and ret <= 15.0:
                            pnl_pct = 15.0
                            break
                        if ratchet and ret <= 0.0:
                            pnl_pct = 0.0
                            break
                    else:
                        last_row = sym_df.iloc[i + 3]
                        final_opt = bs_call_price(last_row['close'], k_itm, dte_years - (3.0/(252*6)), r, sigma)
                        pnl_pct = (final_opt - entry_opt) / entry_opt * 100.0
                    results_model_b.append(pnl_pct)

            # Short Put Setup
            if (1.4 <= vol_surge <= 3.2 and row['close'] < row['open'] and 
                close_to_low > 0.85 and -2.5 <= sma_ext < 0.0):
                k_itm_put = round(spot_entry * 1.02, 1) # ITM Put (Delta ~ -0.65)
                entry_opt = bs_put_price(spot_entry, k_itm_put, dte_years, r, sigma)
                if entry_opt > 0.1:
                    pnl_pct = 0.0
                    ratchet = False
                    profit_locked = False
                    for h in range(1, 4):
                        next_row = sym_df.iloc[i + h]
                        t_dec = dte_years - (h * 1.0 / (252.0 * 6.0))
                        curr_opt = bs_put_price(next_row['close'], k_itm_put, t_dec, r, sigma)
                        ret = (curr_opt - entry_opt) / entry_opt * 100.0
                        if ret >= 25.0: profit_locked = True
                        if ret >= 15.0: ratchet = True
                        if ret <= -15.0:
                            pnl_pct = -15.0
                            break
                        if profit_locked and ret <= 15.0:
                            pnl_pct = 15.0
                            break
                        if ratchet and ret <= 0.0:
                            pnl_pct = 0.0
                            break
                    else:
                        last_row = sym_df.iloc[i + 3]
                        final_opt = bs_put_price(last_row['close'], k_itm_put, dte_years - (3.0/(252*6)), r, sigma)
                        pnl_pct = (final_opt - entry_opt) / entry_opt * 100.0
                    results_model_b.append(pnl_pct)

            # =====================================================================
            # 2. MODEL C: INSTITUTIONAL-REFINED (MODEL B + GATE 2 COST + RS FILTER + EXHAUSTION CAP)
            # =====================================================================
            # In live production, the 09:15 candle finishes at 10:15 AM (the first hourly run).
            # Refinement 1: Tighter Volume Surge window (1.4 <= vol_surge <= 2.8 - rejecting exhaustion tops >2.8x)
            # Refinement 2: Gate 2 Statutory Cost Gate (Expected profit >= 2x friction)
            # Refinement 3: Close strength: close_to_high > 0.88 for Calls, close_to_low > 0.88 for Puts

            ad_ratio = float(row.get('ad_ratio', 1.0) or 1.0)

            # Long Call with Gate 2 Cost Filter & Bullish/Neutral Market Breadth (ad_ratio >= 1.0)
            if (1.4 <= vol_surge <= 2.8 and row['close'] > row['open'] and 
                close_to_high > 0.88 and 0.0 < sma_ext <= 2.0 and ad_ratio >= 1.0):
                k_itm = round(spot_entry * 0.98, 1)
                entry_opt = bs_call_price(spot_entry, k_itm, dte_years, r, sigma)
                if entry_opt > 0.1:
                    friction = calculate_statutory_friction(entry_opt, entry_opt * 1.20, lot_size)
                    round_trip_cost = friction["total_cost"]
                    expected_profit = entry_opt * 0.20 * lot_size
                    if expected_profit >= 2.0 * round_trip_cost:
                        pnl_pct = 0.0
                        ratchet = False
                        profit_locked = False
                        for h in range(1, 4):
                            next_row = sym_df.iloc[i + h]
                            t_dec = dte_years - (h * 1.0 / (252.0 * 6.0))
                            curr_opt = bs_call_price(next_row['close'], k_itm, t_dec, r, sigma)
                            ret = (curr_opt - entry_opt) / entry_opt * 100.0
                            if ret >= 25.0: profit_locked = True
                            if ret >= 15.0: ratchet = True
                            if ret <= -15.0:
                                pnl_pct = -15.0
                                break
                            if profit_locked and ret <= 15.0:
                                pnl_pct = 15.0
                                break
                            if ratchet and ret <= 0.0:
                                pnl_pct = 0.0
                                break
                        else:
                            last_row = sym_df.iloc[i + 3]
                            final_opt = bs_call_price(last_row['close'], k_itm, dte_years - (3.0/(252*6)), r, sigma)
                            pnl_pct = (final_opt - entry_opt) / entry_opt * 100.0
                        results_model_c.append(pnl_pct)

            # Short Put with Gate 2 Cost Filter & Bearish/Neutral Market Breadth (ad_ratio <= 1.0)
            if (1.4 <= vol_surge <= 2.8 and row['close'] < row['open'] and 
                close_to_low > 0.88 and -2.0 <= sma_ext < 0.0 and ad_ratio <= 1.0):
                k_itm_put = round(spot_entry * 1.02, 1)
                entry_opt = bs_put_price(spot_entry, k_itm_put, dte_years, r, sigma)
                if entry_opt > 0.1:
                    friction = calculate_statutory_friction(entry_opt, entry_opt * 1.20, lot_size)
                    round_trip_cost = friction["total_cost"]
                    expected_profit = entry_opt * 0.20 * lot_size
                    if expected_profit >= 2.0 * round_trip_cost:
                        pnl_pct = 0.0
                        ratchet = False
                        profit_locked = False
                        for h in range(1, 4):
                            next_row = sym_df.iloc[i + h]
                            t_dec = dte_years - (h * 1.0 / (252.0 * 6.0))
                            curr_opt = bs_put_price(next_row['close'], k_itm_put, t_dec, r, sigma)
                            ret = (curr_opt - entry_opt) / entry_opt * 100.0
                            if ret >= 25.0: profit_locked = True
                            if ret >= 15.0: ratchet = True
                            if ret <= -15.0:
                                pnl_pct = -15.0
                                break
                            if profit_locked and ret <= 15.0:
                                pnl_pct = 15.0
                                break
                            if ratchet and ret <= 0.0:
                                pnl_pct = 0.0
                                break
                        else:
                            last_row = sym_df.iloc[i + 3]
                            final_opt = bs_put_price(last_row['close'], k_itm_put, dte_years - (3.0/(252*6)), r, sigma)
                            pnl_pct = (final_opt - entry_opt) / entry_opt * 100.0
                        results_model_c.append(pnl_pct)

    def print_metrics(name, data):
        wins = [x for x in data if x > 0.1]
        losses = [x for x in data if x < -0.1]
        be = [x for x in data if abs(x) <= 0.1]
        wr = len(wins) / len(data) * 100.0 if data else 0.0
        avg_w = np.mean(wins) if wins else 0.0
        avg_l = np.mean(losses) if losses else 0.0
        pf = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else 0.0
        exp = np.mean(data) if data else 0.0
        print(f"\n============================== {name} ==============================")
        print(f"Total Trades Evaluated : {len(data)}")
        print(f"Win Rate               : {wr:.1f}% ({len(wins)} Wins, {len(losses)} Losses, {len(be)} Breakeven)")
        print(f"Average Winner         : +{avg_w:.2f}%")
        print(f"Average Loser          : {avg_l:.2f}%")
        print(f"Win/Loss Payoff Ratio  : {(abs(avg_w/avg_l) if avg_l != 0 else 0.0):.2f}x")
        print(f"Profit Factor          : {pf:.2f}")
        print(f"Mathematical Expectancy: {exp:+.2f}% per trade")

    print_metrics("MODEL B: BASELINE (Prior Quant-Hardened)", results_model_b)
    print_metrics("MODEL C: INSTITUTIONAL REFINED (Model B + Gate 2 Cost + Noise Filter)", results_model_c)

if __name__ == '__main__':
    run_baseline_improvement_backtest()
