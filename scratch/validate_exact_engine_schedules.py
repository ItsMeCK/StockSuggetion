import os
import csv
import math
import numpy as np
import polars as pl
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from scipy.stats import norm

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

def run_schedule_accurate_backtest():
    print("=" * 90)
    print("🔬 SCHEDULE-ACCURATE BACKTEST: ENGINE 1 (10:15 - 14:15) & ENGINE 2 (3:15 PM BTST)")
    print("Dataset: data/intraday_ohlcv.parquet (Mapped to Asia/Kolkata IST)")
    print("=" * 90)

    # 1. Load F&O Universe
    fo_symbols = set()
    with open('pipeline/fo_universe.csv', 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r.get('Symbol'): fo_symbols.add(r['Symbol'])
    print(f"Loaded {len(fo_symbols)} F&O underlyings.")

    # 2. Load and Prepare Parquet Data with IST Timezone
    df = pl.read_parquet('data/intraday_ohlcv.parquet')
    df = df.filter(pl.col('symbol').is_in(list(fo_symbols))).sort(['symbol', 'time'])
    
    # Convert UTC time to IST
    df = df.with_columns(
        pl.col("time").dt.convert_time_zone("Asia/Kolkata").alias("ist_time")
    )
    df = df.with_columns([
        pl.col("ist_time").dt.strftime("%H:%M").alias("ist_time_str"),
        pl.col("ist_time").dt.date().alias("trade_date")
    ])

    # Compute Rolling Indicators per stock
    df = df.with_columns([
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
    ])

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

    # Track Engine 1 (Hourly 10:15 - 14:15)
    e1_baseline = []
    e1_quant = []

    # Track Engine 2 (BTST 15:15 Entry -> T+1 09:15 Morning Exit)
    e2_baseline = []
    e2_quant = []

    dte_years = 14.0 / 365.0
    r = 0.07
    sigma = 0.35

    # Mapping of live schedule to completed candle timestamps:
    # At 10:15 IST -> evaluates completed 09:15 candle
    # At 11:15 IST -> evaluates completed 10:15 candle
    # At 12:15 IST -> evaluates completed 11:15 candle
    # At 13:15 IST -> evaluates completed 12:15 candle
    # At 14:15 IST -> evaluates completed 13:15 candle
    # At 15:15 IST (Engine 2 BTST) -> evaluates completed 14:15 candle
    engine_1_candle_times = {"09:15", "10:15", "11:15", "12:15", "13:15"}
    engine_2_candle_time = "14:15"

    for sym in symbols:
        sym_df = pdf[pdf['symbol'] == sym].reset_index(drop=True)
        n = len(sym_df)
        if n < 30: continue

        for i in range(20, n - 2):
            row = sym_df.iloc[i]
            candle_t_str = row['ist_time_str']
            current_date = row['trade_date']
            spot_entry = row['close']
            vol_surge = row['vol_surge']
            close_to_high = row['close_to_high']
            close_to_low = row['close_to_low']
            sma_ext = row['sma_ext_pct']

            # =================================================================
            # A. ENGINE 1: HOURLY SCANNER (Runs 10:15 - 14:15 on candles 09:15 - 13:15)
            # Mandate: Strictly Intraday. Must squash at 15:15 IST EOD.
            # =================================================================
            if candle_t_str in engine_1_candle_times:
                # Find all subsequent candles on the SAME trading day
                same_day_future = []
                for f_idx in range(i + 1, min(i + 7, n)):
                    f_row = sym_df.iloc[f_idx]
                    if f_row['trade_date'] == current_date:
                        same_day_future.append(f_row)
                    else:
                        break # End of day reached

                if same_day_future:
                    # --- BASELINE (Model A: 1% OTM, Long-Only, 50% SL) ---
                    if (vol_surge > 1.2 and row['close'] > row['open'] and 
                        close_to_high > 0.85 and row['close'] > row['sma_20']):
                        
                        k_otm = round(spot_entry * 1.01, 1)
                        entry_opt = bs_call_price(spot_entry, k_otm, dte_years, r, sigma)
                        if entry_opt > 0.1:
                            pnl = 0.0
                            for step, f_row in enumerate(same_day_future):
                                t_dec = dte_years - ((step + 1) * 1.0 / (252.0 * 6.0))
                                curr_opt = bs_call_price(f_row['close'], k_otm, t_dec, r, sigma)
                                ret = (curr_opt - entry_opt) / entry_opt * 100.0
                                if ret <= -50.0:
                                    pnl = -50.0
                                    break
                            else:
                                final_opt = bs_call_price(same_day_future[-1]['close'], k_otm, dte_years - (len(same_day_future)/(252*6)), r, sigma)
                                pnl = (final_opt - entry_opt) / entry_opt * 100.0
                            e1_baseline.append(pnl)

                    # --- QUANT-HARDENED (Model B: ITM Delta 0.65, Bi-Dir, -15% SL, +15% Ratchet, EOD Cut) ---
                    # Long Call Setup
                    if (1.4 <= vol_surge <= 3.2 and row['close'] > row['open'] and 
                        close_to_high > 0.85 and 0.0 < sma_ext <= 2.5):
                        k_itm = round(spot_entry * 0.98, 1) # ITM Call
                        entry_opt = bs_call_price(spot_entry, k_itm, dte_years, r, sigma)
                        if entry_opt > 0.1:
                            pnl = 0.0
                            ratchet = False
                            profit_lock = False
                            for step, f_row in enumerate(same_day_future):
                                t_dec = dte_years - ((step + 1) * 1.0 / (252.0 * 6.0))
                                curr_opt = bs_call_price(f_row['close'], k_itm, t_dec, r, sigma)
                                ret = (curr_opt - entry_opt) / entry_opt * 100.0
                                if ret >= 25.0: profit_lock = True
                                if ret >= 15.0: ratchet = True
                                if ret <= -15.0:
                                    pnl = -15.0
                                    break
                                if profit_lock and ret <= 15.0:
                                    pnl = 15.0
                                    break
                                if ratchet and ret <= 0.0:
                                    pnl = 0.0
                                    break
                            else:
                                final_opt = bs_call_price(same_day_future[-1]['close'], k_itm, dte_years - (len(same_day_future)/(252*6)), r, sigma)
                                pnl = (final_opt - entry_opt) / entry_opt * 100.0
                            e1_quant.append(pnl)

                    # Short Put Setup
                    if (1.4 <= vol_surge <= 3.2 and row['close'] < row['open'] and 
                        close_to_low > 0.85 and -2.5 <= sma_ext < 0.0):
                        k_itm_put = round(spot_entry * 1.02, 1) # ITM Put
                        entry_opt = bs_put_price(spot_entry, k_itm_put, dte_years, r, sigma)
                        if entry_opt > 0.1:
                            pnl = 0.0
                            ratchet = False
                            profit_lock = False
                            for step, f_row in enumerate(same_day_future):
                                t_dec = dte_years - ((step + 1) * 1.0 / (252.0 * 6.0))
                                curr_opt = bs_put_price(f_row['close'], k_itm_put, t_dec, r, sigma)
                                ret = (curr_opt - entry_opt) / entry_opt * 100.0
                                if ret >= 25.0: profit_lock = True
                                if ret >= 15.0: ratchet = True
                                if ret <= -15.0:
                                    pnl = -15.0
                                    break
                                if profit_lock and ret <= 15.0:
                                    pnl = 15.0
                                    break
                                if ratchet and ret <= 0.0:
                                    pnl = 0.0
                                    break
                            else:
                                final_opt = bs_put_price(same_day_future[-1]['close'], k_itm_put, dte_years - (len(same_day_future)/(252*6)), r, sigma)
                                pnl = (final_opt - entry_opt) / entry_opt * 100.0
                            e1_quant.append(pnl)

            # =================================================================
            # B. ENGINE 2: 3:15 PM INSIDER SNIPER (Evaluates 14:15 candle at 15:15 IST)
            # Mandate: Held overnight. Exited next morning at 09:15 IST on T+1.
            # =================================================================
            elif candle_t_str == engine_2_candle_time:
                # Find the next trading day's opening candle (09:15 IST on T+1)
                t_plus_1_row = None
                for f_idx in range(i + 1, min(i + 4, n)):
                    f_row = sym_df.iloc[f_idx]
                    if f_row['trade_date'] > current_date:
                        t_plus_1_row = f_row
                        break

                if t_plus_1_row is not None:
                    # Baseline BTST (Legacy: OTM 1% Call, Uncapped overnight risk)
                    if vol_surge > 1.3 and row['close'] > row['open'] and close_to_high > 0.80:
                        k_otm = round(spot_entry * 1.01, 1)
                        entry_opt = bs_call_price(spot_entry, k_otm, dte_years, r, sigma)
                        if entry_opt > 0.1:
                            t_dec = dte_years - (1.0 / 365.0) # Overnight theta decay
                            next_opt = bs_call_price(t_plus_1_row['open'], k_otm, t_dec, r, sigma)
                            pnl = (next_opt - entry_opt) / entry_opt * 100.0
                            e2_baseline.append(pnl)

                    # Quant-Hardened BTST (ITM Delta 0.65 Call, Anti-FOMO <=2.5%, 09:15 Morning Liquidation)
                    if (1.4 <= vol_surge <= 3.2 and row['close'] > row['open'] and 
                        close_to_high > 0.85 and 0.0 < sma_ext <= 2.5):
                        k_itm = round(spot_entry * 0.98, 1) # ITM Delta 0.65
                        entry_opt = bs_call_price(spot_entry, k_itm, dte_years, r, sigma)
                        if entry_opt > 0.1:
                            t_dec = dte_years - (1.0 / 365.0)
                            next_opt = bs_call_price(t_plus_1_row['open'], k_itm, t_dec, r, sigma)
                            pnl = (next_opt - entry_opt) / entry_opt * 100.0
                            e2_quant.append(pnl)

    # Output Comparative Results
    def summarize(name, arr):
        if not arr:
            print(f"[{name}] No trades executed.")
            return
        wins = [x for x in arr if x > 0.1]
        losses = [x for x in arr if x < -0.1]
        wr = len(wins) / len(arr) * 100.0
        avg_w = np.mean(wins) if wins else 0.0
        avg_l = np.mean(losses) if losses else 0.0
        pf = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else 0.0
        exp = np.mean(arr)
        print(f"\n--- {name} ---")
        print(f"Total Trades Evaluated : {len(arr)}")
        print(f"Win Rate               : {wr:.1f}% ({len(wins)} W, {len(losses)} L)")
        print(f"Average Winner         : +{avg_w:.2f}%")
        print(f"Average Loser          : {avg_l:.2f}%")
        print(f"Profit Factor          : {pf:.2f}")
        print(f"Mathematical Expectancy: {exp:+.2f}% per trade")

    print("\n" + "=" * 40 + " ENGINE 1: HOURLY INTRADAY RESULTS (10:15 - 14:15 IST) " + "=" * 40)
    summarize("ENGINE 1: BASELINE (BEFORE - 1% OTM, 50% SL, Long-Only)", e1_baseline)
    summarize("ENGINE 1: QUANT-HARDENED (AFTER - ITM 0.65, Bi-Dir, -15% SL)", e1_quant)

    print("\n" + "=" * 40 + " ENGINE 2: 3:15 PM BTST OVERNIGHT RESULTS " + "=" * 40)
    summarize("ENGINE 2: BASELINE (BEFORE - 1% OTM Call, Unhedged)", e2_baseline)
    summarize("ENGINE 2: QUANT-HARDENED (AFTER - ITM Delta 0.65, 09:15 Exit)", e2_quant)

if __name__ == '__main__':
    run_schedule_accurate_backtest()
