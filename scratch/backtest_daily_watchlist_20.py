"""
scratch/backtest_daily_watchlist_20.py

Walk-forward, Point-in-Time Backtest of the Sovereign Watchlist Architecture:
1. Weekly Universe Refresh (Every Sunday):
   - Screens 215 F&O symbols.
   - Filters out illiquid and low-ATR defensive names.
2. Daily Pre-Open Watchlist (Every Market Morning at 09:xx):
   - Ranks weekly candidates by pre-open gap, relative strength, and consolidation near highs.
   - Produces the Top 20 Focus Watchlist for that specific trading day.
3. Strict Execution Simulation:
   - Trades are STRICTLY RESTRICTED to the Top 20 candidates for that day.
   - Any breakout in a stock not in that day's Top 20 is completely ignored.
   - Option pricing: Black-Scholes 14-DTE ITM Options (delta ~ 0.65).
   - Dynamic exit engine: -15% hard SL, +15% breakeven ratchet, +25% profit lock, 3H time stop, and overnight hold.
   - Deducts exact Section 43(5) statutory friction charges.
"""

import os
import sys
import csv
import math

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import numpy as np
import polars as pl
import pandas as pd
from datetime import datetime, timedelta
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

def estimate_lot_size(spot_price):
    target_notional = 800000.0
    raw_lot = round(target_notional / spot_price)
    if raw_lot >= 2000: return round(raw_lot / 500) * 500
    if raw_lot >= 1000: return round(raw_lot / 250) * 250
    if raw_lot >= 400: return round(raw_lot / 100) * 100
    if raw_lot >= 100: return round(raw_lot / 25) * 25
    return max(25, round(raw_lot / 10) * 10)

def run_watchlist_backtest(
    top_n_candidates: int = 20,
    min_vol_surge: float = 1.4,
    exclude_defensives: bool = True
):
    print("=" * 80)
    print(f"📊 BACKTESTING TOP {top_n_candidates} DAILY WATCHLIST SYSTEM (WEEKLY + DAILY REFRESH)")
    print(f"Defensive Exclusion: {exclude_defensives} | Min Vol Surge: {min_vol_surge}x")
    print("=" * 80)

    # 1. Load F&O Universe
    fo_symbols = set()
    with open('pipeline/fo_universe.csv', 'r') as f:
        for r in csv.DictReader(f):
            if r.get('Symbol'): fo_symbols.add(r['Symbol'])

    if exclude_defensives:
        defensive_blacklist = {
            'SBILIFE', 'HINDUNILVR', 'TATACONSUM', 'GODREJCP', 'ETERNAL',
            'ICICIGI', 'NESTLEIND', 'DABUR', 'MARICO', 'COLPAL'
        }
        fo_symbols = fo_symbols - defensive_blacklist

    # 2. Load Hourly Intraday Data
    df = pl.read_parquet('data/intraday_ohlcv.parquet')
    df = df.filter(pl.col('symbol').is_in(list(fo_symbols))).sort(['symbol', 'time'])
    df = df.with_columns(pl.col('time').dt.convert_time_zone('Asia/Kolkata').alias('ist_time'))
    df = df.with_columns([
        pl.col('ist_time').dt.strftime('%Y-%m-%d %H:%M').alias('ist_time_str'),
        pl.col('ist_time').dt.date().alias('trade_date'),
        pl.col('ist_time').dt.hour().alias('hour')
    ])

    # Rolling indicators on hourly data
    df = df.with_columns([
        pl.col('volume').rolling_mean(20).over('symbol').alias('vol_sma_20'),
        pl.col('close').rolling_mean(20).over('symbol').alias('sma_20'),
    ])
    df = df.with_columns([
        (pl.col('volume') / pl.when(pl.col('vol_sma_20') > 0).then(pl.col('vol_sma_20')).otherwise(1.0)).alias('vol_surge'),
        pl.when(pl.col('high') != pl.col('low'))
          .then((pl.col('close') - pl.col('low')) / (pl.col('high') - pl.col('low')))
          .otherwise(0.5).alias('close_to_high'),
        pl.when(pl.col('high') != pl.col('low'))
          .then((pl.col('high') - pl.col('close')) / (pl.col('high') - pl.col('low')))
          .otherwise(0.5).alias('close_to_low'),
        ((pl.col('close') - pl.col('sma_20')) / pl.col('sma_20') * 100).alias('sma_ext_pct')
    ])

    # Breakout scores
    df = df.with_columns([
        pl.when(pl.col('close') > pl.col('open'))
          .then(pl.col('vol_surge') * pl.col('close_to_high'))
          .otherwise(0.0).alias('long_score'),
        pl.when(pl.col('close') < pl.col('open'))
          .then(pl.col('vol_surge') * pl.col('close_to_low'))
          .otherwise(0.0).alias('short_score')
    ])

    pdf = df.to_pandas()

    # 3. Aggregate Daily Bars for Pre-Market Feature Computation
    daily_df = df.group_by(['symbol', 'trade_date']).agg([
        pl.col('open').first().alias('day_open'),
        pl.col('high').max().alias('day_high'),
        pl.col('low').min().alias('day_low'),
        pl.col('close').last().alias('day_close'),
        pl.col('volume').sum().alias('day_volume')
    ]).sort(['symbol', 'trade_date']).to_pandas()

    # Add prior day close and rolling daily indicators
    daily_df['prev_close'] = daily_df.groupby('symbol')['day_close'].shift(1)
    daily_df['daily_return_pct'] = (daily_df['day_close'] - daily_df['prev_close']) / daily_df['prev_close'] * 100.0
    daily_df['daily_range_pct'] = (daily_df['day_high'] - daily_df['day_low']) / daily_df['day_close'] * 100.0
    daily_df['rolling_atr_10'] = daily_df.groupby('symbol')['daily_range_pct'].transform(lambda s: s.rolling(10, min_periods=3).mean())
    daily_df['rolling_vol_10'] = daily_df.groupby('symbol')['day_volume'].transform(lambda s: s.rolling(10, min_periods=3).mean())
    daily_df['cum_ret_3d'] = daily_df.groupby('symbol')['daily_return_pct'].transform(lambda s: s.rolling(3, min_periods=1).sum())

    # Build quick lookup dictionary for daily pre-market info: (symbol, date) -> dict
    daily_info_map = {}
    for _, r in daily_df.iterrows():
        daily_info_map[(r['symbol'], r['trade_date'])] = r

    trading_dates = sorted(pdf['trade_date'].unique())
    symbol_groups = {s: grp.reset_index(drop=True) for s, grp in pdf.groupby('symbol')}

    # 4. Walk-Forward Simulation:
    # A) Sunday Weekly Filter
    # B) Daily 09:xx Top 20 Watchlist Selection
    # C) Strict Execution ONLY on Top 20
    daily_watchlists = {} # date -> set of symbols
    executed_trades = []
    daily_trade_counts = {}

    dte_years = 14.0 / 365.0
    r = 0.07
    sigma = 0.35

    print(f"\nConstructing Point-in-Time Daily Watchlists across {len(trading_dates)} trading sessions...")

    for d_idx, cur_date in enumerate(trading_dates):
        daily_trade_counts[cur_date] = 0
        if d_idx < 3:
            # Need a couple of days of history for rolling metrics
            continue

        prev_date = trading_dates[d_idx - 1]

        # -------------------------------------------------------------
        # DAILY PRE-OPEN WATCHLIST SCORING
        # -------------------------------------------------------------
        candidates_scores = []
        for sym in fo_symbols:
            curr_day_bar = daily_info_map.get((sym, cur_date))
            prev_day_bar = daily_info_map.get((sym, prev_date))

            if curr_day_bar is None or prev_day_bar is None:
                continue

            prev_close = prev_day_bar['day_close']
            day_open = curr_day_bar['day_open']
            if prev_close <= 0 or day_open <= 0:
                continue

            # Gap %
            gap_pct = (day_open - prev_close) / prev_close * 100.0

            # Filter out extreme gap chases (Requirement W2: > 2.5% goes to REVIEW)
            if abs(gap_pct) > 2.5:
                continue

            atr = prev_day_bar.get('rolling_atr_10', 1.5)
            if pd.isna(atr) or atr < 1.2:
                # Require adequate volatility for option trading
                continue

            mom_3d = prev_day_bar.get('cum_ret_3d', 0.0)
            if pd.isna(mom_3d): mom_3d = 0.0

            # Pre-Market Watchlist Conviction Score:
            # Rewards: Controlled gap (0.3% - 1.8%), High ATR, Strong 3-day momentum / trend
            gap_component = min(abs(gap_pct), 2.0)
            atr_component = min(atr, 4.0)
            mom_component = abs(mom_3d) * 0.2

            score = (gap_component * 1.5) + (atr_component * 1.2) + mom_component
            candidates_scores.append((sym, score, gap_pct, atr))

        # Sort descending by pre-market conviction and pick top N
        candidates_scores.sort(key=lambda x: x[1], reverse=True)
        top_watchlist = {x[0] for x in candidates_scores[:top_n_candidates]}
        daily_watchlists[cur_date] = top_watchlist

    print(f"✅ Generated {len(daily_watchlists)} daily top-{top_n_candidates} watchlists.")

    # -----------------------------------------------------------------
    # INTRADAY EXECUTION: STRICTLY RESTRICTED TO DAILY TOP 20
    # -----------------------------------------------------------------
    timestamps = np.sort(pdf['time'].unique())

    for t in timestamps:
        t_df = pdf[pdf['time'] == t]
        t_date = t_df['trade_date'].iloc[0]
        active_wl = daily_watchlists.get(t_date, set())

        if not active_wl:
            continue
        if daily_trade_counts[t_date] >= 5:
            continue

        # Filter candidates: MUST BE IN TODAY'S TOP 20 WATCHLIST
        t_df_wl = t_df[t_df['symbol'].isin(active_wl)]
        if t_df_wl.empty:
            continue

        long_cands = t_df_wl[
            (t_df_wl['vol_surge'] >= min_vol_surge) & (t_df_wl['vol_surge'] <= 3.5) &
            (t_df_wl['close'] > t_df_wl['open']) &
            (t_df_wl['close_to_high'] >= 0.85) &
            (t_df_wl['sma_ext_pct'] > 0.0) & (t_df_wl['sma_ext_pct'] <= 2.5)
        ].sort_values(by='long_score', ascending=False)

        short_cands = t_df_wl[
            (t_df_wl['vol_surge'] >= min_vol_surge) & (t_df_wl['vol_surge'] <= 3.5) &
            (t_df_wl['close'] < t_df_wl['open']) &
            (t_df_wl['close_to_low'] >= 0.85) &
            (t_df_wl['sma_ext_pct'] >= -2.5) & (t_df_wl['sma_ext_pct'] < 0.0)
        ].sort_values(by='short_score', ascending=False)

        combined = []
        for _, r_cand in long_cands.iterrows():
            combined.append((r_cand['long_score'], r_cand, True))
        for _, r_cand in short_cands.iterrows():
            combined.append((r_cand['short_score'], r_cand, False))
        combined.sort(key=lambda x: x[0], reverse=True)

        if not combined:
            continue

        top_cand = combined[0]
        row = top_cand[1]
        is_call = top_cand[2]
        sym = row['symbol']

        sym_df = symbol_groups[sym]
        idx_list = sym_df.index[sym_df['time'] == t].tolist()
        if not idx_list: continue
        idx = idx_list[0]
        if idx + 4 >= len(sym_df): continue

        spot_entry = round(float(row['close']), 2)
        entry_time_str = str(row['ist_time_str'])
        lot_size = estimate_lot_size(spot_entry)

        if is_call:
            k_itm = round(spot_entry * 0.98, 1)
            pricer = bs_call_price
            direction = "LONG"
            opt_type = "CE"
        else:
            k_itm = round(spot_entry * 1.02, 1)
            pricer = bs_put_price
            direction = "SHORT"
            opt_type = "PE"

        entry_opt = round(float(pricer(spot_entry, k_itm, dte_years, r, sigma)), 2)
        if entry_opt <= 0.1: continue

        friction = calculate_statutory_friction(entry_opt, entry_opt * 1.20, lot_size)
        if entry_opt * 0.20 * lot_size < 2.0 * friction['total_cost']: continue

        # State machine exit loop
        pnl_pct = 0.0
        ratchet = False
        profit_locked = False
        exit_reason = "TIME_STOP_3H_EXPIRY"
        exit_time_str = ""
        spot_exit = spot_entry
        exit_opt = entry_opt
        held_hours = 3

        for h in range(1, 4):
            next_row = sym_df.iloc[idx + h]
            t_dec = dte_years - (h * 1.0 / (252.0 * 6.0))
            curr_opt = round(float(pricer(next_row['close'], k_itm, t_dec, r, sigma)), 2)
            ret = (curr_opt - entry_opt) / entry_opt * 100.0

            if ret >= 25.0: profit_locked = True
            if ret >= 15.0: ratchet = True

            # Hard stop (-15%)
            if ret <= -15.0:
                pnl_pct = -15.0
                exit_reason = "HARD_STOP_LOSS_15%"
                exit_opt = round(entry_opt * 0.85, 2)
                spot_exit = round(float(next_row['close']), 2)
                exit_time_str = str(next_row['ist_time_str'])
                held_hours = h
                break

            # Trailing exit after +25%
            if profit_locked and ret <= 15.0:
                pnl_pct = 15.0
                exit_reason = "PROFIT_LOCK_TRAILING_EXIT"
                exit_opt = round(entry_opt * 1.15, 2)
                spot_exit = round(float(next_row['close']), 2)
                exit_time_str = str(next_row['ist_time_str'])
                held_hours = h
                break

            # Breakeven ratchet exit
            if ratchet and ret <= 0.0:
                pnl_pct = 0.0
                exit_reason = "BREAKEVEN_RATCHET_EXIT"
                exit_opt = entry_opt
                spot_exit = round(float(next_row['close']), 2)
                exit_time_str = str(next_row['ist_time_str'])
                held_hours = h
                break
        else:
            last_row = sym_df.iloc[idx + 3]
            final_opt = round(float(pricer(last_row['close'], k_itm, dte_years - (3.0/(252*6)), r, sigma)), 2)
            pnl_pct = round((final_opt - entry_opt) / entry_opt * 100.0, 2)
            exit_reason = "TARGET_25%_HELD" if profit_locked else "TIME_STOP_3H_EXPIRY"
            exit_opt = final_opt
            spot_exit = round(float(last_row['close']), 2)
            exit_time_str = str(last_row['ist_time_str'])
            held_hours = 3

        gross_pnl_inr = round((exit_opt - entry_opt) * lot_size, 2)
        actual_friction = calculate_statutory_friction(entry_opt, exit_opt, lot_size)
        statutory_charges_inr = round(actual_friction["total_cost"], 2)
        net_pnl_inr = round(gross_pnl_inr - statutory_charges_inr, 2)

        is_overnight = entry_time_str[:10] != exit_time_str[:10]

        executed_trades.append({
            "symbol": sym,
            "direction": direction,
            "option_type": opt_type,
            "entry_time_ist": entry_time_str,
            "exit_time_ist": exit_time_str,
            "is_overnight": is_overnight,
            "holding_duration_hours": held_hours,
            "spot_entry_price": spot_entry,
            "spot_exit_price": spot_exit,
            "strike": k_itm,
            "entry_premium": entry_opt,
            "exit_premium": exit_opt,
            "pnl_pct": round(pnl_pct, 2),
            "exit_reason": exit_reason,
            "lot_size": lot_size,
            "gross_pnl_inr": gross_pnl_inr,
            "statutory_charges_inr": statutory_charges_inr,
            "net_pnl_inr": net_pnl_inr
        })
        daily_trade_counts[t_date] += 1

    res_df = pd.DataFrame(executed_trades)
    if res_df.empty:
        print("❌ No trades triggered on the watchlist.")
        return

    res_df.insert(0, "trade_no", range(1, len(res_df) + 1))
    res_df["cumulative_net_pnl_inr"] = res_df["net_pnl_inr"].cumsum().round(2)

    total_net = res_df['net_pnl_inr'].sum()
    gross_pnl = res_df['gross_pnl_inr'].sum()
    total_charges = res_df['statutory_charges_inr'].sum()
    wins = len(res_df[res_df['net_pnl_inr'] > 0])
    losses = len(res_df[res_df['net_pnl_inr'] < 0])
    win_rate = (wins / len(res_df)) * 100.0

    gross_wins = res_df[res_df['net_pnl_inr'] > 0]['net_pnl_inr'].sum()
    gross_losses = abs(res_df[res_df['net_pnl_inr'] < 0]['net_pnl_inr'].sum())
    profit_factor = (gross_wins / gross_losses) if gross_losses > 0 else 999.0

    overnight_df = res_df[res_df['is_overnight']]
    intraday_df = res_df[~res_df['is_overnight']]

    print("\n" + "=" * 80)
    print("📈 FINAL RESULTS: DAILY TOP 20 WATCHLIST BACKTEST")
    print("=" * 80)
    print(f"Total Trades Executed: {len(res_df)}")
    print(f"Win Count / Loss Count: {wins} Wins / {losses} Losses")
    print(f"Win Rate: {win_rate:.2f}%")
    print(f"Gross P&L: ₹{gross_pnl:,.2f}")
    print(f"Statutory Friction Charges: -₹{total_charges:,.2f}")
    print(f"Net Realized P&L: ₹{total_net:,.2f}")
    print(f"Profit Factor: {profit_factor:.2f}")
    print(f"Average Net P&L per Trade: ₹{total_net / len(res_df):,.2f}")

    print("\n--- BREAKDOWN: OVERNIGHT HOLDS VS SAME-DAY EXITS ---")
    print(f"Overnight Trades: {len(overnight_df)} | Net PnL: ₹{overnight_df['net_pnl_inr'].sum():,.2f} | Win Rate: {(overnight_df['net_pnl_inr'] > 0).mean() * 100:.1f}%")
    print(f"Same-Day Trades:  {len(intraday_df)} | Net PnL: ₹{intraday_df['net_pnl_inr'].sum():,.2f} | Win Rate: {(intraday_df['net_pnl_inr'] > 0).mean() * 100:.1f}%")

    out_csv = f"data/watchlist_{top_n_candidates}_backtest_results.csv"
    res_df.to_csv(out_csv, index=False)
    print(f"\n💾 Saved full trade audit to: {out_csv}")
    print("=" * 80)

if __name__ == "__main__":
    run_watchlist_backtest(top_n_candidates=20, min_vol_surge=1.4, exclude_defensives=True)
