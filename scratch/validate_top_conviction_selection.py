import os
import csv
import math
import numpy as np
import polars as pl
from datetime import datetime
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

def run_top_conviction_backtest():
    print("=" * 90)
    print("🎯 EMPIRICAL AUDIT: ALL TRADES (1,373) VS. TOP CONVICTION SELECTION (MAX 2 PER WINDOW)")
    print("=" * 90)

    fo_symbols = set()
    with open('pipeline/fo_universe.csv', 'r') as f:
        reader = csv.DictReader(f)
        for r in reader:
            if r.get('Symbol'): fo_symbols.add(r['Symbol'])

    df = pl.read_parquet('data/intraday_ohlcv.parquet')
    df = df.filter(pl.col('symbol').is_in(list(fo_symbols))).sort(['symbol', 'time'])
    
    df = df.with_columns(
        pl.col("time").dt.convert_time_zone("Asia/Kolkata").alias("ist_time")
    )
    df = df.with_columns([
        pl.col("ist_time").dt.strftime("%H:%M").alias("ist_time_str"),
        pl.col("ist_time").dt.date().alias("trade_date")
    ])

    df = df.with_columns([
        pl.col("volume").rolling_mean(20).over("symbol").alias("vol_sma_20"),
        pl.col("close").rolling_mean(20).over("symbol").alias("sma_20"),
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

    # Rank by Breakout Conviction Score
    df = df.with_columns([
        pl.when(pl.col("close") > pl.col("open"))
          .then(pl.col("vol_surge") * pl.col("close_to_high"))
          .otherwise(0.0).alias("long_score"),
        pl.when(pl.col("close") < pl.col("open"))
          .then(pl.col("vol_surge") * pl.col("close_to_low"))
          .otherwise(0.0).alias("short_score")
    ])

    pdf = df.to_pandas()
    
    # Group by timestamp to select ONLY Top 1 or 2 per hourly interval
    timestamps = np.sort(pdf['time'].unique())

    all_trades_b = []
    top_1_trades = []
    top_2_trades = []

    dte_years = 14.0 / 365.0
    r = 0.07
    sigma = 0.35

    # Create fast symbol lookup
    symbol_groups = {s: grp.reset_index(drop=True) for s, grp in pdf.groupby('symbol')}

    for t in timestamps:
        t_df = pdf[pdf['time'] == t]
        
        # Valid Long Candidates: 1.4 <= vol_surge <= 3.2, close near high > 0.85, sma_ext <= 2.5%
        long_cands = t_df[
            (t_df['vol_surge'] >= 1.4) & (t_df['vol_surge'] <= 3.2) &
            (t_df['close'] > t_df['open']) &
            (t_df['close_to_high'] > 0.85) &
            (t_df['sma_ext_pct'] > 0.0) & (t_df['sma_ext_pct'] <= 2.5)
        ].sort_values(by='long_score', ascending=False)

        # Valid Short Candidates
        short_cands = t_df[
            (t_df['vol_surge'] >= 1.4) & (t_df['vol_surge'] <= 3.2) &
            (t_df['close'] < t_df['open']) &
            (t_df['close_to_low'] > 0.85) &
            (t_df['sma_ext_pct'] >= -2.5) & (t_df['sma_ext_pct'] < 0.0)
        ].sort_values(by='short_score', ascending=False)

        def eval_cand(row, is_call=True):
            sym = row['symbol']
            sym_df = symbol_groups[sym]
            idx_list = sym_df.index[sym_df['time'] == t].tolist()
            if not idx_list: return None
            idx = idx_list[0]
            if idx + 4 >= len(sym_df): return None

            spot_entry = row['close']
            if is_call:
                k_itm = round(spot_entry * 0.98, 1)
                entry_opt = bs_call_price(spot_entry, k_itm, dte_years, r, sigma)
                pricer = bs_call_price
            else:
                k_itm = round(spot_entry * 1.02, 1)
                entry_opt = bs_put_price(spot_entry, k_itm, dte_years, r, sigma)
                pricer = bs_put_price

            if entry_opt <= 0.1: return None

            pnl = 0.0
            ratchet = False
            profit_locked = False
            for h in range(1, 4):
                next_row = sym_df.iloc[idx + h]
                t_dec = dte_years - (h * 1.0 / (252.0 * 6.0))
                curr_opt = pricer(next_row['close'], k_itm, t_dec, r, sigma)
                ret = (curr_opt - entry_opt) / entry_opt * 100.0
                if ret >= 25.0: profit_locked = True
                if ret >= 15.0: ratchet = True
                if ret <= -15.0:
                    pnl = -15.0
                    break
                if profit_locked and ret <= 15.0:
                    pnl = 15.0
                    break
                if ratchet and ret <= 0.0:
                    pnl = 0.0
                    break
            else:
                last_row = sym_df.iloc[idx + 3]
                final_opt = pricer(last_row['close'], k_itm, dte_years - (3.0/(252*6)), r, sigma)
                pnl = (final_opt - entry_opt) / entry_opt * 100.0
            return pnl

        # 1. Evaluate All candidates (Model B)
        for _, r_cand in long_cands.iterrows():
            res = eval_cand(r_cand, is_call=True)
            if res is not None: all_trades_b.append(res)
        for _, r_cand in short_cands.iterrows():
            res = eval_cand(r_cand, is_call=False)
            if res is not None: all_trades_b.append(res)

        # 2. Evaluate Top 1 & Top 2 Candidates Only
        # Combine and take highest conviction score
        all_cands_ranked = []
        for _, r_cand in long_cands.iterrows():
            all_cands_ranked.append((r_cand['long_score'], r_cand, True))
        for _, r_cand in short_cands.iterrows():
            all_cands_ranked.append((r_cand['short_score'], r_cand, False))
        all_cands_ranked.sort(key=lambda x: x[0], reverse=True)

        if len(all_cands_ranked) >= 1:
            res1 = eval_cand(all_cands_ranked[0][1], is_call=all_cands_ranked[0][2])
            if res1 is not None:
                top_1_trades.append(res1)
                top_2_trades.append(res1)

        if len(all_cands_ranked) >= 2:
            res2 = eval_cand(all_cands_ranked[1][1], is_call=all_cands_ranked[1][2])
            if res2 is not None:
                top_2_trades.append(res2)

    def print_metrics(name, data):
        wins = [x for x in data if x > 0.1]
        losses = [x for x in data if x < -0.1]
        be = [x for x in data if abs(x) <= 0.1]
        wr = len(wins) / len(data) * 100.0 if data else 0.0
        avg_w = np.mean(wins) if wins else 0.0
        avg_l = np.mean(losses) if losses else 0.0
        pf = (sum(wins) / abs(sum(losses))) if losses and sum(losses) != 0 else 0.0
        exp = np.mean(data) if data else 0.0
        print(f"\n--- {name} ---")
        print(f"Total Trades Evaluated : {len(data)}")
        print(f"Win Rate               : {wr:.1f}% ({len(wins)} W, {len(losses)} L, {len(be)} Breakeven)")
        print(f"Average Winner         : +{avg_w:.2f}%")
        print(f"Average Loser          : {avg_l:.2f}%")
        print(f"Win/Loss Payoff Ratio  : {(abs(avg_w/avg_l) if avg_l != 0 else 0.0):.2f}x")
        print(f"Profit Factor          : {pf:.2f}")
        print(f"Mathematical Expectancy: {exp:+.2f}% per trade")

    print_metrics("ALL TRADES UNFILTERED (Model B: 20 Trades/Day Noise)", all_trades_b)
    print_metrics("TOP 1 HIGHEST CONVICTION PER WINDOW (Selective Sniper)", top_1_trades)
    print_metrics("TOP 2 HIGHEST CONVICTION PER WINDOW (Max 2 Concurrency)", top_2_trades)

if __name__ == '__main__':
    run_top_conviction_backtest()
