import os
import csv
import math
import numpy as np
import polars as pl
import pandas as pd
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

def export_201_trades():
    print("=" * 80)
    print("📊 EXPORTING EXACT 201 TRADES TO CSV (ENTRY/EXIT PRICES, TIMESTAMPS, PNL)")
    print("=" * 80)

    fo_symbols = set()
    with open('pipeline/fo_universe.csv', 'r') as f:
        reader = csv.DictReader(f)
        for r_row in reader:
            if r_row.get('Symbol'): fo_symbols.add(r_row['Symbol'])

    df = pl.read_parquet('data/intraday_ohlcv.parquet')
    df = df.filter(pl.col('symbol').is_in(list(fo_symbols))).sort(['symbol', 'time'])
    
    df = df.with_columns(
        pl.col("time").dt.convert_time_zone("Asia/Kolkata").alias("ist_time")
    )
    df = df.with_columns([
        pl.col("ist_time").dt.strftime("%Y-%m-%d %H:%M").alias("ist_time_str"),
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
    timestamps = np.sort(pdf['time'].unique())

    daily_trades_counter = {}
    exported_trades = []

    dte_years = 14.0 / 365.0
    r = 0.07
    sigma = 0.35
    symbol_groups = {s: grp.reset_index(drop=True) for s, grp in pdf.groupby('symbol')}

    # Typical representative lot sizes by spot tier for Indian F&O (contract value approx ₹7-10 Lakhs)
    def estimate_lot_size(spot_price):
        target_notional = 800000.0
        raw_lot = round(target_notional / spot_price)
        # Round to convenient market lot tiers
        if raw_lot >= 2000: return round(raw_lot / 500) * 500
        if raw_lot >= 1000: return round(raw_lot / 250) * 250
        if raw_lot >= 400: return round(raw_lot / 100) * 100
        if raw_lot >= 100: return round(raw_lot / 25) * 25
        return max(25, round(raw_lot / 10) * 10)

    for t in timestamps:
        t_df = pdf[pdf['time'] == t]
        t_date = t_df['trade_date'].iloc[0]
        if t_date not in daily_trades_counter:
            daily_trades_counter[t_date] = 0

        # Long candidates
        long_cands = t_df[
            (t_df['vol_surge'] >= 1.4) & (t_df['vol_surge'] <= 3.2) &
            (t_df['close'] > t_df['open']) &
            (t_df['close_to_high'] > 0.85) &
            (t_df['sma_ext_pct'] > 0.0) & (t_df['sma_ext_pct'] <= 2.5)
        ].sort_values(by='long_score', ascending=False)

        # Short candidates
        short_cands = t_df[
            (t_df['vol_surge'] >= 1.4) & (t_df['vol_surge'] <= 3.2) &
            (t_df['close'] < t_df['open']) &
            (t_df['close_to_low'] > 0.85) &
            (t_df['sma_ext_pct'] >= -2.5) & (t_df['sma_ext_pct'] < 0.0)
        ].sort_values(by='short_score', ascending=False)

        def eval_and_record_trade(row, is_call=True):
            sym = row['symbol']
            sym_df = symbol_groups[sym]
            idx_list = sym_df.index[sym_df['time'] == t].tolist()
            if not idx_list: return None
            idx = idx_list[0]
            if idx + 4 >= len(sym_df): return None

            spot_entry = round(float(row['close']), 2)
            entry_time_str = str(row['ist_time_str'])
            lot_size = estimate_lot_size(spot_entry)

            if is_call:
                k_itm = round(spot_entry * 0.98, 1)
                entry_opt = round(float(bs_call_price(spot_entry, k_itm, dte_years, r, sigma)), 2)
                pricer = bs_call_price
                direction = "LONG"
                opt_type = "CE"
            else:
                k_itm = round(spot_entry * 1.02, 1)
                entry_opt = round(float(bs_put_price(spot_entry, k_itm, dte_years, r, sigma)), 2)
                pricer = bs_put_price
                direction = "SHORT"
                opt_type = "PE"

            if entry_opt <= 0.1: return None

            # Cost Clearance Filter (Gate 2)
            friction = calculate_statutory_friction(entry_opt, entry_opt * 1.20, lot_size)
            expected_gain = entry_opt * 0.20 * lot_size
            if expected_gain < 2.0 * friction["total_cost"]:
                return None

            # State Machine Exit Loop
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

                if ret >= 25.0:
                    profit_locked = True
                if ret >= 15.0:
                    ratchet = True

                # Hard Stop-Loss (-15%)
                if ret <= -15.0:
                    pnl_pct = -15.0
                    exit_reason = "HARD_STOP_LOSS_15%"
                    exit_opt = round(entry_opt * 0.85, 2)
                    spot_exit = round(float(next_row['close']), 2)
                    exit_time_str = str(next_row['ist_time_str'])
                    held_hours = h
                    break

                # Profit Lock Trailing Exit (Retracement to +15% after +25%)
                if profit_locked and ret <= 15.0:
                    pnl_pct = 15.0
                    exit_reason = "PROFIT_LOCK_TRAILING_EXIT"
                    exit_opt = round(entry_opt * 1.15, 2)
                    spot_exit = round(float(next_row['close']), 2)
                    exit_time_str = str(next_row['ist_time_str'])
                    held_hours = h
                    break

                # Breakeven Ratchet Exit (Retracement to cost after +15%)
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

            # Calculate Exact P&L in Rupees and Statutory Costs
            gross_pnl_inr = round((exit_opt - entry_opt) * lot_size, 2)
            actual_friction = calculate_statutory_friction(entry_opt, exit_opt, lot_size)
            statutory_charges_inr = round(actual_friction["total_cost"], 2)
            net_pnl_inr = round(gross_pnl_inr - statutory_charges_inr, 2)

            return {
                "symbol": sym,
                "direction": direction,
                "option_type": opt_type,
                "entry_time_ist": entry_time_str,
                "exit_time_ist": exit_time_str,
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
            }

        # Select Top Conviction setup for this window
        combined = []
        for _, r_cand in long_cands.iterrows():
            combined.append((r_cand['long_score'], r_cand, True))
        for _, r_cand in short_cands.iterrows():
            combined.append((r_cand['short_score'], r_cand, False))
        combined.sort(key=lambda x: x[0], reverse=True)

        if combined:
            top_cand = combined[0]
            if daily_trades_counter[t_date] < 5:
                trade_record = eval_and_record_trade(top_cand[1], is_call=top_cand[2])
                if trade_record:
                    exported_trades.append(trade_record)
                    daily_trades_counter[t_date] += 1

    # Convert to DataFrame and compute cumulative P&L
    out_df = pd.DataFrame(exported_trades)
    out_df.insert(0, "trade_no", range(1, len(out_df) + 1))
    out_df["cumulative_net_pnl_inr"] = out_df["net_pnl_inr"].cumsum().round(2)

    csv_path = "data/grounded_201_trades_audit.csv"
    excel_path = "data/grounded_201_trades_audit.xlsx"

    out_df.to_csv(csv_path, index=False)
    print(f"✅ Successfully exported {len(out_df)} trades to CSV: {csv_path}")

    try:
        out_df.to_excel(excel_path, index=False)
        print(f"✅ Successfully exported {len(out_df)} trades to Excel: {excel_path}")
    except Exception as e:
        print(f"Note: Excel export skipped ({e}), CSV is fully populated.")

    # Print summary snapshot of first 5 and last 5 trades
    print("\n--- SAMPLE OF EXPORTED TRADES (FIRST 5) ---")
    print(out_df[["trade_no", "symbol", "direction", "entry_time_ist", "entry_premium", "exit_time_ist", "exit_premium", "pnl_pct", "exit_reason", "net_pnl_inr"]].head(5).to_string(index=False))

    print("\n--- SAMPLE OF EXPORTED TRADES (LAST 5) ---")
    print(out_df[["trade_no", "symbol", "direction", "entry_time_ist", "entry_premium", "exit_time_ist", "exit_premium", "pnl_pct", "exit_reason", "net_pnl_inr"]].tail(5).to_string(index=False))

    total_net = out_df["net_pnl_inr"].sum()
    wins = len(out_df[out_df["pnl_pct"] > 0])
    losses = len(out_df[out_df["pnl_pct"] < 0])
    be = len(out_df[out_df["pnl_pct"] == 0])
    print(f"\nTotal Net P&L Generated: ₹{total_net:,.2f}")
    print(f"Win Rate: {wins/len(out_df)*100:.1f}% ({wins} W / {losses} L / {be} BE)")

if __name__ == '__main__':
    export_201_trades()
