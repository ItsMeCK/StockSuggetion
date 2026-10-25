import os
import csv
import math
import numpy as np
import polars as pl
from datetime import datetime, timedelta
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

def run_comparative_backtest():
    print("=" * 80)
    print("📊 RUNNING SCIENTIFIC BACKTEST: BASELINE (BEFORE) VS. QUANT-HARDENED (AFTER)")
    print("Dataset: data/intraday_ohlcv.parquet (July 16, 2026 - October 23, 2026)")
    print("=" * 80)

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
    print(f"Filtered to {len(df):,} hourly candles across F&O stocks.")

    # Compute Indicators
    df = df.with_columns([
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_sma_20"),
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").shift(1).over("symbol").alias("prev_close"),
        pl.col("low").shift(1).over("symbol").alias("prev_low"),
        pl.col("high").shift(1).over("symbol").alias("prev_high"),
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

    # Convert to pandas for fast row iteration per symbol
    pdf = df.to_pandas()
    symbols = pdf['symbol'].unique()

    # Metrics Tracking
    results_baseline = []
    results_quant = []

    # Assume 14 DTE average for options
    dte_years = 14.0 / 365.0
    r = 0.07
    sigma = 0.35 # 35% typical single stock IV

    for sym in symbols:
        sym_df = pdf[pdf['symbol'] == sym].reset_index(drop=True)
        n = len(sym_df)
        if n < 30: continue

        for i in range(20, n - 4):
            row = sym_df.iloc[i]
            spot_entry = row['close']
            vol_surge = row['vol_surge']
            close_to_high = row['close_to_high']
            close_to_low = row['close_to_low']
            sma_ext = row['sma_ext_pct']

            # =========================================================
            # MODEL A: BASELINE (BEFORE - Legacy 1% OTM, Long-Only)
            # =========================================================
            # Criteria: vol_surge > 1.2, green candle, close near HOD (>0.85), close > sma_20
            if (vol_surge > 1.2 and row['close'] > row['open'] and 
                close_to_high > 0.85 and row['close'] > row['sma_20']):
                
                # 1% OTM Call: Strike = Spot * 1.01 (Delta ~0.35)
                k_otm = round(spot_entry * 1.01, 1)
                entry_opt_price = bs_call_price(spot_entry, k_otm, dte_years, r, sigma)
                
                if entry_opt_price > 0.1:
                    # Simulate next 4 hours or until exit
                    # Legacy exit: 50% SL, trailing at +50%, or EOD cut at 0%
                    pnl_pct = 0.0
                    peak_opt = entry_opt_price
                    exit_reason = "EOD_SQUASH"
                    
                    for h in range(1, 4):
                        next_row = sym_df.iloc[i + h]
                        t_decay = dte_years - (h * 1.0 / (252.0 * 6.0))
                        curr_opt = bs_call_price(next_row['close'], k_otm, t_decay, r, sigma)
                        opt_return = (curr_opt - entry_opt_price) / entry_opt_price * 100.0
                        peak_opt = max(peak_opt, curr_opt)
                        
                        # Legacy hard stop: -50%
                        if opt_return <= -50.0:
                            pnl_pct = -50.0
                            exit_reason = "HARD_STOP_50"
                            break
                        # Legacy trailing: if peak reached +50%, trail to +25%
                        if (peak_opt - entry_opt_price) / entry_opt_price >= 50.0:
                            if opt_return <= 25.0:
                                pnl_pct = 25.0
                                exit_reason = "TRAILING_STOP"
                                break
                    else:
                        # Stagnated / EOD squash
                        last_row = sym_df.iloc[i + 3]
                        t_decay = dte_years - (3.0 / (252.0 * 6.0))
                        final_opt = bs_call_price(last_row['close'], k_otm, t_decay, r, sigma)
                        pnl_pct = (final_opt - entry_opt_price) / entry_opt_price * 100.0
                        exit_reason = "TIME_EXIT"

                    results_baseline.append({
                        'symbol': sym,
                        'type': 'CE',
                        'strike_type': '1% OTM',
                        'pnl_pct': pnl_pct,
                        'exit_reason': exit_reason
                    })

            # =========================================================
            # MODEL B: QUANT-HARDENED (AFTER - Delta 0.65 ITM, Bi-Directional, Anti-FOMO)
            # =========================================================
            # Filter 1: Anti-FOMO (vol_surge between 1.4 and 3.2 - reject exhaustion >3.5x)
            # Filter 2: Extension cap: sma_ext <= 2.5% (reject extended tops)
            
            # --- LONG SETUP (CE) ---
            if (1.4 <= vol_surge <= 3.2 and row['close'] > row['open'] and 
                close_to_high > 0.85 and 0.0 < sma_ext <= 2.5):
                
                # In-The-Money Call: Strike = Spot * 0.98 (Delta ~0.65 - 0.70)
                k_itm = round(spot_entry * 0.98, 1)
                entry_opt_price = bs_call_price(spot_entry, k_itm, dte_years, r, sigma)
                
                if entry_opt_price > 0.1:
                    # Dynamic Exit Engine:
                    # Hard SL: -15%
                    # Breakeven Ratchet: +15% hit -> SL moves to cost (0%)
                    # Profit Lock: +25% hit -> lock +15%
                    pnl_pct = 0.0
                    ratchet_active = False
                    profit_locked = False
                    exit_reason = "TIME_STOP"
                    
                    for h in range(1, 4):
                        next_row = sym_df.iloc[i + h]
                        t_decay = dte_years - (h * 1.0 / (252.0 * 6.0))
                        curr_opt = bs_call_price(next_row['close'], k_itm, t_decay, r, sigma)
                        opt_return = (curr_opt - entry_opt_price) / entry_opt_price * 100.0
                        
                        # 1. Profit Lock check
                        if opt_return >= 25.0:
                            profit_locked = True
                        # 2. Breakeven check
                        if opt_return >= 15.0:
                            ratchet_active = True
                            
                        # Trailing stops
                        if profit_locked and opt_return <= 15.0:
                            pnl_pct = 15.0
                            exit_reason = "PROFIT_LOCK_HIT"
                            break
                        if ratchet_active and opt_return <= 0.0:
                            pnl_pct = 0.0
                            exit_reason = "BREAKEVEN_STOP"
                            break
                        if opt_return <= -15.0:
                            pnl_pct = -15.0
                            exit_reason = "HARD_STOP_15"
                            break
                    else:
                        last_row = sym_df.iloc[i + 3]
                        t_decay = dte_years - (3.0 / (252.0 * 6.0))
                        final_opt = bs_call_price(last_row['close'], k_itm, t_decay, r, sigma)
                        pnl_pct = (final_opt - entry_opt_price) / entry_opt_price * 100.0
                        if ratchet_active: pnl_pct = max(0.0, pnl_pct)
                        if profit_locked: pnl_pct = max(15.0, pnl_pct)
                        pnl_pct = max(-15.0, pnl_pct)

                    results_quant.append({
                        'symbol': sym,
                        'type': 'CE',
                        'strike_type': 'ITM (Delta 0.65)',
                        'pnl_pct': pnl_pct,
                        'exit_reason': exit_reason
                    })

            # --- SHORT SETUP (PE) ---
            # Criteria: Red candle, closing in bottom 15% (close_to_low > 0.85), volume surge 1.4-3.2, below 20 SMA
            if (1.4 <= vol_surge <= 3.2 and row['close'] < row['open'] and 
                close_to_low > 0.85 and -2.5 <= sma_ext < 0.0):
                
                # In-The-Money Put: Strike = Spot * 1.02 (Put Delta ~ -0.65)
                k_itm_put = round(spot_entry * 1.02, 1)
                entry_opt_price = bs_put_price(spot_entry, k_itm_put, dte_years, r, sigma)
                
                if entry_opt_price > 0.1:
                    pnl_pct = 0.0
                    ratchet_active = False
                    profit_locked = False
                    exit_reason = "TIME_STOP"
                    
                    for h in range(1, 4):
                        next_row = sym_df.iloc[i + h]
                        t_decay = dte_years - (h * 1.0 / (252.0 * 6.0))
                        curr_opt = bs_put_price(next_row['close'], k_itm_put, t_decay, r, sigma)
                        opt_return = (curr_opt - entry_opt_price) / entry_opt_price * 100.0
                        
                        if opt_return >= 25.0: profit_locked = True
                        if opt_return >= 15.0: ratchet_active = True
                            
                        if profit_locked and opt_return <= 15.0:
                            pnl_pct = 15.0
                            exit_reason = "PROFIT_LOCK_HIT"
                            break
                        if ratchet_active and opt_return <= 0.0:
                            pnl_pct = 0.0
                            exit_reason = "BREAKEVEN_STOP"
                            break
                        if opt_return <= -15.0:
                            pnl_pct = -15.0
                            exit_reason = "HARD_STOP_15"
                            break
                    else:
                        last_row = sym_df.iloc[i + 3]
                        t_decay = dte_years - (3.0 / (252.0 * 6.0))
                        final_opt = bs_put_price(last_row['close'], k_itm_put, t_decay, r, sigma)
                        pnl_pct = (final_opt - entry_opt_price) / entry_opt_price * 100.0
                        if ratchet_active: pnl_pct = max(0.0, pnl_pct)
                        if profit_locked: pnl_pct = max(15.0, pnl_pct)
                        pnl_pct = max(-15.0, pnl_pct)

                    results_quant.append({
                        'symbol': sym,
                        'type': 'PE',
                        'strike_type': 'ITM Put (Delta -0.65)',
                        'pnl_pct': pnl_pct,
                        'exit_reason': exit_reason
                    })

    # Summary Statistics Function
    def summarize(trades, name):
        if not trades:
            print(f"No trades for {name}")
            return
        pnls = [t['pnl_pct'] for t in trades]
        wins = [p for p in pnls if p > 0.01]
        losses = [p for p in pnls if p < -0.01]
        breakevens = [p for p in pnls if abs(p) <= 0.01]
        
        win_rate = len(wins) / len(trades) * 100.0
        avg_win = np.mean(wins) if wins else 0.0
        avg_loss = np.mean(losses) if losses else 0.0
        profit_factor = (sum(wins) / abs(sum(losses))) if sum(losses) != 0 else 999.0
        expectancy = np.mean(pnls)
        
        print(f"\n{'='*30} {name} {'='*30}")
        print(f"Total Trades Evaluated : {len(trades):,}")
        print(f"Win Rate               : {win_rate:.1f}% ({len(wins)} Wins, {len(losses)} Losses, {len(breakevens)} Breakeven)")
        print(f"Average Winner         : +{avg_win:.2f}%")
        print(f"Average Loser          : {avg_loss:.2f}%")
        print(f"Win/Loss Payoff Ratio  : {abs(avg_win / avg_loss) if avg_loss != 0 else 0:.2f}x")
        print(f"Profit Factor          : {profit_factor:.2f}")
        print(f"Mathematical Expectancy: {expectancy:+.2f}% per trade")
        
        # Breakdown by trade type
        ce_trades = [t for t in trades if t.get('type') == 'CE']
        pe_trades = [t for t in trades if t.get('type') == 'PE']
        if ce_trades:
            print(f"  -> Calls (CE) Win Rate: {len([t for t in ce_trades if t['pnl_pct'] > 0]) / len(ce_trades) * 100:.1f}% ({len(ce_trades)} trades, avg PnL: {np.mean([t['pnl_pct'] for t in ce_trades]):+.2f}%)")
        if pe_trades:
            print(f"  -> Puts  (PE) Win Rate: {len([t for t in pe_trades if t['pnl_pct'] > 0]) / len(pe_trades) * 100:.1f}% ({len(pe_trades)} trades, avg PnL: {np.mean([t['pnl_pct'] for t in pe_trades]):+.2f}%)")

    summarize(results_baseline, "MODEL A: BASELINE (BEFORE - OTM 1%, Long-Only, No Anti-FOMO)")
    summarize(results_quant, "MODEL B: QUANT-HARDENED (AFTER - ITM Delta 0.65, Bi-Directional, Anti-FOMO & Ratchet)")

if __name__ == '__main__':
    run_comparative_backtest()
