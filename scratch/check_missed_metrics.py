import psycopg2
import os
import polars as pl
from pipeline.screener import SovereignScreener

def check_missed():
    screener = SovereignScreener()
    df = screener.fetch_market_data()
    df = screener.apply_stage_2_filter(df)
    df = screener.apply_avwap_filter(df)
    
    # Filter for May 21, 2026 (DB representation of Friday before Monday's rally)
    target_dt = "2026-05-21"
    df_day = df.filter(pl.col("time").dt.date() == pl.date(2026, 5, 21))
    
    missed_symbols = ["TITAGARH", "EMMVEE", "HFCL", "ARE&M", "JWL", "ADANIPOWER"]
    
    print(f"--- TECHNICAL METRICS FOR MISSED STOCKS ON {target_dt} ---")
    for sym in missed_symbols:
        sym_row = df_day.filter(pl.col("symbol") == sym)
        if sym_row.is_empty():
            print(f"{sym}: No data on {target_dt}")
            continue
        row = sym_row.to_dicts()[0]
        
        # Calculate conditions
        below_50 = row["close"] <= row["sma_50"]
        sma_50_below_200 = row["sma_50"] <= row["sma_200"] if row["sma_200"] else False
        neg_slope = row["sma_50_slope_10d"] <= 0 if row["sma_50_slope_10d"] else False
        over_ext = row["extension_pct"] > 12.0
        vol_thrust_fail = row["volume"] < (1.5 * row["vol_avg_20"]) if row["vol_avg_20"] else False
        atr_squeeze_fail = row["atr_3"] > row["atr_20"] if (row["atr_3"] and row["atr_20"]) else False
        
        print(f"\nSymbol: {sym}")
        sma_200_val = row['sma_200'] if row['sma_200'] is not None else 0.0
        print(f"  Close: {row['close']:.2f} | SMA50: {row['sma_50']:.2f} | SMA200: {sma_200_val:.2f}")
        print(f"  Extension %: {row['extension_pct']:.2f}% (Limit: 12% or 15%)")
        vol_ratio = row['volume'] / row['vol_avg_20'] if row['vol_avg_20'] else 0.0
        atr_ratio = row['atr_3'] / row['atr_20'] if row['atr_20'] else 0.0
        slope_val = row['sma_50_slope_10d'] if row['sma_50_slope_10d'] is not None else 0.0
        print(f"  Volume: {row['volume']:.0f} | Avg 20: {row['vol_avg_20']:.0f} | Ratio: {vol_ratio:.2f}")
        print(f"  ATR3: {row['atr_3']:.2f} | ATR20: {row['atr_20']:.2f} | Ratio: {atr_ratio:.2f}")
        print(f"  Turnover: {row['turnover']/10000000.0:.2f} Cr")
        print(f"  ROC10: {row['roc_10']:.2f}% | ROC20: {row['roc_20']:.2f}%")
        print(f"  SMA50 Slope 10d: {slope_val:.2f}")
        print(f"  Failed Checks: ")
        if below_50: print("    - Below 50 SMA")
        if sma_50_below_200: print("    - 50 SMA below 200 SMA")
        if neg_slope: print("    - Negative SMA50 Slope")
        if over_ext: print("    - Over-extended")
        if vol_thrust_fail: print("    - Volume Thrust Fail")
        if atr_squeeze_fail: print("    - ATR Squeeze Fail")

if __name__ == "__main__":
    check_missed()
