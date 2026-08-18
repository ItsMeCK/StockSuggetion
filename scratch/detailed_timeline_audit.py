"""
scratch/detailed_timeline_audit.py
Detailed comparison of trigger timing, price levels, and trade quality for winners and losers under:
1. Baseline: 1h vol_surge > 2.5
2. Cumulative 3H Volume: 3h vol_surge >= 1.5 (with 1h >= 1.1)
3. VWAP Trend + Cumulative Volume: close > VWAP, VWAP slope >= 0, 3h vol_surge >= 1.4
4. Dual-Track Hybrid Engine: (1h > 2.5) OR (3h >= 1.5 & 1h >= 1.1 & close > VWAP)
"""

import polars as pl
import pandas as pd

df = pl.read_parquet("data/intraday_ohlcv.parquet")
df = df.sort(["symbol", "time"])

# Base indicators
df = df.with_columns([
    pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
    pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
    pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
    pl.col("volume").rolling_sum(window_size=3).over("symbol").alias("vol_sum_3h"),
    pl.col("volume").rolling_sum(window_size=2).over("symbol").alias("vol_sum_2h"),
])

df = df.with_columns([
    ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
    (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge_1h"),
    (pl.col("vol_sum_3h") / (3.0 * pl.col("vol_avg_20"))).alias("vol_surge_3h"),
    (pl.col("vol_sum_2h") / (2.0 * pl.col("vol_avg_20"))).alias("vol_surge_2h"),
    (((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20")) * 100).alias("sma20_dist_pct"),
    (((pl.col("close") - pl.col("open")) / pl.col("open")) * 100).alias("candle_pct"),
])

# VWAP
df = df.with_columns([
    pl.col("time").dt.date().alias("date"),
    ((pl.col("high") + pl.col("low") + pl.col("close")) / 3.0).alias("typical_price"),
])
df = df.with_columns([
    (pl.col("typical_price") * pl.col("volume")).alias("pv")
])
df = df.with_columns([
    pl.col("pv").cum_sum().over(["symbol", "date"]).alias("cum_pv"),
    pl.col("volume").cum_sum().over(["symbol", "date"]).alias("cum_vol"),
])
df = df.with_columns([
    (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap")
])
df = df.with_columns([
    (((pl.col("close") - pl.col("vwap")) / pl.col("vwap")) * 100).alias("vwap_dist_pct"),
    pl.col("vwap").shift(1).over(["symbol", "date"]).alias("vwap_prev"),
])
df = df.with_columns([
    (((pl.col("vwap") - pl.col("vwap_prev")) / pl.col("vwap_prev")) * 100).alias("vwap_slope_pct"),
])

# Forward Returns within next 14 hourly bars (2 days)
df = df.with_columns([
    pl.col("high").rolling_max(window_size=14).shift(-14).over("symbol").alias("fwd_max_high_2d"),
    pl.col("low").rolling_min(window_size=14).shift(-14).over("symbol").alias("fwd_min_low_2d"),
    pl.col("close").shift(-14).over("symbol").alias("fwd_close_2d"),
])

df = df.with_columns([
    (((pl.col("fwd_max_high_2d") - pl.col("close")) / pl.col("close")) * 100).alias("max_gain_2d"),
    (((pl.col("fwd_min_low_2d") - pl.col("close")) / pl.col("close")) * 100).alias("max_dd_2d"),
    (((pl.col("fwd_close_2d") - pl.col("close")) / pl.col("close")) * 100).alias("net_gain_2d"),
])

aug_df = df.filter(
    (pl.col("time") >= pl.datetime(2026, 8, 3, 0, 0, 0, time_zone="UTC")) &
    (pl.col("time") <= pl.datetime(2026, 8, 14, 23, 59, 59, time_zone="UTC"))
)

symbols_to_study = [
    ("ZYDUSLIFE", "WINNER (+8.7%)"),
    ("OBEROIRLTY", "WINNER (+5.0%)"),
    ("CIPLA", "WINNER (+10.8%)"),
    ("DRREDDY", "WINNER (+5.3%)"),
    ("ADANIENT", "WINNER (+12.9%)"),
    ("SAREGAMA", "WINNER (+24.9%)"),
    ("BERGEPAINT", "WINNER (+9.3%)"),
    ("SOLARINDS", "WINNER (+11.1%)"),
    ("CAPLIPOINT", "WINNER (+10.1%)"),
    ("TRITURBINE", "WINNER (+8.2%)"),
    ("PRESTIGE", "LOSER"),
    ("NATIONALUM", "LOSER"),
    ("PATANJALI", "LOSER"),
    ("TRENT", "LOSER"),
    ("TATAPOWER", "LOSER"),
]

print("="*120)
print(f"{'DEEP DIVE: TRIGGER TIMING, ENTRY PRICE & 2-DAY GAIN ACROSS CANDIDATE RULES':^120}")
print("="*120)

for sym, label in symbols_to_study:
    sym_df = aug_df.filter(pl.col("symbol") == sym).sort("time")
    if len(sym_df) == 0:
        print(f"\n{sym}: No data found.")
        continue
        
    print(f"\n" + "-"*110)
    print(f"SYMBOL: {sym:<12} | Category: {label}")
    print("-"*110)
    
    # 1. Baseline: 1h Vol > 2.5
    base_sig = sym_df.filter(
        (pl.col("bbw") < 0.25) &
        (pl.col("vol_surge_1h") > 2.5) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") < pl.col("sma_20") * 1.03) &
        (pl.col("close") > pl.col("open"))
    )
    
    # 2. Cumulative 3H Volume: 3h >= 1.5 & 1h >= 1.1
    cum3_sig = sym_df.filter(
        (pl.col("bbw") < 0.25) &
        (pl.col("vol_surge_3h") >= 1.5) &
        (pl.col("vol_surge_1h") >= 1.1) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") < pl.col("sma_20") * 1.03) &
        (pl.col("close") > pl.col("open"))
    )
    
    # 3. VWAP Trend + Cumulative Volume: close > VWAP & 3h >= 1.4 & 1h >= 1.1
    vwap_sig = sym_df.filter(
        (pl.col("bbw") < 0.25) &
        (pl.col("close") > pl.col("vwap")) &
        (pl.col("vol_surge_3h") >= 1.4) &
        (pl.col("vol_surge_1h") >= 1.1) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") < pl.col("sma_20") * 1.03) &
        (pl.col("close") > pl.col("open"))
    )
    
    # 4. Cumulative 2H Volume: 2h >= 1.4 & 1h >= 1.1 & close > VWAP
    cum2_sig = sym_df.filter(
        (pl.col("bbw") < 0.25) &
        (pl.col("vol_surge_2h") >= 1.4) &
        (pl.col("vol_surge_1h") >= 1.1) &
        (pl.col("close") > pl.col("vwap")) &
        (pl.col("close") > pl.col("sma_20")) &
        (pl.col("close") < pl.col("sma_20") * 1.03) &
        (pl.col("close") > pl.col("open"))
    )

    models_dict = [
        ("Baseline (1h Vol > 2.5)", base_sig),
        ("Cumul 3H (3h>=1.5, 1h>=1.1)", cum3_sig),
        ("VWAP Trend (Close>VWAP, 3h>=1.4)", vwap_sig),
        ("Cumul 2H + VWAP (2h>=1.4, 1h>=1.1)", cum2_sig),
    ]
    
    for m_name, df_sig in models_dict:
        if len(df_sig) == 0:
            print(f"   [{m_name:<34}] -> NO SIGNAL TRIGGERED")
        else:
            first = df_sig.head(1).to_dicts()[0]
            t = str(first["time"])
            px = first["close"]
            v1 = first["vol_surge_1h"]
            v2 = first["vol_surge_2h"]
            v3 = first["vol_surge_3h"]
            vw_d = first["vwap_dist_pct"]
            mg = first["max_gain_2d"]
            mdd = first["max_dd_2d"]
            mg_str = f"{mg:>+5.2f}%" if mg is not None else "N/A"
            mdd_str = f"{mdd:>+5.2f}%" if mdd is not None else "N/A"
            print(f"   [{m_name:<34}] -> {t} | Px: ₹{px:>8.2f} | 1h: {v1:>4.2f}x | 2h: {v2:>4.2f}x | 3h: {v3:>4.2f}x | VWAP: {vw_d:>+5.2f}% | MaxGain: {mg_str} | MaxDD: {mdd_str} (Triggers: {len(df_sig)})")
