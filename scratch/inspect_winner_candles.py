"""
scratch/inspect_winner_candles.py
Print every candle of key winners and losers to see their volume profile, VWAP, and BBW progression.
"""
import polars as pl

df = pl.read_parquet("data/intraday_ohlcv.parquet")
df = df.sort(["symbol", "time"])

# Rolling indicators
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

symbols = ["ZYDUSLIFE", "OBEROIRLTY", "CIPLA", "DRREDDY", "ADANIENT", "SAREGAMA", "PRESTIGE", "NATIONALUM", "PATANJALI", "TRENT", "TATAPOWER"]

for sym in symbols:
    print("\n" + "="*100)
    print(f"SYMBOL: {sym}")
    print("="*100)
    sub = df.filter(
        (pl.col("symbol") == sym) &
        (pl.col("time") >= pl.datetime(2026, 8, 10, 0, 0, 0, time_zone="UTC"))
    )
    for r in sub.iter_rows(named=True):
        t_utc = str(r['time'])
        c = r['close']
        o = r['open']
        h = r['high']
        l = r['low']
        v1 = r['vol_surge_1h']
        v2 = r['vol_surge_2h']
        v3 = r['vol_surge_3h']
        bbw = r['bbw']
        vwap = r['vwap']
        vwap_d = r['vwap_dist_pct']
        sma_d = r['sma20_dist_pct']
        c_pct = r['candle_pct']
        
        # Check conditions
        baseline_pass = (bbw < 0.25) and (v1 > 2.5) and (c > r['sma_20']) and (c < r['sma_20'] * 1.03) and (c > o)
        cum3h_pass = (bbw < 0.25) and (v3 >= 1.5) and (v1 >= 1.1) and (c > r['sma_20']) and (c < r['sma_20'] * 1.03) and (c > o)
        vwap_cum_pass = (bbw < 0.25) and (c > vwap) and (v3 >= 1.4) and (c > r['sma_20']) and (c < r['sma_20'] * 1.03) and (c > o)
        
        tags = []
        if baseline_pass: tags.append("BASELINE(1h>2.5)")
        if cum3h_pass: tags.append("CUM_3H")
        if vwap_cum_pass: tags.append("VWAP_CUM")
        tag_str = " | ".join(tags) if tags else "-"
        
        print(f"{t_utc} | O:{o:>7.2f} H:{h:>7.2f} L:{l:>7.2f} C:{c:>7.2f} ({c_pct:>+5.2f}%) | 1hVol:{v1:>4.2f}x 2hVol:{v2:>4.2f}x 3hVol:{v3:>4.2f}x | BBW:{bbw:.4f} | VWAP:{vwap:>7.2f} (Dist:{vwap_d:>+5.2f}%) | SMA20Dist:{sma_d:>+5.2f}% | [{tag_str}]")
