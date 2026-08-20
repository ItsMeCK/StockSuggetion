import sys
import polars as pl
from datetime import datetime
import pytz

def analyze_last_week():
    print("Loading Parquet data...")
    parquet_path = "data/intraday_ohlcv.parquet"
    df = pl.read_parquet(parquet_path)
    df = df.sort(["symbol", "time"])

    # Engine logic from live_hourly_job.py
    df = df.with_columns([
        pl.col("close").rolling_mean(window_size=20).over("symbol").alias("sma_20"),
        pl.col("close").rolling_std(window_size=20).over("symbol").alias("std_20"),
        pl.col("volume").rolling_mean(window_size=20).over("symbol").alias("vol_avg_20"),
        pl.col("volume").rolling_sum(window_size=3).over("symbol").alias("vol_sum_3"),
        pl.col("time").dt.date().alias("date"),
        (pl.col("close") * pl.col("volume")).alias("pv")
    ])

    df = df.with_columns([
        pl.col("volume").cum_sum().over(["symbol", "date"]).alias("cum_vol"),
        pl.col("pv").cum_sum().over(["symbol", "date"]).alias("cum_pv")
    ])

    df = df.with_columns([
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap"),
        (pl.col("close") * pl.col("volume")).rolling_mean(window_size=20*6).over("symbol").alias("atv")
    ])

    df = df.with_columns([
        ((pl.col("sma_20") + 2 * pl.col("std_20") - (pl.col("sma_20") - 2 * pl.col("std_20"))) / pl.col("sma_20")).alias("bbw")
    ])
    df = df.with_columns([
        pl.col("bbw").rolling_quantile(quantile=0.10, window_size=100).over("symbol").alias("bbw_threshold_10th")
    ])

    df = df.with_columns([
        pl.max_horizontal(
            (pl.col("high") - pl.col("low")),
            (pl.col("high") - pl.col("close").shift(1).over("symbol")).abs(),
            (pl.col("low") - pl.col("close").shift(1).over("symbol")).abs()
        ).alias("true_range")
    ])
    df = df.with_columns([
        pl.col("low").shift(1).over("symbol").alias("low_1"),
        pl.col("low").shift(2).over("symbol").alias("low_2"),
        pl.col("true_range").shift(1).over("symbol").alias("tr_1"),
        pl.col("true_range").shift(2).over("symbol").alias("tr_2"),
        ((pl.col("close") - pl.col("low")) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("close_range_pct"),
        pl.col("volume").shift(1).over("symbol").alias("vol_1")
    ])

    df = df.with_columns([
        pl.col("vwap").shift(3).over(["symbol", "date"]).alias("vwap_prev3"),
        pl.col("sma_20").shift(3).over("symbol").alias("sma_20_prev3"),
    ])
    df = df.with_columns([
        (((pl.col("vwap") - pl.col("vwap_prev3")) / pl.col("vwap_prev3")) * 100).alias("vwap_slope_pct"),
        (((pl.col("sma_20") - pl.col("sma_20_prev3")) / pl.col("sma_20_prev3")) * 100).alias("sma20_slope_pct"),
    ])

    df = df.with_columns([
        (((pl.col("close") - pl.col("close").shift(3).over("symbol")) / pl.col("close").shift(3).over("symbol")) * 100).alias("ret_3h")
    ])

    df = df.with_columns([
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (pl.col("vol_sum_3") / (3 * pl.col("vol_avg_20"))).alias("vol_surge_3h"),
        ((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20") * 100).alias("dist_sma20"),
        ((pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("upper_wick_pct")
    ])

    target_dates = [
        pl.date(2026, 8, 10),
        pl.date(2026, 8, 11),
        pl.date(2026, 8, 12),
        pl.date(2026, 8, 13),
        pl.date(2026, 8, 14)
    ]
    
    all_results = []

    for t_date in target_dates:
        # Simulation loop for date
        unique_times = df.filter(pl.col("time").dt.date() == t_date).select("time").unique().sort("time")
        if unique_times.is_empty():
            continue
            
        eod_time = unique_times.to_series()[-1]
        eod_df = df.filter(pl.col("time") == eod_time)
        
        for row in unique_times.iter_rows():
            ts = row[0]
            if ts.hour < 4 or (ts.hour == 4 and ts.minute < 45):
                continue # Skip before 10:15 IST
                
            current_hour_df = df.filter(pl.col("time") == ts)
            nifty_df = current_hour_df.filter(pl.col("symbol") == "NIFTY 50")
            nifty_ret_3h = nifty_df.select(pl.col("ret_3h"))[0, 0] if not nifty_df.is_empty() else 0.0
            if nifty_ret_3h is None: nifty_ret_3h = 0.0
                
            dynamic_coil = (pl.col("bbw") < pl.col("bbw_threshold_10th")) | (pl.col("bbw") < 0.22)
            base_filter = dynamic_coil & (pl.col("close") > pl.col("sma_20")) & (pl.col("close") > pl.col("open"))
            ma_awareness = (pl.col("vwap_slope_pct") > 0.1) & (pl.col("sma20_slope_pct").abs() < 0.2)
            track_a = (pl.col("vol_surge") > 2.5) & (pl.col("upper_wick_pct") < 25.0)
            
            mega_cap_stealth = (pl.col("atv") >= 500_000_000) & (pl.col("vol_surge_3h") >= 1.60) & (pl.col("close") <= (pl.col("vwap") * 1.015))
            mid_cap_stealth = (pl.col("atv") < 500_000_000) & (pl.col("atv") >= 100_000_000) & (pl.col("vol_surge_3h") >= 1.83) & (pl.col("close") <= (pl.col("vwap") * 1.010))
            multi_candle_iaf = (
                (pl.col("low") >= pl.col("low_1")) & (pl.col("low_1") >= pl.col("low_2")) & 
                (pl.col("true_range") <= pl.col("tr_1")) & (pl.col("tr_1") <= pl.col("tr_2")) & 
                (pl.col("volume") >= pl.col("vol_1")) & (pl.col("close_range_pct") <= 85.0)
            )
            track_b = (mega_cap_stealth | mid_cap_stealth) & multi_candle_iaf & ma_awareness
            rs_divergence = (pl.col("ret_3h") > (nifty_ret_3h + 0.5))
            
            breakouts = current_hour_df.filter(
                base_filter & (track_a | track_b) & rs_divergence
            ).sort("vol_surge", descending=True).head(4)
            
            valid_symbols = [r['symbol'] for r in breakouts.iter_rows(named=True) if r['symbol'] != "NIFTY 50"]
            
            # Forward return calculation (bypassing debate)
            for sym in valid_symbols:
                entry_price = current_hour_df.filter(pl.col("symbol") == sym).select(pl.col("close"))[0, 0]
                sym_df = df.filter((pl.col("symbol") == sym) & (pl.col("time").dt.date() == t_date))
                forward_df = sym_df.filter(pl.col("time") > ts)
                
                if forward_df.is_empty():
                    max_high = entry_price
                    eod_close = entry_price
                else:
                    max_high = forward_df.select(pl.max("high"))[0, 0]
                    eod_close = forward_df.select(pl.last("close"))[0, 0]
                    
                max_exc_pct = ((max_high - entry_price) / entry_price) * 100
                eod_pnl_pct = ((eod_close - entry_price) / entry_price) * 100
                
                ist_tz = pytz.timezone('Asia/Kolkata')
                target_ist = ts.astimezone(ist_tz)
                ist_str = target_ist.strftime("%H:%M")
                
                all_results.append({
                    "Date": str(t_date),
                    "Time": ist_str,
                    "Symbol": sym,
                    "Max Excursion %": round(max_exc_pct, 2),
                    "EOD P&L %": round(eod_pnl_pct, 2)
                })

    print("\n--- LAST WEEK (AUG 10 - AUG 14) MATH ENGINE RESULTS ---")
    res_df = pl.DataFrame(all_results)
    if not res_df.is_empty():
        print(res_df)
        print("\nSummary Statistics:")
        print(f"Total Setups: {len(res_df)}")
        print(f"Average Max Excursion: {res_df['Max Excursion %'].mean():.2f}%")
        print(f"Average EOD P&L: {res_df['EOD P&L %'].mean():.2f}%")
        
        breakouts = res_df.filter(pl.col("Max Excursion %") >= 1.5)
        print(f"\nBreakouts (> 1.5% push): {len(breakouts)}")
        if len(breakouts) > 0:
            print(breakouts.select(["Date", "Time", "Symbol", "Max Excursion %", "EOD P&L %"]))
            
        winners = res_df.filter(pl.col("EOD P&L %") > 0.5)
        print(f"\nStrong EOD Winners (> 0.5%): {len(winners)}")

if __name__ == "__main__":
    analyze_last_week()
