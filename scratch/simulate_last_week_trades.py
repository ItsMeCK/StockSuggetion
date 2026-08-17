import polars as pl
from datetime import datetime
import os

def run_simulation():
    print("Loading data...")
    df = pl.read_parquet("data/intraday_ohlcv.parquet")
    
    # We also need the sector data to mock the sector gate
    sector_df = pl.read_parquet("scratch/sector_60m_august_2026.parquet")
    
    # Calculate all the new math engine variables
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
        ((pl.col("std_20") * 4) / pl.col("sma_20")).alias("bbw"),
        (pl.col("volume") / pl.col("vol_avg_20")).alias("vol_surge"),
        (pl.col("vol_sum_3") / (3 * pl.col("vol_avg_20"))).alias("vol_surge_3h"),
        (pl.col("cum_pv") / pl.col("cum_vol")).alias("vwap"),
        ((pl.col("close") - pl.col("sma_20")) / pl.col("sma_20") * 100).alias("dist_sma20"),
        ((pl.col("high") - pl.max_horizontal(pl.col("open"), pl.col("close"))) / (pl.col("high") - pl.col("low") + 0.0001) * 100).alias("upper_wick_pct")
    ])
    
    # Filter to last week (Aug 10 - Aug 14)
    start_date = datetime(2026, 8, 10).date()
    end_date = datetime(2026, 8, 14).date()
    df = df.filter((pl.col("date") >= start_date) & (pl.col("date") <= end_date))
    
    # Get all unique hours in the week
    unique_hours = df.select(pl.col("time")).unique().sort("time")['time'].to_list()
    
    from core.sector_mapping import get_sector_for_symbol
    
    # Pre-calculate daily sector returns
    sector_df = sector_df.with_columns([
        pl.col("time").dt.date().alias("date")
    ])
    
    daily_sector_returns = {}
    for d in sector_df.select("date").unique()['date'].to_list():
        day_data = sector_df.filter(pl.col("date") == d).sort("time")
        daily_sector_returns[d] = {}
        for sector in day_data.select("sector").unique()['sector'].to_list():
            sec_day = day_data.filter(pl.col("sector") == sector)
            if len(sec_day) > 0:
                open_p = sec_day[0, "open"]
                close_p = sec_day[-1, "close"]
                ret = ((close_p - open_p) / open_p) * 100
                daily_sector_returns[d][sector] = ret
                
    total_trades = 0
    print("\n--- SIMULATION TRADES (Aug 10 - Aug 14) ---")
    
    for t in unique_hours:
        current_hour_df = df.filter(pl.col("time") == t)
        
        # 4. Math Engine (Dual-Track Volume + Dual-Tier Gap & Go)
        base_filter = (pl.col("bbw") < 0.22) & (pl.col("close") > pl.col("sma_20")) & (pl.col("close") > pl.col("open"))
        
        track_a = (pl.col("vol_surge") > 2.5)
        track_b = (pl.col("vol_surge_3h") > 1.5) & (pl.col("vol_surge") > 1.1) & (pl.col("close") > pl.col("vwap"))
        volume_filter = (track_a | track_b)
        
        tier_1 = (pl.col("dist_sma20") <= 3.0) & (pl.col("upper_wick_pct") < 25.0)
        tier_2 = (pl.col("dist_sma20") > 3.0) & (pl.col("dist_sma20") <= 7.5) & (pl.col("upper_wick_pct") < 12.0) & (pl.col("vol_surge") > 3.5)
        tier_filter = (tier_1 | tier_2)
        
        breakouts = current_hour_df.filter(
            base_filter & volume_filter & tier_filter
        ).sort("vol_surge", descending=True).head(4)
        
        valid_symbols = [row['symbol'] for row in breakouts.iter_rows(named=True)]
        
        if len(valid_symbols) == 0:
            continue
            
        # Sector Veto
        d = t.date()
        approved_symbols = []
        for sym in valid_symbols:
            sec = get_sector_for_symbol(sym)
            if not sec:
                approved_symbols.append(sym)
                continue
            
            sec_ret = daily_sector_returns.get(d, {}).get(sec, 0.0)
            if sec_ret >= 0.0:
                approved_symbols.append(sym)
                
        if len(approved_symbols) > 0:
            print(f"\n[{t.strftime('%Y-%m-%d %H:%M')}] System triggered {len(approved_symbols)} Math Setups:")
            for sym in approved_symbols:
                row = breakouts.filter(pl.col("symbol") == sym).row(0, named=True)
                track = "Track A (Spike)" if row['vol_surge'] > 2.5 else "Track B (Cumul)"
                tier = "Tier 2 (Gap&Go)" if row['dist_sma20'] > 3.0 else "Tier 1"
                print(f"  -> {sym}: {track} | {tier} | Wick: {row['upper_wick_pct']:.1f}% | Entry: {row['close']}")
                total_trades += 1
                
    print(f"\nTotal Simulated Mathematical Setups passing Sector Veto: {total_trades}")
    print("(Note: The LLM would then filter these down to max 2 per hour)")

if __name__ == '__main__':
    run_simulation()
