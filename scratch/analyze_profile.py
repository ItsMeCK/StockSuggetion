import polars as pl
import pandas as pd
import numpy as np

def calculate_volume_profile(df_day, bins=50):
    if df_day.empty: return None
    min_price = df_day['low'].min()
    max_price = df_day['high'].max()
    if min_price == max_price: return None
    
    price_bins = np.linspace(min_price, max_price, bins)
    volume_profile = np.zeros(bins - 1)
    
    for _, row in df_day.iterrows():
        candle_range = row['high'] - row['low']
        if candle_range == 0:
            idx = np.searchsorted(price_bins, row['close']) - 1
            idx = max(0, min(bins-2, idx))
            volume_profile[idx] += row['volume']
            continue
            
        for i in range(len(price_bins)-1):
            bin_low = price_bins[i]
            bin_high = price_bins[i+1]
            overlap_low = max(row['low'], bin_low)
            overlap_high = min(row['high'], bin_high)
            
            if overlap_low < overlap_high:
                overlap_ratio = (overlap_high - overlap_low) / candle_range
                volume_profile[i] += row['volume'] * overlap_ratio
                
    poc_idx = np.argmax(volume_profile)
    poc = (price_bins[poc_idx] + price_bins[poc_idx+1]) / 2
    
    total_vol = np.sum(volume_profile)
    target_vol = total_vol * 0.70
    
    current_vol = volume_profile[poc_idx]
    upper_idx = poc_idx + 1
    lower_idx = poc_idx - 1
    
    while current_vol < target_vol and (upper_idx < len(volume_profile) or lower_idx >= 0):
        upper_vol = volume_profile[upper_idx] if upper_idx < len(volume_profile) else 0
        lower_vol = volume_profile[lower_idx] if lower_idx >= 0 else 0
        
        if upper_vol > lower_vol:
            current_vol += upper_vol
            upper_idx += 1
        else:
            current_vol += lower_vol
            lower_idx -= 1
            
    vah = price_bins[min(len(price_bins)-1, upper_idx)]
    val = price_bins[max(0, lower_idx)]
    
    return {'POC': poc, 'VAH': vah, 'VAL': val}

# Read data
df = pl.read_parquet('data/intraday_ohlcv.parquet').to_pandas()
df['time'] = pd.to_datetime(df['time'])
df['date'] = df['time'].dt.date

symbols = ['PREMIERENE', 'CROMPTON']
dates = df['date'].unique()
dates = sorted([d for d in dates if d.strftime('%Y-%m-%d') <= '2026-08-20'])

if len(dates) >= 2:
    prev_date = dates[-2]
    target_date = dates[-1] 
    print(f"Analyzing Prev Date: {prev_date}, Target Date (Aug 20): {target_date}")
    
    for sym in symbols:
        df_sym = df[df['symbol'] == sym]
        df_prev = df_sym[df_sym['date'] == prev_date]
        df_target = df_sym[df_sym['date'] == target_date]
        
        profile = calculate_volume_profile(df_prev)
        if profile:
            print(f"\n{sym} Previous Day ({prev_date}) Value Area:")
            print(f"VAH: {profile['VAH']:.2f}, POC: {profile['POC']:.2f}, VAL: {profile['VAL']:.2f}")
            
            print(f"{sym} Aug 20 Activity:")
            for _, row in df_target.iterrows():
                print(f"Time: {row['time'].time()}, Open: {row['open']:.2f}, High: {row['high']:.2f}, Low: {row['low']:.2f}, Close: {row['close']:.2f}, Vol: {row['volume']}")
        else:
            print(f"No data for {sym} on previous day.")
