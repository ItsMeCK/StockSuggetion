import os
import logging
import pandas as pd
from kiteconnect import KiteConnect
from dotenv import load_dotenv
from datetime import datetime, timedelta
import numpy as np

logging.basicConfig(level=logging.ERROR)
load_dotenv()

kite = KiteConnect(api_key=os.getenv('KITE_API_KEY'))
kite.set_access_token(os.getenv('KITE_ACCESS_TOKEN').strip("'"))

print("Loading Nifty 500 Universe...")
try:
    universe_df = pd.read_csv('pipeline/master_universe.csv')
    if 'Symbol' in universe_df.columns:
        symbols = universe_df['Symbol'].tolist()
    else:
        symbols = universe_df.iloc[:, 0].tolist()
except:
    print("Could not load master_universe.csv")
    symbols = []

print("Fetching NSE Instruments mapping...")
instruments = kite.instruments("NSE")
token_map = {i['tradingsymbol']: i['instrument_token'] for i in instruments if i['tradingsymbol'] in symbols}

today = datetime.now()
from_date = today - timedelta(days=7) # Get 7 days to ensure we have at least 3-4 trading days

results = []

print(f"Fetching 7-day intraday data for {len(token_map)} stocks to test VSA/VCP theory...")
count = 0

for symbol, token in token_map.items():
    count += 1
    if count % 50 == 0:
        print(f"Processed {count}/{len(token_map)}")
        
    try:
        data = kite.historical_data(token, from_date, today, '15minute')
        if not data:
            continue
            
        df = pd.DataFrame(data)
        df['date'] = pd.to_datetime(df['date'])
        
        # Get unique dates
        unique_dates = df['date'].dt.date.unique()
        if len(unique_dates) < 2:
            continue
            
        today_date = unique_dates[-1]
        
        # Calculate morning volume (9:15 to 10:30) for all historical days
        # This includes candles: 09:15, 09:30, 09:45, 10:00, 10:15
        morning_times = ['09:15', '09:30', '09:45', '10:00', '10:15']
        
        historical_morning_vols = []
        for past_date in unique_dates[:-1]:
            past_df = df[df['date'].dt.date == past_date]
            past_morning = past_df[past_df['date'].dt.strftime('%H:%M').isin(morning_times)]
            if not past_morning.empty:
                historical_morning_vols.append(past_morning['volume'].sum())
                
        if not historical_morning_vols:
            continue
            
        avg_morning_vol = np.mean(historical_morning_vols)
        
        # Today's metrics
        today_df = df[df['date'].dt.date == today_date]
        today_morning = today_df[today_df['date'].dt.strftime('%H:%M').isin(morning_times)]
        
        if today_morning.empty or avg_morning_vol == 0:
            continue
            
        today_morning_vol = today_morning['volume'].sum()
        relative_morning_vol = today_morning_vol / avg_morning_vol
        
        # Price action
        yesterday_df = df[df['date'].dt.date == unique_dates[-2]]
        yesterday_close = yesterday_df.iloc[-1]['close']
        
        candle_1015 = today_morning[today_morning['date'].dt.strftime('%H:%M') == '10:15']
        if candle_1015.empty:
            continue
            
        price_1030 = candle_1015.iloc[0]['close']
        eod_close = today_df.iloc[-1]['close']
        
        morning_move_pct = ((price_1030 - yesterday_close) / yesterday_close) * 100
        afternoon_move_pct = ((eod_close - price_1030) / price_1030) * 100
        total_day_move = ((eod_close - yesterday_close) / yesterday_close) * 100
        
        results.append({
            'symbol': symbol,
            'rel_morning_vol': relative_morning_vol,
            'morning_move_pct': morning_move_pct,
            'afternoon_move_pct': afternoon_move_pct,
            'total_day_move': total_day_move,
            'today_vol': today_morning_vol,
            'avg_vol': avg_morning_vol
        })
        
    except Exception as e:
        continue

df_res = pd.DataFrame(results)

print("\n=== VSA THEORY TEST RESULTS ===")

print("\n1. Let's look at the stocks with HUGE hidden morning volume (>2.5x normal) but FLAT price (-1% to 2%):")
vsa_candidates = df_res[(df_res['rel_morning_vol'] >= 2.0) & (df_res['morning_move_pct'] > -1.0) & (df_res['morning_move_pct'] <= 2.0)]
vsa_candidates = vsa_candidates.sort_values('rel_morning_vol', ascending=False)
print(f"Found {len(vsa_candidates)} VSA/VCP candidates at 10:30 AM.")
if not vsa_candidates.empty:
    print(vsa_candidates[['symbol', 'rel_morning_vol', 'morning_move_pct', 'afternoon_move_pct']].head(15).to_string(index=False))

print("\n2. Where did our previous missed breakouts (CAPLIPOINT, KEI, CEMPRO) rank in Volume Surge?")
missed = ['CAPLIPOINT', 'KEI', 'CEMPRO']
missed_df = df_res[df_res['symbol'].isin(missed)]
if not missed_df.empty:
    print(missed_df[['symbol', 'rel_morning_vol', 'morning_move_pct', 'afternoon_move_pct']].to_string(index=False))
