import os
import logging
import pandas as pd
from kiteconnect import KiteConnect
from dotenv import load_dotenv
from datetime import datetime, timedelta

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
from_date = today - timedelta(days=2)

results = []

print(f"Fetching intraday data for {len(token_map)} stocks. This will take a moment...")
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
        # Convert date to local time assuming it comes in as aware datetime
        df['date'] = pd.to_datetime(df['date'])
        
        # Split into yesterday and today
        today_date = df['date'].iloc[-1].date()
        yesterday_df = df[df['date'].dt.date < today_date]
        today_df = df[df['date'].dt.date == today_date]
        
        if yesterday_df.empty or today_df.empty:
            continue
            
        yesterday_close = yesterday_df.iloc[-1]['close']
        today_open = today_df.iloc[0]['open']
        eod_close = today_df.iloc[-1]['close']
        
        # Find 10:30 candle (which is the 10:15 - 10:30 candle, closing at 10:30)
        # Kite 15m candles: 09:15, 09:30, 09:45, 10:00, 10:15
        # The 10:15 candle represents data up to 10:30.
        candle_1015 = today_df[today_df['date'].dt.strftime('%H:%M') == '10:15']
        
        if candle_1015.empty:
            continue
            
        price_1030 = candle_1015.iloc[0]['close']
        
        morning_move_pct = ((price_1030 - yesterday_close) / yesterday_close) * 100
        afternoon_move_pct = ((eod_close - price_1030) / price_1030) * 100
        total_day_move = ((eod_close - yesterday_close) / yesterday_close) * 100
        
        results.append({
            'symbol': symbol,
            'yest_close': yesterday_close,
            'open': today_open,
            'price_1030': price_1030,
            'close': eod_close,
            'morning_move_pct': morning_move_pct,
            'afternoon_move_pct': afternoon_move_pct,
            'total_day_move': total_day_move
        })
        
    except Exception as e:
        continue

df_res = pd.DataFrame(results)

print("\n=== STOCKS UP > 4% BY 10:30 AM (THE GAP UPS / MORNING CHASES) ===")
gap_ups = df_res[df_res['morning_move_pct'] > 4.0].sort_values('morning_move_pct', ascending=False)
print(f"Total Gap Ups / Morning Surges: {len(gap_ups)}")
print(gap_ups[['symbol', 'morning_move_pct', 'afternoon_move_pct']].head(10).to_string(index=False))

print("\n=== STOCKS FLAT IN MORNING, BUT SURGED > 4% AFTER 10:30 AM (THE TRUE INTRADAY BREAKOUTS) ===")
# Flat in morning means between -1% and +2% at 10:30.
true_breakouts = df_res[(df_res['morning_move_pct'] > -1.0) & (df_res['morning_move_pct'] <= 2.0) & (df_res['afternoon_move_pct'] > 4.0)]
true_breakouts = true_breakouts.sort_values('afternoon_move_pct', ascending=False)
print(f"Total True Intraday Breakouts: {len(true_breakouts)}")
print(true_breakouts[['symbol', 'morning_move_pct', 'afternoon_move_pct', 'total_day_move']].head(10).to_string(index=False))
