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
from_date = today - timedelta(days=15) # Pull 15 days to have enough history for 5 test days

all_signals = []

print(f"Fetching 15-day intraday data for {len(token_map)} stocks to run strict backtest...")
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
        
        unique_dates = sorted(df['date'].dt.date.unique())
        if len(unique_dates) < 8:
            continue
            
        # We will test the last 5 days
        test_dates = unique_dates[-5:]
        morning_times = ['09:15', '09:30', '09:45', '10:00', '10:15']
        
        for test_idx, test_date in enumerate(test_dates):
            # Find the index of this test date in the unique_dates array
            act_idx = unique_dates.index(test_date)
            
            # Need at least 3 days of history prior to test date for moving average volume
            if act_idx < 3:
                continue
                
            history_dates = unique_dates[act_idx-3 : act_idx]
            
            # Calculate historical average morning volume
            historical_morning_vols = []
            for h_date in history_dates:
                h_df = df[df['date'].dt.date == h_date]
                h_morning = h_df[h_df['date'].dt.strftime('%H:%M').isin(morning_times)]
                if not h_morning.empty:
                    historical_morning_vols.append(h_morning['volume'].sum())
                    
            if not historical_morning_vols:
                continue
                
            avg_morning_vol = np.mean(historical_morning_vols)
            if avg_morning_vol == 0:
                continue
                
            # Evaluate Test Date (10:30 AM logic)
            t_df = df[df['date'].dt.date == test_date]
            t_morning = t_df[t_df['date'].dt.strftime('%H:%M').isin(morning_times)]
            
            if t_morning.empty:
                continue
                
            today_morning_vol = t_morning['volume'].sum()
            relative_morning_vol = today_morning_vol / avg_morning_vol
            
            # Price Action
            yest_df = df[df['date'].dt.date == history_dates[-1]]
            yest_close = yest_df.iloc[-1]['close']
            
            candle_1015 = t_morning[t_morning['date'].dt.strftime('%H:%M') == '10:15']
            if candle_1015.empty:
                continue
                
            price_1030 = candle_1015.iloc[0]['close']
            eod_close = t_df.iloc[-1]['close']
            
            morning_move_pct = ((price_1030 - yest_close) / yest_close) * 100
            afternoon_move_pct = ((eod_close - price_1030) / price_1030) * 100
            
            # VOLATILITY VACUUM STRATEGY RULES:
            # 1. Price is tight/flat (-0.5% to +1.5%)
            # 2. Volume has completely dried up (RMV < 0.6)
            if -0.5 <= morning_move_pct <= 1.5 and relative_morning_vol < 0.6:
                all_signals.append({
                    'date': test_date,
                    'symbol': symbol,
                    'morning_move': round(morning_move_pct, 2),
                    'rel_vol': round(relative_morning_vol, 2),
                    'afternoon_move': round(afternoon_move_pct, 2)
                })
                
    except Exception as e:
        continue

res_df = pd.DataFrame(all_signals)

print("\n\n" + "="*60)
print("     VOLATILITY VACUUM STRATEGY - 5 DAY STRICT BACKTEST")
print("="*60)

if res_df.empty:
    print("No signals found matching criteria.")
else:
    total_trades = len(res_df)
    winners = res_df[res_df['afternoon_move'] > 0]
    losers = res_df[res_df['afternoon_move'] <= 0]
    
    win_rate = (len(winners) / total_trades) * 100
    avg_win = winners['afternoon_move'].mean() if not winners.empty else 0
    avg_loss = losers['afternoon_move'].mean() if not losers.empty else 0
    avg_total = res_df['afternoon_move'].mean()
    
    # Simulate a portfolio that risks 5k per trade
    # If a trade drops > 5% in afternoon, we assume the 5% stop loss cut it.
    res_df['sim_move'] = np.where(res_df['afternoon_move'] < -5.0, -5.0, res_df['afternoon_move'])
    avg_sim_move = res_df['sim_move'].mean()
    
    print(f"Total Signals Fired (Over 5 Days): {total_trades}")
    print(f"Win Rate: {win_rate:.1f}%")
    print(f"Average Winner: +{avg_win:.2f}%")
    print(f"Average Loser: {avg_loss:.2f}%")
    print(f"EXPECTANCY (Avg Move per Trade): {avg_total:+.2f}%")
    print(f"EXPECTANCY w/ 5% Stop-Loss Guard: {avg_sim_move:+.2f}%")
    
    print("\n--- TOP 10 EXPLOSIVE WINNERS CAUGHT AT 10:30 AM ---")
    top_winners = res_df.sort_values('afternoon_move', ascending=False).head(10)
    print(top_winners.to_string(index=False))
    
    print("\n--- WORST 5 LOSERS ---")
    worst_losers = res_df.sort_values('afternoon_move', ascending=True).head(5)
    print(worst_losers.to_string(index=False))
