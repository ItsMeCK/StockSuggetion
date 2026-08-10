import os
import json
import polars as pl
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kiteconnect import KiteConnect

def calculate_options_pnl():
    load_dotenv()
    
    # 1. Load saved signals
    signals_file = "data/intraday_signals.json"
    if not os.path.exists(signals_file):
        print("No saved signals found.")
        return
        
    with open(signals_file, "r") as f:
        trades = json.load(f)
        
    if not trades:
        print("No trades found in signals file.")
        return
        
    # 2. Connect to Kite
    api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    print("Fetching NFO instruments...")
    try:
        instruments = kite.instruments("NFO")
    except Exception as e:
        print(f"Error fetching instruments (token might be invalid): {e}")
        return
        
    total_option_pnl_pct = 0.0
    processed_trades = 0
    
    # 3. Process each trade
    for trade in trades:
        symbol = trade["symbol"]
        entry_time_str = trade["time"]
        entry_price = trade["entry_price"]
        
        try:
            entry_time = datetime.fromisoformat(entry_time_str)
        except:
            entry_time = datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S%z")
            
        print(f"\nEvaluating Trade: {symbol} at {entry_time}")
        
        # Find ATM Call Option (CE)
        symbol_options = [i for i in instruments if i['name'] == symbol and i['instrument_type'] == 'CE']
        if not symbol_options:
            print(f"No CE options found for {symbol}")
            continue
            
        # Get current month expiry
        symbol_options.sort(key=lambda x: x['expiry'])
        nearest_expiry = symbol_options[0]['expiry']
        current_month_options = [i for i in symbol_options if i['expiry'] == nearest_expiry]
        
        # Find strike closest to entry_price
        closest_option = min(current_month_options, key=lambda x: abs(x['strike'] - entry_price))
        option_token = closest_option['instrument_token']
        option_symbol = closest_option['tradingsymbol']
        
        print(f"ATM Option Selected: {option_symbol} (Strike: {closest_option['strike']})")
        
        trade_date = entry_time.strftime("%Y-%m-%d")
        from_date = f"{trade_date} 09:15:00"
        to_date = f"{trade_date} 15:30:00"
        
        try:
            records = kite.historical_data(
                instrument_token=option_token,
                from_date=from_date,
                to_date=to_date,
                interval="5minute",
                continuous=False,
                oi=False
            )
        except Exception as e:
            print(f"Error fetching historical data for {option_symbol}: {e}")
            continue
            
        if not records:
            print(f"No historical data returned for {option_symbol}")
            continue
            
        # Find entry candle (first candle >= entry_time)
        entry_candle = None
        for r in records:
            if r['date'] >= entry_time:
                entry_candle = r
                break
                
        if not entry_candle:
            print(f"Could not find entry candle after {entry_time}")
            continue
            
        p_entry = entry_candle['close']
        initial_sl = p_entry * 0.50
        current_sl = initial_sl
        target_activation = p_entry * 1.50  # 50% profit
        
        highest_high = p_entry
        trailing_active = False
        
        exit_price = None
        exit_reason = ""
        exit_time = None
        
        start_idx = records.index(entry_candle) + 1
        
        for r in records[start_idx:]:
            candle_high = r['high']
            candle_low = r['low']
            
            # Check if SL is hit
            if candle_low <= current_sl:
                if r['open'] < current_sl:
                    exit_price = r['open']
                else:
                    exit_price = current_sl
                exit_reason = "SL Hit"
                exit_time = r['date']
                break
                
            # Update Highs and Trailing SL
            if candle_high > highest_high:
                highest_high = candle_high
                
            if highest_high >= target_activation:
                trailing_active = True
                new_sl = highest_high - (highest_high * 0.10)
                if new_sl > current_sl:
                    current_sl = new_sl
                    
        # If not exited, exit at EOD
        if exit_price is None:
            exit_price = records[-1]['close']
            exit_reason = "EOD Exit"
            exit_time = records[-1]['date']
            
        pnl_pct = ((exit_price - p_entry) / p_entry) * 100
        total_option_pnl_pct += pnl_pct
        processed_trades += 1
        
        print(f"Entry: {p_entry:.2f} | Exit: {exit_price:.2f} ({exit_reason} at {exit_time}) | PnL: {pnl_pct:+.2f}%")
        print(f"Max Profit Reached: {((highest_high - p_entry)/p_entry)*100:+.2f}% | Final SL: {current_sl:.2f}")

    if processed_trades > 0:
        avg_pnl = total_option_pnl_pct / processed_trades
        print(f"\n=================================")
        print(f"💰 Total Average Option PnL: {avg_pnl:+.2f}%")
        print(f"=================================")
    else:
        print("\nNo trades were processed.")

if __name__ == "__main__":
    calculate_options_pnl()
