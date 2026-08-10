import os
import sys
from datetime import datetime, date

sys.path.append(os.getcwd())

from kiteconnect import KiteConnect

def main():
    api_key = os.getenv("KITE_API_KEY", "anywvvfkcyjhhqiy")
    # User's new access token
    access_token = "hPAlx3v2CncM1pynw9Rs1wu1fr00Gwtz"
    
    print(f"Initializing KiteConnect with API key: {api_key}")
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    
    # 1. Fetch stock quotes for underlyings
    underlyings = ['NSE:CONCOR', 'NSE:LICHSGFIN', 'NSE:RELIANCE', 'NSE:SBICARD', 'NSE:VOLTAS']
    print("\nFetching quotes for underlyings...")
    try:
        quotes = kite.quote(underlyings)
        for sym in underlyings:
            q = quotes.get(sym, {})
            last_price = q.get('last_price')
            ohlc = q.get('ohlc', {})
            prev_close = ohlc.get('close', 0.0)
            change = (last_price - prev_close) / prev_close * 100 if prev_close > 0 else 0.0
            print(f"  - {sym}: Close/LTP: {last_price} | Prev Close (July 8): {prev_close} | Change: {change:.2f}%")
    except Exception as e:
        print(f"Error fetching stock quotes: {e}")
        return

    # 2. Find option instruments
    print("\nLoading NFO instruments...")
    try:
        instruments = kite.instruments('NFO')
    except Exception as e:
        print(f"Error fetching NFO instruments: {e}")
        return

    # We want July 2026 expiry option contracts.
    # Expiry is July 28th, 2026
    target_expiry = date(2026, 7, 28)
    
    # Underlyings and their ATM strikes:
    atm_strikes = {
        'CONCOR': 450.0,
        'LICHSGFIN': 530.0,
        'RELIANCE': 1280.0,
        'SBICARD': 590.0,
        'VOLTAS': 1250.0
    }
    
    options_to_fetch = []
    symbol_by_token = {}
    
    for inst in instruments:
        underlying = inst['expiry']
        # check expiry is correct and instrument is PE
        if inst['expiry'] == target_expiry and inst['instrument_type'] == 'PE':
            und = inst['name']
            if und in atm_strikes:
                # check if strike matches our ATM strike
                if abs(float(inst['strike']) - atm_strikes[und]) < 0.1:
                    options_to_fetch.append(f"NFO:{inst['tradingsymbol']}")
                    symbol_by_token[f"NFO:{inst['tradingsymbol']}"] = {
                        "symbol": und,
                        "strike": inst['strike'],
                        "tradingsymbol": inst['tradingsymbol']
                    }

    print(f"Found option contracts to query: {options_to_fetch}")
    
    if not options_to_fetch:
        print("No matching PE option instruments found.")
        return

    print("\nFetching quotes for options...")
    try:
        opt_quotes = kite.quote(options_to_fetch)
        for sym in options_to_fetch:
            q = opt_quotes.get(sym, {})
            meta = symbol_by_token[sym]
            last_price = q.get('last_price')
            ohlc = q.get('ohlc', {})
            open_p = ohlc.get('open', 0.0)
            high_p = q.get('depth', {}).get('buy', [{}])[0].get('price', last_price) # fallback or high
            high_p = max(high_p, last_price, ohlc.get('high', 0.0))
            prev_close = ohlc.get('close', 0.0) # July 8 close
            
            # Max gain calculation: (High of July 9 - July 8 Close) / July 8 Close
            max_gain = (high_p - prev_close) / prev_close * 100 if prev_close > 0 else 0.0
            ltp_gain = (last_price - prev_close) / prev_close * 100 if prev_close > 0 else 0.0
            
            print(f"  - {meta['symbol']} {meta['strike']} PE ({meta['tradingsymbol']}):")
            print(f"    * July 8 Close (Entry): ₹{prev_close:.2f}")
            print(f"    * July 9 Open: ₹{open_p:.2f}")
            print(f"    * July 9 High: ₹{high_p:.2f}")
            print(f"    * July 9 LTP: ₹{last_price:.2f}")
            print(f"    * Max Intraday Gain: {max_gain:+.2f}%")
            print(f"    * End of Day Gain: {ltp_gain:+.2f}%")
    except Exception as e:
        print(f"Error fetching options quotes: {e}")

if __name__ == "__main__":
    main()
