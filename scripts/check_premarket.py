import os
import sys
from dotenv import load_dotenv
from kiteconnect import KiteConnect

sys.path.append(os.getcwd())
load_dotenv()

def main():
    api_key = os.getenv('KITE_API_KEY')
    access_token = os.getenv('KITE_ACCESS_TOKEN')
    
    kite = KiteConnect(api_key=api_key.strip("'\""))
    kite.set_access_token(access_token.strip("'\""))
    
    instruments = [
        'NSE:NIFTY 50', 'NSE:NIFTY BANK',
        'NSE:ABCAPITAL', 'NSE:EICHERMOT', 'NSE:HAL', 'NSE:SHRIRAMFIN', 'NSE:AMBER'
    ]
    
    try:
        q = kite.quote(instruments)
        print("Premarket Status:")
        nifty = q.get('NSE:NIFTY 50', {})
        nifty_ltp = nifty.get('last_price', 0)
        nifty_close = nifty.get('ohlc', {}).get('close', 1)
        nifty_pct = (nifty_ltp - nifty_close) / nifty_close * 100
        print(f"NIFTY 50: {nifty_ltp:.2f} ({nifty_pct:+.2f}%)")
        
        for inst in instruments:
            if inst != 'NSE:NIFTY 50':
                data = q.get(inst, {})
                ltp = data.get('last_price', 0)
                close = data.get('ohlc', {}).get('close', 1)
                pct = (ltp - close) / close * 100
                print(f"  {inst:15s}: LTP={ltp:<7.2f} ({pct:+.2f}%)")
    except Exception as e:
        print("Error fetching quotes:", e)

if __name__ == "__main__":
    main()
