import os
import sys
import time
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
        'NSE:NIFTY 50',
        'NSE:HDFCBANK', 'NFO:HDFCBANK26JUL810CE', 'NFO:HDFCBANK26JUL820CE',
        'NSE:RBLBANK', 'NFO:RBLBANK26JUL390CE', 'NFO:RBLBANK26JUL380CE',
        'NSE:360ONE', 'NSE:BAJFINANCE', 'NSE:EICHERMOT', 'NSE:ICICIBANK', 'NSE:PRESTIGE',
        'NSE:MFSL', 'NFO:MFSL26JUL1540PE'
    ]
    
    print("Waiting for market open at 09:15:02 AM...")
    # Polling loop
    for i in range(15):
        try:
            q = kite.quote(instruments)
            nifty = q.get('NSE:NIFTY 50', {})
            nifty_ltp = nifty.get('last_price', 0)
            nifty_close = nifty.get('ohlc', {}).get('close', 1)
            nifty_pct = (nifty_ltp - nifty_close) / nifty_close * 100
            
            print(f"\n--- Poll {i+1} | Nifty: {nifty_ltp:.2f} ({nifty_pct:+.2f}%) ---")
            for inst in instruments:
                if inst in q and inst != 'NSE:NIFTY 50':
                    data = q[inst]
                    ltp = data.get('last_price', 0)
                    close = data.get('ohlc', {}).get('close', 0)
                    pct = (ltp - close) / close * 100 if close else 0
                    print(f"  {inst:25s}: LTP={ltp:<7.2f} ({pct:+.2f}%)")
        except Exception as e:
            print("Error fetching quote:", e)
        time.sleep(2)

if __name__ == "__main__":
    main()
