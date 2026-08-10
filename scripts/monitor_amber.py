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
        'NSE:NIFTY 50', 'NSE:AMBER', 'NFO:AMBER26JUL7700PE'
    ]
    
    print("Waiting for market open to poll AMBER PE quotes...")
    # Polling loop
    for i in range(10):
        try:
            q = kite.quote(instruments)
            nifty = q.get('NSE:NIFTY 50', {})
            nifty_ltp = nifty.get('last_price', 0)
            nifty_close = nifty.get('ohlc', {}).get('close', 1)
            nifty_pct = (nifty_ltp - nifty_close) / nifty_close * 100
            
            amber = q.get('NSE:AMBER', {})
            amber_ltp = amber.get('last_price', 0)
            amber_close = amber.get('ohlc', {}).get('close', 1)
            amber_pct = (amber_ltp - amber_close) / amber_close * 100
            
            opt = q.get('NFO:AMBER26JUL7700PE', {})
            opt_ltp = opt.get('last_price', 0)
            
            depth = opt.get('depth', {})
            buy = depth.get('buy', [])
            sell = depth.get('sell', [])
            
            best_buy = buy[0].get('price', 0) if buy else 0
            best_sell = sell[0].get('price', 0) if sell else 0
            spread = best_sell - best_buy
            spread_pct = (spread / opt_ltp * 100) if opt_ltp else 0
            
            print(f"\n--- Poll {i+1} | Nifty: {nifty_ltp:.2f} ({nifty_pct:+.2f}%) ---")
            print(f"  AMBER Spot: ₹{amber_ltp:.2f} ({amber_pct:+.2f}%)")
            print(f"  AMBER 7700 PE: LTP=₹{opt_ltp:.2f} | Bid=₹{best_buy} | Ask=₹{best_sell} | Spread=₹{spread:.2f} ({spread_pct:.2f}%)")
        except Exception as e:
            print("Error:", e)
        time.sleep(2)

if __name__ == "__main__":
    main()
