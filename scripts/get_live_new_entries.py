"""
Fetches live market quotes for the newly approved Monday NFO option entries.
"""
import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

def main():
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    
    if not api_key or not access_token:
        print("Error: Kite credentials missing")
        return
        
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token.strip("'"))
        
        options = {
            "NFO:CIPLA26JUL1460CE": {"symbol": "CIPLA", "suggested_entry": 35.65, "stop": 21.40, "lot_size": 425},
            "NFO:UNOMINDA26JUL1140CE": {"symbol": "UNOMINDA", "suggested_entry": 34.05, "stop": 20.45, "lot_size": 550}
        }
        
        underlying = ["NSE:CIPLA", "NSE:UNOMINDA"]
        instruments = list(options.keys()) + underlying
        
        ltp_data = kite.ltp(instruments)
        
        print("\n=========================================================================")
        print("NEW MONDAY LIVE ENTRIES STATUS")
        print("=========================================================================")
        
        for inst, opt in options.items():
            symbol = opt["symbol"]
            s_entry = opt["suggested_entry"]
            stop = opt["stop"]
            
            opt_ltp = ltp_data.get(inst, {}).get("last_price")
            und_ltp = ltp_data.get(f"NSE:{symbol}", {}).get("last_price")
            
            if opt_ltp is None:
                print(f"{symbol}: Error fetching quote")
                continue
                
            diff_pct = (opt_ltp - s_entry) / s_entry * 100
            
            print(f"Stock: {symbol:10s} | Price: ₹{und_ltp:.2f}")
            print(f"  Option:       {inst.split(':')[1]}")
            print(f"  Engine Entry: ₹{s_entry:.2f} (Stop: ₹{stop:.2f})")
            print(f"  Current LTP:  ₹{opt_ltp:.2f} ({diff_pct:+.2f}% vs Engine Entry)")
            print("-" * 50)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
