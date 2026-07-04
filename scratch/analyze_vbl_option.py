import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect
import datetime

load_dotenv()

def analyze_vbl_option():
    api_key = os.getenv("KITE_API_KEY").strip("'\"")
    access_token = os.getenv("KITE_ACCESS_TOKEN").strip("'\"")
    
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        
        print("Downloading NFO instruments from Zerodha...")
        instruments = kite.instruments(exchange="NFO")
        
        # Filter for VBL CE options
        vbl_options = [i for i in instruments if i['name'] == 'VBL' and i['instrument_type'] == 'CE']
        
        # We want JUNE expiry. Let's find expiries in June 2026
        # Usually, the monthly expiry is the last Thursday. Let's just find anything > May 31.
        june_options = [i for i in vbl_options if i['expiry'].month == 6 and i['strike'] == 550.0]
        
        if not june_options:
            print("Could not find VBL JUNE 550 CE NFO.")
            # Let's just print all 550 CE expiries available
            all_550 = set([i['expiry'].strftime("%Y-%m-%d") for i in vbl_options if i['strike'] == 550.0])
            print(f"Available 550 CE expiries: {sorted(list(all_550))}")
            return
            
        target_instrument = june_options[0]
        tradingsymbol = target_instrument['tradingsymbol']
        instrument_token = target_instrument['instrument_token']
        lot_size = target_instrument['lot_size']
        
        print(f"\nFound Target: {tradingsymbol} (Token: {instrument_token})")
        print(f"Lot Size: {lot_size}")
        
        # Fetch live quote for the option and the underlying
        underlying = [i for i in kite.instruments("NSE") if i['tradingsymbol'] == 'VBL'][0]
        underlying_symbol = f"NSE:{underlying['tradingsymbol']}"
        option_symbol = f"NFO:{tradingsymbol}"
        
        print(f"\nFetching Live Quotes for {underlying_symbol} and {option_symbol}...")
        quotes = kite.quote([underlying_symbol, option_symbol])
        
        opt_data = quotes.get(option_symbol, {})
        und_data = quotes.get(underlying_symbol, {})
        
        if not opt_data:
            print("Failed to fetch quote for option.")
            return
            
        print("\n--- UNDERLYING VBL ---")
        print(f"LTP: ₹{und_data.get('last_price')}")
        print(f"Day High: ₹{und_data.get('ohlc', {}).get('high')}")
        print(f"Volume: {und_data.get('volume')}")
        
        print(f"\n--- {tradingsymbol} ---")
        print(f"LTP: ₹{opt_data.get('last_price')}")
        print(f"Open Interest (OI): {opt_data.get('oi')}")
        print(f"Volume: {opt_data.get('volume')}")
        
        depth = opt_data.get('depth', {})
        buy_depth = depth.get('buy', [])
        sell_depth = depth.get('sell', [])
        
        print("\nMarket Depth (Liquidity Check):")
        top_bid = buy_depth[0]['price'] if buy_depth else 0
        top_ask = sell_depth[0]['price'] if sell_depth else 0
        spread = top_ask - top_bid if top_bid and top_ask else 0
        spread_pct = (spread / top_bid * 100) if top_bid > 0 else 0
        
        print(f"Top Bid: ₹{top_bid} | Top Ask: ₹{top_ask}")
        print(f"Bid-Ask Spread: ₹{spread:.2f} ({spread_pct:.2f}%)")
        print("Top 5 Bids (Buyers):")
        for b in buy_depth[:5]:
            print(f"   ₹{b['price']} (Qty: {b['quantity']})")
            
        print("Top 5 Asks (Sellers):")
        for s in sell_depth[:5]:
            print(f"   ₹{s['price']} (Qty: {s['quantity']})")
            
    except Exception as e:
        print(f"Error checking option liquidity: {e}")

if __name__ == "__main__":
    analyze_vbl_option()
