import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

def check_options():
    api_key = os.getenv("KITE_API_KEY").strip("'\"")
    access_token = os.getenv("KITE_ACCESS_TOKEN").strip("'\"")
    
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token)
        
        print("Downloading NFO instruments from Zerodha...")
        instruments = kite.instruments(exchange="NFO")
        
        candidates = ['HONAUT', 'ANURAS', 'SAMMAANCAP', 'PAGEIND', 'WIPRO', 'VBL']
        
        results = {}
        
        for cand in candidates:
            # Find all options for this underlying
            # In Zerodha, the name field for NFO is usually the underlying symbol
            # For example, name="WIPRO"
            fno_insts = [i for i in instruments if i['name'] == cand]
            
            if not fno_insts:
                results[cand] = "No Options Available (Not in F&O)"
                continue
                
            # It has options! Let's find the lot size and nearest expiry
            lot_size = fno_insts[0]['lot_size']
            
            # Find unique expiries
            expiries = sorted(list(set([i['expiry'] for i in fno_insts if i['instrument_type'] in ['CE', 'PE']])))
            if expiries:
                nearest_expiry = expiries[0].strftime("%Y-%m-%d")
                results[cand] = f"F&O Enabled | Lot Size: {lot_size} | Nearest Expiry: {nearest_expiry}"
            else:
                results[cand] = "No CE/PE found"
                
        print("\n--- OPTIONS AVAILABILITY FOR 3 PM RUN ---")
        for cand, res in results.items():
            print(f"{cand.ljust(15)} : {res}")
            
    except Exception as e:
        print(f"Error checking options: {e}")

if __name__ == "__main__":
    check_options()
