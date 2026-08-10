import os
import sys
import csv
from dotenv import load_dotenv
from kiteconnect import KiteConnect

# Ensure current directory is in path
sys.path.append(os.getcwd())
load_dotenv()

def get_kite_client():
    api_key = os.getenv('KITE_API_KEY')
    access_token = os.getenv('KITE_ACCESS_TOKEN')
    if not api_key or not access_token:
        print("Error: KITE_API_KEY or KITE_ACCESS_TOKEN missing from environment.")
        sys.exit(1)
    
    # Strip quotes if any
    api_key = api_key.strip("'\"")
    access_token = access_token.strip("'\"")
    
    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)
    return kite

def main():
    # Load F&O universe
    fo_symbols = []
    try:
        with open('pipeline/fo_universe.csv', 'r') as f:
            reader = csv.DictReader(f)
            fo_symbols = [row['Symbol'] for row in reader if row.get('Symbol')]
    except FileNotFoundError:
        print("Error: pipeline/fo_universe.csv not found.")
        sys.exit(1)

    print("Connecting to Kite Connect...")
    kite = get_kite_client()

    print(f"Fetching live quotes for {len(fo_symbols)} F&O underlyings...")
    keys = [f"NSE:{s}" for s in fo_symbols]
    
    # Chunk queries because of API limits (max 500 per query, we have ~217, so 1 chunk is fine, but chunking at 100 for safety)
    q = {}
    CHUNK = 100
    try:
        for i in range(0, len(keys), CHUNK):
            chunk_q = kite.quote(keys[i:i+CHUNK])
            q.update(chunk_q)
    except Exception as e:
        print(f"Error querying quotes: {e}")
        sys.exit(1)

    leaders = []
    laggards = []

    for sym in fo_symbols:
        k = f"NSE:{sym}"
        if k in q:
            data = q[k]
            ohlc = data.get('ohlc', {})
            prev_close = ohlc.get('close', 1.0)
            open_p = ohlc.get('open', 1.0)
            ltp = data.get('last_price', 0.0)
            buy_qty = data.get('buy_quantity', 0)
            sell_qty = data.get('sell_quantity', 0)
            
            if not prev_close or not open_p:
                continue
                
            change = (ltp - prev_close) / prev_close * 100
            open_to_ltp = (ltp - open_p) / open_p * 100
            ratio = buy_qty / sell_qty if sell_qty else 1.0
            
            # Check for strong long breakout intraday
            if change >= 2.0 and open_to_ltp > 0.5 and ratio >= 1.25:
                leaders.append({
                    'symbol': sym,
                    'ltp': ltp,
                    'change': change,
                    'open_to_ltp': open_to_ltp,
                    'ratio': ratio,
                    'volume': data.get('volume', 0)
                })
                
            # Check for strong short breakdown intraday
            if change <= -2.0 and open_to_ltp < -0.5 and ratio <= 0.8:
                laggards.append({
                    'symbol': sym,
                    'ltp': ltp,
                    'change': change,
                    'open_to_ltp': open_to_ltp,
                    'ratio': ratio,
                    'volume': data.get('volume', 0)
                })

    # Sort leaders and laggards
    leaders = sorted(leaders, key=lambda x: x['change'], reverse=True)
    laggards = sorted(laggards, key=lambda x: x['change'])

    print("\n" + "="*80)
    print("LIVE INTRADAY BREAKOUTS (CE CANDIDATES)")
    print("Criteria: Change >= +2%, Open-to-LTP > +0.5%, Order Ratio >= 1.25x")
    print("="*80)
    if not leaders:
        print("No active candidates meet the long breakout criteria right now.")
    for l in leaders[:5]:
        print(f"Stock: {l['symbol']:12s} | Price: ₹{l['ltp']:<7.2f} | Change: {l['change']:+.2f}% | Open-to-LTP: {l['open_to_ltp']:+.2f}% | Order Book: {l['ratio']:.2f}x | Vol: {l['volume']:,}")

    print("\n" + "="*80)
    print("LIVE INTRADAY BREAKDOWNS (PE CANDIDATES)")
    print("Criteria: Change <= -2%, Open-to-LTP < -0.5%, Order Ratio <= 0.8x")
    print("="*80)
    if not laggards:
        print("No active candidates meet the short breakdown criteria right now.")
    for l in laggards[:5]:
        print(f"Stock: {l['symbol']:12s} | Price: ₹{l['ltp']:<7.2f} | Change: {l['change']:+.2f}% | Open-to-LTP: {l['open_to_ltp']:+.2f}% | Order Book: {l['ratio']:.2f}x | Vol: {l['volume']:,}")

if __name__ == "__main__":
    main()
