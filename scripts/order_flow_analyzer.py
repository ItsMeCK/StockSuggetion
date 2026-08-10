import os
import sys
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

def analyze_depth(name, data):
    if not data:
        print(f"\nNo data found for {name}")
        return

    ltp = data.get('last_price', 0)
    vol = data.get('volume', 0)
    oi = data.get('oi', 0)
    total_buy = data.get('buy_quantity', 0)
    total_sell = data.get('sell_quantity', 0)
    
    depth = data.get('depth', {})
    bids = depth.get('buy', [])
    asks = depth.get('sell', [])
    
    print(f"\n=================== {name} ===================")
    print(f"LTP: {ltp} | Volume: {vol} | OI: {oi}")
    ratio_str = f"{total_buy:,} vs {total_sell:,}"
    ratio = total_buy / total_sell if total_sell else 0
    print(f"Total Buy vs Sell Qty: {ratio_str} (Ratio: {ratio:.2f}x)")
    
    # Analyze Bids (Buy side)
    print("\n[BUY DEPTH (BIDS)]")
    avg_buy_qty = sum(b.get('quantity', 0) for b in bids) / len(bids) if bids else 1
    for b in bids:
        p = b.get('price', 0)
        q = b.get('quantity', 0)
        o = b.get('orders', 0)
        avg_per_order = q / o if o else 0
        
        flags = []
        if q >= avg_buy_qty * 3 and q > 1000:
            flags.append("🚨 LARGE WALL")
        if avg_per_order >= 1000:
            flags.append("🐳 INSTITUTIONAL ICEBERG")
            
        flag_str = " | ".join(flags)
        flag_str = f" <-- {flag_str}" if flag_str else ""
        print(f"  Price: ₹{p:<7} | Qty: {q:<6} | Orders: {o:<3} | Avg/Order: {avg_per_order:<5.0f} {flag_str}")
        
    # Analyze Asks (Sell side)
    print("\n[SELL DEPTH (ASKS)]")
    avg_sell_qty = sum(s.get('quantity', 0) for s in asks) / len(asks) if asks else 1
    for s in asks:
        p = s.get('price', 0)
        q = s.get('quantity', 0)
        o = s.get('orders', 0)
        avg_per_order = q / o if o else 0
        
        flags = []
        if q >= avg_sell_qty * 3 and q > 1000:
            flags.append("🚨 LARGE ASK WALL")
        if avg_per_order >= 1000:
            flags.append("🐳 INSTITUTIONAL BLOCK ASK")
            
        flag_str = " | ".join(flags)
        flag_str = f" <-- {flag_str}" if flag_str else ""
        print(f"  Price: ₹{p:<7} | Qty: {q:<6} | Orders: {o:<3} | Avg/Order: {avg_per_order:<5.0f} {flag_str}")
        
    # Smart "Read Between the Lines" Conclusion
    print("\n[SMART TAPE DIAGNOSIS]")
    if ratio > 1.5:
        print("  - Macro Book: Strongly BULLISH (bids outnumber asks significantly).")
    elif ratio < 0.67:
        print("  - Macro Book: Strongly BEARISH (asks outnumber bids significantly).")
    else:
        print("  - Macro Book: Balanced / Consolidation.")
        
    # Find gaps/liquidity pockets
    if len(bids) >= 2:
        max_gap = 0
        for i in range(len(bids) - 1):
            gap = abs(bids[i]['price'] - bids[i+1]['price'])
            if gap > max_gap:
                max_gap = gap
        # If gap is wider than 0.1% of LTP, it's a pocket
        if max_gap > ltp * 0.001:
            print(f"  - ⚠️ LIQUIDITY VACUUM: Detected price gaps of ₹{max_gap:.2f} in buy depth. High slippage risk.")

def main():
    # Targets: open positions and current active candidates
    # We dynamically query their active ATM option strikes if we know them
    targets = [
        'NSE:RBLBANK', 'NFO:RBLBANK26JUL380CE',
        'NSE:HDFCBANK', 'NFO:HDFCBANK26JUL810CE', 'NFO:HDFCBANK26JUL820CE',
        'NSE:BHARTIARTL', 'NFO:BHARTIARTL26JUL1940CE',
        'NSE:NTPC', 'NFO:NTPC26JUL345PE', 'NFO:NTPC26JUL350PE'
    ]
    
    print("Connecting to Kite Connect...")
    kite = get_kite_client()
    
    print(f"Querying order flow depth for {len(targets)} instruments...")
    try:
        quotes = kite.quote(targets)
    except Exception as e:
        print(f"Error fetching quotes: {e}")
        sys.exit(1)
        
    for name in targets:
        analyze_depth(name, quotes.get(name))

if __name__ == "__main__":
    main()
