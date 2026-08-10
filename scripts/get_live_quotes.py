"""
Fetches live market quotes for the Friday approved NFO options and underlying stocks,
and calculates the current real-time P&L as of Monday morning.
"""
import os
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

def main():
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    
    if not api_key or not access_token:
        print("Error: Kite API credentials missing in .env")
        return
        
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token.strip("'"))
        
        # 4 Option contracts and their entries/stops
        options = {
            "NFO:MPHASIS26JUL2300CE": {"symbol": "MPHASIS", "entry": 94.10, "stop": 56.45, "lot_size": 275},
            "NFO:CONCOR26JUL490CE": {"symbol": "CONCOR", "entry": 13.25, "stop": 7.95, "lot_size": 1250},
            "NFO:AMBUJACEM26JUL445CE": {"symbol": "AMBUJACEM", "entry": 13.50, "stop": 8.10, "lot_size": 1200},
            "NFO:NHPC26JUL81CE": {"symbol": "NHPC", "entry": 2.03, "stop": 1.20, "lot_size": 6950}
        }
        
        underlying = ["NSE:MPHASIS", "NSE:CONCOR", "NSE:AMBUJACEM", "NSE:NHPC"]
        
        # Fetch LTP for options and underlying
        instruments = list(options.keys()) + underlying
        ltp_data = kite.ltp(instruments)
        
        print("\n=========================================================================")
        # Print time of quote
        import datetime
        print(f"LIVE NFO OPTIONS PERFORMANCE - MONDAY {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=========================================================================")
        
        print(f"  {'Symbol':12s} | {'Underlying':10s} | {'Entry':7s} | {'Stop':6s} | {'Live LTP':8s} | {'PnL %':7s} | {'PnL ₹':9s}")
        print(f"  {'-'*81}")
        
        total_pnl_rs = 0.0
        total_cost = 0.0
        
        for inst, opt in options.items():
            symbol = opt["symbol"]
            entry = opt["entry"]
            stop = opt["stop"]
            lot_size = opt["lot_size"]
            cost = entry * lot_size
            total_cost += cost
            
            # Fetch LTPs
            opt_ltp = ltp_data.get(inst, {}).get("last_price")
            und_ltp = ltp_data.get(f"NSE:{symbol}", {}).get("last_price")
            
            if opt_ltp is None:
                print(f"  {symbol:12s} | Error fetching quote")
                continue
                
            pnl_pct = (opt_ltp - entry) / entry * 100
            pnl_rs = (opt_ltp - entry) * lot_size
            total_pnl_rs += pnl_rs
            
            # Check GTT Stop condition
            sl_hit_str = "⚠️ GTT SL HIT" if opt_ltp <= stop else "Active"
            
            print(f"  {symbol:12s} | {und_ltp:10.2f} | {entry:7.2f} | {stop:6.2f} | {opt_ltp:8.2f} | {pnl_pct:+6.2f}% | ₹{pnl_rs:+7.0f} | {sl_hit_str}")
            
        print(f"  {'-'*81}")
        print(f"  Total Capital Deployed: ₹{total_cost:,.2f}")
        print(f"  Total Unrealized PnL:   ₹{total_pnl_rs:+,.2f} ({total_pnl_rs/total_cost*100:+.2f}%)")
        print("=========================================================================\n")
        
    except Exception as e:
        print(f"Error fetching live quotes: {e}")

if __name__ == "__main__":
    main()
