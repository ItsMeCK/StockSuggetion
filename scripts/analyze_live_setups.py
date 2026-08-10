"""
Fetches live OHLC and market depth (buy/sell pressure) for the 4 underlying stocks
to evaluate if their breakout setups are still valid or showing signs of failure.
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
        
        symbols = ["NSE:MPHASIS", "NSE:CONCOR", "NSE:AMBUJACEM", "NSE:NHPC"]
        
        # Fetch full quotes containing depth and OHLC
        quotes = kite.quote(symbols)
        
        print("\n=========================================================================")
        print("LIVE UNDERLYING SETUP ANALYSIS - MONDAY MORNING")
        print("=========================================================================")
        
        for sym in symbols:
            q = quotes.get(sym)
            if not q:
                print(f"{sym}: Error fetching quote")
                continue
                
            name = sym.split(":")[1]
            ltp = q["last_price"]
            ohlc = q["ohlc"]
            prev_close = ohlc["close"]
            open_price = ohlc["open"]
            high_price = q["depth"]["buy"][0].get("price") or ltp # approximation if empty
            
            # Buy vs Sell depth pressure
            total_buy = q["buy_quantity"]
            total_sell = q["sell_quantity"]
            pressure_ratio = total_buy / total_sell if total_sell > 0 else 1.0
            
            # Performance relative to Friday Close & Monday Open
            vs_prev_close = (ltp - prev_close) / prev_close * 100
            vs_open = (ltp - open_price) / open_price * 100
            
            # Position inside the day's range
            day_range = q["upper_circuit_limit"] - q["lower_circuit_limit"] # or high - low
            day_high = q["ohlc"]["high"]
            day_low = q["ohlc"]["low"]
            day_range_act = day_high - day_low
            range_pos = (ltp - day_low) / day_range_act * 100 if day_range_act > 0 else 50.0
            
            print(f"Stock: {name:12s} | Price: ₹{ltp:7.2f} ({vs_prev_close:+.2f}% vs Fri Close)")
            print(f"  Monday Open:  ₹{open_price:.2f} (Current: {vs_open:+.2f}% vs Open)")
            print(f"  Day's Range:  ₹{day_low:.2f} - ₹{day_high:.2f} (LTP is at {range_pos:.1f}% of range)")
            print(f"  Order Depth:  Bids: {total_buy:,} vs Asks: {total_sell:,} | Ratio: {pressure_ratio:.2f}x")
            
            # Qualitative assessment
            verdict = "HOLD/NEUTRAL"
            if vs_prev_close > 0.5 and pressure_ratio > 1.2:
                verdict = "STRONG - Good to Enter"
            elif vs_prev_close < -1.5 or pressure_ratio < 0.7:
                verdict = "WEAK - Stay Out"
            elif range_pos > 60:
                verdict = "STRENGTHENING - Can Enter"
            elif range_pos < 30:
                verdict = "WEAKENING - Hold Off"
                
            print(f"  👉 VERDICT:   {verdict}")
            print("-" * 73)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
