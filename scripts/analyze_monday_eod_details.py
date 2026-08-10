"""
Fetches EOD metrics and option contract details for the top technical candidates
from today's screen to provide high-probability trading candidates for tomorrow.
"""
import os
import psycopg2
from dotenv import load_dotenv
from kiteconnect import KiteConnect

load_dotenv()

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data")
    )

def get_db_20d_volume(symbol):
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("""
            SELECT AVG(volume) FROM (
                SELECT volume FROM daily_ohlcv 
                WHERE symbol=%s 
                ORDER BY time DESC LIMIT 20
            ) t
        """, (symbol,))
        avg_vol = cur.fetchone()[0]
        cur.close()
        conn.close()
        return float(avg_vol) if avg_vol else 1.0
    except Exception as e:
        return 1.0

def main():
    api_key = os.getenv("KITE_API_KEY")
    access_token = os.getenv("KITE_ACCESS_TOKEN")
    
    if not api_key or not access_token:
        print("Error: Kite credentials missing")
        return
        
    try:
        kite = KiteConnect(api_key=api_key)
        kite.set_access_token(access_token.strip("'"))
        
        # Test NFO options availability and get EOD statistics for top 8 candidates + momentum breakouts
        stocks = [
            "SPLPETRO", "BRIGADE", "SOBHA", "SWANCORP", "AARTIIND", "JUBLINGREA", 
            "LODHA", "OBEROIRLTY", "HDFCBANK"
        ]
        
        instruments = [f"NSE:{s}" for s in stocks]
        quotes = kite.quote(instruments)
        
        # Fetch all NFO instruments to check if options exist
        print("Checking NFO contract availability...")
        all_nfo = kite.instruments("NFO")
        nfo_symbols = set(inst["name"] for inst in all_nfo if inst["segment"] == "NFO-OPT")
        
        print("\n=========================================================================")
        print("MONDAY EOD SCREEN CANDIDATES DETAILED TECHNICAL AUDIT (FOR TUESDAY)")
        print("=========================================================================")
        print(f"  {'Symbol':10s} | {'Price':8s} | {'Change%':7s} | {'Vol Ratio':9s} | {'Close%':6s} | {'Option Trading':14s}")
        print(f"  {'-'*71}")
        
        for s in stocks:
            q = quotes.get(f"NSE:{s}")
            if not q:
                continue
                
            ltp = q["last_price"]
            prev_close = q["ohlc"]["close"]
            high = q["ohlc"]["high"]
            low = q["ohlc"]["low"]
            volume = q["volume"]
            
            # Volume ratio compared to 20d average
            avg_vol = get_db_20d_volume(s)
            vol_ratio = volume / avg_vol
            
            # Close position inside the daily range
            day_range = high - low
            close_pos = (ltp - low) / day_range * 100 if day_range > 0 else 50.0
            
            # Change pct
            change_pct = (ltp - prev_close) / prev_close * 100
            
            # Check NFO Options
            has_options = s in nfo_symbols
            opt_status = "✅ NFO Option" if has_options else "❌ Equity Only"
            
            print(f"  {s:10s} | ₹{ltp:7.2f} | {change_pct:+6.2f}% | {vol_ratio:8.2f}x | {close_pos:5.1f}% | {opt_status:14s}")
            
        print(f"  {'-'*71}")
        print("  * Vol Ratio: Current EOD volume vs 20-day daily average volume.")
        print("  * Close%: How close the price settled to the top of today's range (100% = High of Day).")
        print("=========================================================================\n")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
