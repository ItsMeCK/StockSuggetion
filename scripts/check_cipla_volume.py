"""
Queries the database and Kite API to analyze the current live volume and volume ratios
for CIPLA and UNOMINDA to verify if they have genuine breakout momentum.
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
        print(f"DB Error: {e}")
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
        
        symbols = ["NSE:CIPLA", "NSE:UNOMINDA"]
        quotes = kite.quote(symbols)
        
        print("\n=========================================================================")
        print("MOMENTUM & VOLUME AUDIT FOR NEW ENTRIES")
        print("=========================================================================")
        
        for sym in symbols:
            q = quotes.get(sym)
            if not q:
                continue
                
            name = sym.split(":")[1]
            ltp = q["last_price"]
            open_p = q["ohlc"]["open"]
            prev_close = q["ohlc"]["close"]
            
            # Live volume traded so far today
            live_vol = q["volume"]
            
            # DB 20-day average daily volume
            db_avg_vol = get_db_20d_volume(name)
            
            # How much of the average daily volume has been done in 2.5 hours
            # Market is open from 9:15 to 15:30 (375 minutes).
            # It has been open for 165 minutes (~44% of the trading day).
            vol_ratio_live = live_vol / db_avg_vol
            
            # Buy/sell pressure
            total_buy = q["buy_quantity"]
            total_sell = q["sell_quantity"]
            depth_ratio = total_buy / total_sell if total_sell > 0 else 1.0
            
            # Average volume traded per minute
            print(f"Stock: {name:12s} | Price: ₹{ltp:.2f} ({ (ltp-prev_close)/prev_close*100:+.2f}% vs Fri Close)")
            print(f"  Live Volume Today:     {live_vol:,} shares")
            print(f"  20-Day Avg Daily Vol:  {int(db_avg_vol):,} shares")
            print(f"  Current Volume Ratio:  {vol_ratio_live:.2f}x of avg daily volume (44% of day completed)")
            print(f"  Order Depth Pressure:  Bids: {total_buy:,} vs Asks: {total_sell:,} (Ratio: {depth_ratio:.2f}x)")
            
            # Momentum assessment
            has_momentum = "NO"
            if vol_ratio_live >= 0.6 and depth_ratio > 1.2:
                has_momentum = "YES - High Institutional Volume"
            elif vol_ratio_live < 0.3:
                has_momentum = "NO - Extremely Thin Volume / Dry breakout"
                
            print(f"  👉 MOMENTUM VERDICT:  {has_momentum}")
            print("-" * 73)
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
