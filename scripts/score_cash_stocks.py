import os
import sys
import math
import psycopg2
from datetime import date

sys.path.append(os.getcwd())

def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )

def main():
    conn = get_db_conn()
    cur = conn.cursor()
    
    symbols = ['ASTERDM', 'KARURVYSYA', 'USHAMART', 'ACUTAAS', 'IDBI']
    target_date = date(2026, 7, 9)
    
    # Fetch historical stock prices
    cur.execute("""
        SELECT symbol, time::date as date, open, high, low, close, volume 
        FROM daily_ohlcv 
        WHERE symbol = ANY(%s) AND time::date <= %s
        ORDER BY symbol, time
    """, (symbols, target_date))
    rows = cur.fetchall()
    
    ohlcv_by_symbol = {}
    for r in rows:
        sym, dt, op, hi, lo, cl, vol = r
        ohlcv_by_symbol.setdefault(sym, []).append({
            "date": dt, "open": float(op), "high": float(hi), "low": float(lo),
            "close": float(cl), "volume": int(vol)
        })
        
    print(f"Loaded price history for {len(ohlcv_by_symbol)} symbols.")
    
    for sym in symbols:
        prices = ohlcv_by_symbol.get(sym)
        if not prices or len(prices) < 50:
            print(f"\n--- {sym} ---")
            print("  Insufficient historical data (requires at least 50 trading days).")
            continue
            
        # Find index of target_date in price history
        target_idx = next((idx for idx, p in enumerate(prices) if p["date"] == target_date), None)
        if target_idx is None:
            print(f"\n--- {sym} ---")
            print(f"  No price data for date {target_date}.")
            continue
            
        p_day = prices[target_idx]
        p_prev = prices[target_idx - 1]
        
        spot_price = p_day["close"]
        prev_close = p_prev["close"]
        
        # Calculate SMAs
        closes_50 = [r["close"] for r in prices[target_idx-49:target_idx+1]]
        sma50 = sum(closes_50) / 50.0
        closes_20 = [r["close"] for r in prices[target_idx-19:target_idx+1]]
        sma20 = sum(closes_20) / 20.0
        
        # Bollinger Squeeze Index (CI)
        std_dev20 = math.sqrt(sum((c - sma20)**2 for c in closes_20) / 19.0)
        bbw = (4.0 * std_dev20) / sma20 if sma20 > 0 else 0.0
        
        prior_slice = prices[max(0, target_idx-39):target_idx+1]
        prior_bbw = []
        for j in range(len(prior_slice)):
            if j < 19:
                continue
            sub_closes = [r["close"] for r in prior_slice[j-19:j+1]]
            sub_sma = sum(sub_closes) / 20.0
            sub_var = sum((c - sub_sma) ** 2 for c in sub_closes) / 19.0
            sub_bbw = (4.0 * math.sqrt(sub_var)) / sub_sma if sub_sma > 0 else 0.0
            prior_bbw.append(sub_bbw)
            
        min_bbw = min(prior_bbw)
        max_bbw = max(prior_bbw)
        ci = (bbw - min_bbw) / (max_bbw - min_bbw) if (max_bbw - min_bbw) > 0 else 0.5
        
        # Volume ratio
        vol_avg20 = sum(r["volume"] for r in prices[target_idx-20:target_idx]) / 20.0
        vol_ratio = p_day["volume"] / vol_avg20 if vol_avg20 > 0 else 1.0
        
        # ROC and Velocity
        close_14ago = prices[target_idx-14]["close"]
        roc_t = (spot_price - close_14ago) / close_14ago * 100 if close_14ago > 0 else 0.0
        close_15ago = prices[target_idx-15]["close"]
        roc_prev = (prev_close - close_15ago) / close_15ago * 100 if close_15ago > 0 else 0.0
        velocity = roc_t - roc_prev
        
        # Daily range position
        tr_range = p_day["high"] - p_day["low"]
        close_pos = (p_day["close"] - p_day["low"]) / tr_range if tr_range > 0 else 0.5
        
        # Print diagnostic report
        print(f"\n--- {sym} ---")
        print(f"  LTP (July 9): ₹{spot_price:.2f} ({((spot_price-prev_close)/prev_close*100):+.2f}%)")
        print(f"  Trend Stack: 20-SMA = ₹{sma20:.2f} | 50-SMA = ₹{sma50:.2f}")
        
        # Evaluate setup
        score = 50.0 # baseline neutral
        checks = []
        
        # Trend check
        if spot_price > sma20 and sma20 > sma50:
            score += 15
            checks.append("Bullish Trend Stack (+15)")
        elif spot_price < sma20 and sma20 < sma50:
            score -= 15
            checks.append("Bearish Trend Stack (-15)")
        else:
            checks.append("No Trend Stack Alignment (+0)")
            
        # Squeeze check
        if ci <= 0.35:
            score += 15
            checks.append(f"Volatility Compression Squeeze (CI={ci:.2f}) (+15)")
        elif ci >= 0.75:
            score -= 10
            checks.append(f"Extended/Overbought Release (CI={ci:.2f}) (-10)")
        else:
            checks.append(f"Neutral Compression Range (CI={ci:.2f}) (+0)")
            
        # Volume check
        if vol_ratio >= 1.5:
            score += 15
            checks.append(f"High Volume Expansion (VolRatio={vol_ratio:.2f}x) (+15)")
        elif vol_ratio <= 0.6:
            score -= 10
            checks.append(f"Low Volume consolidation (VolRatio={vol_ratio:.2f}x) (-10)")
        else:
            checks.append(f"Neutral Volume (VolRatio={vol_ratio:.2f}x) (+0)")
            
        # Momentum check
        if roc_t > 0 and velocity > 0:
            score += 10
            checks.append(f"Positive Momentum & Velocity (ROC={roc_t:.1f}%, Vel={velocity:.2f}) (+10)")
        elif roc_t < 0 and velocity < 0:
            score -= 10
            checks.append(f"Negative Momentum & Velocity (ROC={roc_t:.1f}%, Vel={velocity:.2f}) (-10)")
            
        # Close position
        if close_pos >= 0.70:
            score += 10
            checks.append(f"Closed Strong at Daily Highs (ClosePos={close_pos:.1%}) (+10)")
        elif close_pos <= 0.30:
            score -= 10
            checks.append(f"Closed Weak at Daily Lows (ClosePos={close_pos:.1%}) (-10)")
            
        # Cap score between 0 and 100
        score = max(0.0, min(100.0, score))
        print(f"  Final Score: {score:.1f}/100")
        print("  Scoring Logic:")
        for c in checks:
            print(f"    * {c}")
            
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
