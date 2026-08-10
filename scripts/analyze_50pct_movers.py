import os
import csv
import math
import psycopg2
from datetime import datetime, date, timedelta

# --- Pure-Python Black-Scholes Greeks Solver ---
def norm_pdf(x):
    return math.exp(-x*x/2.0) / math.sqrt(2.0 * math.pi)

def norm_cdf(x):
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))

def bs_price(S, K, T, r, sigma, opt_type):
    if T <= 0:
        return max(0.0, S - K) if opt_type == 'CE' else max(0.0, K - S)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    if opt_type == 'CE':
        return S * norm_cdf(d1) - K * math.exp(-r * T) * norm_cdf(d2)
    else:
        return K * math.exp(-r * T) * norm_cdf(-d2) - S * norm_cdf(-d1)

def bs_gamma(S, K, T, r, sigma):
    if T <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return 0.0
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    return norm_pdf(d1) / (S * sigma * math.sqrt(T))

def solve_iv(market_price, S, K, T, r, opt_type):
    if market_price <= 0.01:
        return 0.0
    low_iv, high_iv = 0.001, 3.0
    intrinsic = max(0.0, S - K) if opt_type == 'CE' else max(0.0, K - S)
    if market_price <= intrinsic:
        return 0.01
    for _ in range(24):
        mid_iv = (low_iv + high_iv) / 2.0
        p = bs_price(S, K, T, r, mid_iv, opt_type)
        if abs(p - market_price) < 1e-3:
            return mid_iv
        if p < market_price:
            low_iv = mid_iv
        else:
            high_iv = mid_iv
    return (low_iv + high_iv) / 2.0

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

    # Load option underlyings
    cur.execute("SELECT DISTINCT underlying FROM option_oi_daily ORDER BY underlying")
    symbols = [r[0] for r in cur.fetchall()]
    print(f"Loaded {len(symbols)} symbols.")

    # Fetch daily OHLCV prices (May 1st to July 8th)
    cur.execute("""
        SELECT symbol, time::date as date, open, high, low, close, volume 
        FROM daily_ohlcv 
        WHERE time::date BETWEEN '2026-05-01' AND '2026-07-08'
        ORDER BY symbol, time
    """)
    ohlcv_rows = cur.fetchall()
    
    ohlcv_by_symbol = {}
    for r in ohlcv_rows:
        sym, dt, op, hi, lo, cl, vol = r
        ohlcv_by_symbol.setdefault(sym, []).append({
            "date": dt, "open": float(op), "high": float(hi), "low": float(lo),
            "close": float(cl), "volume": int(vol)
        })

    # Fetch daily options records from July 1st to July 8th
    cur.execute("""
        SELECT date, underlying, strike, option_type, expiry, close, oi, volume, spot, tradingsymbol 
        FROM option_oi_daily 
        WHERE date BETWEEN '2026-07-01' AND '2026-07-08'
        ORDER BY underlying, date
    """)
    option_rows = cur.fetchall()
    
    options_by_symbol = {}
    for r in option_rows:
        dt, sym, strike, opt_type, expiry, close, oi, volume, spot, trsym = r
        if None in (strike, close, oi, volume):
            continue
        options_by_symbol.setdefault(sym, []).append({
            "date": dt, "strike": float(strike), "option_type": opt_type, "expiry": expiry,
            "close": float(close), "oi": int(oi), "volume": int(volume), "spot": float(spot) if spot is not None else 0.0,
            "tradingsymbol": trsym
        })

    # Find the big movers! Option contracts that moved 50%+ in premium between July 1 and July 7 (measured over next 3 days)
    # We map options by tradingsymbol and date to trace their premiums
    option_premium_history = {}
    for sym, opt_list in options_by_symbol.items():
        for opt in opt_list:
            option_premium_history.setdefault(opt["tradingsymbol"], {})[opt["date"]] = opt["close"]

    big_movers = []
    
    eval_dates = sorted(list(set(r["date"] for sym in options_by_symbol for r in options_by_symbol[sym])))
    # Limit evaluation dates from July 1 to July 7
    eval_dates = [d for d in eval_dates if d <= date(2026, 7, 7)]
    print(f"Evaluating options for dates: {[d.isoformat() for d in eval_dates]}")

    for sym in symbols:
        prices = ohlcv_by_symbol.get(sym)
        opt_data = options_by_symbol.get(sym)
        if not prices or not opt_data:
            continue

        opt_by_date = {}
        for row in opt_data:
            opt_by_date.setdefault(row["date"], []).append(row)

        for i in range(len(prices)):
            p_day = prices[i]
            d = p_day["date"]
            if d not in opt_by_date or d > date(2026, 7, 7):
                continue
            if i < 21:
                continue

            # Check if any ATM options on this day moved 50%+ in the next 3 trading days
            day_opts = opt_by_date[d]
            spot_price = p_day["close"]

            # Calculate ATM strikes
            ce_opts = [o for o in day_opts if o["option_type"] == 'CE']
            pe_opts = [o for o in day_opts if o["option_type"] == 'PE']
            if not ce_opts or not pe_opts:
                continue

            unique_strikes = sorted(list(set(o["strike"] for o in ce_opts)))
            atm_strike = min(unique_strikes, key=lambda s: abs(s - spot_price))
            
            # ATM +/- 1 strike
            atm_idx = unique_strikes.index(atm_strike)
            lo_idx, hi_idx = max(0, atm_idx - 1), min(len(unique_strikes) - 1, atm_idx + 1)
            selected_strikes = unique_strikes[lo_idx:hi_idx+1]

            atm_contracts = [o for o in day_opts if o["strike"] in selected_strikes]
            
            future_dates = [prices[idx]["date"] for idx in range(i+1, min(len(prices), i+4))]
            if not future_dates:
                continue

            for opt in atm_contracts:
                trsym = opt["tradingsymbol"]
                premium_t = opt["close"]
                if premium_t <= 1.0: # skip extremely cheap/illiquid options
                    continue

                # Find max future close premium in the next 3 days
                future_prems = []
                for fd in future_dates:
                    if fd in option_premium_history.get(trsym, {}):
                        future_prems.append(option_premium_history[trsym][fd])
                
                if not future_prems:
                    continue

                max_future_prem = max(future_prems)
                prem_gain_pct = (max_future_prem - premium_t) / premium_t * 100

                # If it moved 50%+, record it!
                if prem_gain_pct >= 50.0:
                    # Let's compute indicator values on day D
                    # 1. BBW & CI
                    slice_20 = prices[i-19:i+1]
                    closes_20 = [r["close"] for r in slice_20]
                    sma20 = sum(closes_20) / 20.0
                    std_dev = math.sqrt(sum((c - sma20)**2 for c in closes_20) / 19.0)
                    bbw = (4.0 * std_dev) / sma20 if sma20 > 0 else 0.0

                    prev_slice_20 = prices[i-20:i]
                    prev_closes_20 = [r["close"] for r in prev_slice_20]
                    prev_sma20 = sum(prev_closes_20) / 20.0
                    prev_std_dev = math.sqrt(sum((c - prev_sma20)**2 for c in prev_closes_20) / 19.0)
                    bbw_prev = (4.0 * prev_std_dev) / prev_sma20 if prev_sma20 > 0 else 0.0

                    prior_slice = prices[max(0, i-39):i+1]
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

                    # 2. Volume Ratio
                    vol_avg20 = sum(r["volume"] for r in prices[i-20:i]) / 20.0
                    vol_ratio = p_day["volume"] / vol_avg20 if vol_avg20 > 0 else 1.0

                    # 3. Pring Momentum ROC Velocity & Acceleration
                    close_t = p_day["close"]
                    close_14ago = prices[i-14]["close"]
                    roc_t = (close_t - close_14ago) / close_14ago * 100 if close_14ago > 0 else 0.0
                    p_prev = prices[i-1]
                    close_prev = p_prev["close"]
                    close_15ago = prices[i-15]["close"]
                    roc_prev = (close_prev - close_15ago) / close_15ago * 100 if close_15ago > 0 else 0.0
                    p_prev2 = prices[i-2]
                    close_prev2 = p_prev2["close"]
                    close_16ago = prices[i-16]["close"]
                    roc_prev2 = (close_prev2 - close_16ago) / close_16ago * 100 if close_16ago > 0 else 0.0
                    velocity = roc_t - roc_prev
                    prev_velocity = roc_prev - roc_prev2
                    acceleration = velocity - prev_velocity

                    # 4. PCR & PCR_prev
                    def get_pcr_local(opts, spot):
                        ce = [c for c in opts if c["option_type"] == 'CE']
                        pe = [c for c in opts if c["option_type"] == 'PE']
                        if not ce or not pe:
                            return 1.0
                        strikes = sorted(list(set(c["strike"] for c in ce)))
                        atm = min(strikes, key=lambda s: abs(s - spot))
                        idx = strikes.index(atm)
                        lo, hi = max(0, idx - 3), min(len(strikes) - 1, idx + 3)
                        sel = strikes[lo:hi+1]
                        ce_f = [c for c in ce if c["strike"] in sel]
                        pe_f = [c for c in pe if c["strike"] in sel]
                        ce_oi = sum(c["oi"] for c in ce_f)
                        pe_oi = sum(c["oi"] for c in pe_f)
                        return pe_oi / ce_oi if ce_oi > 0 else 1.0

                    pcr_t = get_pcr_local(day_opts, spot_price)
                    pcr_prev_val = pcr_t
                    prev_d = p_prev["date"]
                    if prev_d in opt_by_date:
                        pcr_prev_val = get_pcr_local(opt_by_date[prev_d], p_prev["close"])

                    # 5. GEX crores
                    net_gex = 0.0
                    r_rate = 0.07
                    # Find lot size
                    lot_size = 100 # Default
                    
                    ce_filtered = [c for c in ce_opts if c["strike"] in selected_strikes]
                    pe_filtered = [c for c in pe_opts if c["strike"] in selected_strikes]
                    for opt_c in ce_filtered + pe_filtered:
                        K = opt_c["strike"]
                        T = (opt_c["expiry"] - d).days / 365.0
                        opt_type = opt_c["option_type"]
                        premium = opt_c["close"]
                        oi = opt_c["oi"]
                        iv = solve_iv(premium, spot_price, K, T, r_rate, opt_type)
                        gamma = bs_gamma(spot_price, K, T, r_rate, iv)
                        gex_val = gamma * oi * spot_price * lot_size
                        if opt_type == 'PE':
                            gex_val *= -1
                        net_gex += gex_val
                    gex_crores = net_gex / 10000000.0

                    # Underlyings return in next 3 days
                    future_stock_prices = prices[i+1:i+4]
                    max_stock_high = max(p["high"] for p in future_stock_prices)
                    min_stock_low = min(p["low"] for p in future_stock_prices)
                    stock_high_ret = (max_stock_high - close_t) / close_t * 100
                    stock_low_ret = (min_stock_low - close_t) / close_t * 100

                    big_movers.append({
                        "symbol": sym, "date": d, "tradingsymbol": trsym, "option_type": opt["option_type"],
                        "premium_entry": premium_t, "premium_max": max_future_prem, "gain_pct": prem_gain_pct,
                        "ci": ci, "bbw": bbw, "bbw_prev": bbw_prev, "vol_ratio": vol_ratio,
                        "velocity": velocity, "acceleration": acceleration,
                        "pcr": pcr_t, "pcr_prev": pcr_prev_val, "gex_crores": gex_crores,
                        "stock_high_ret": stock_high_ret, "stock_low_ret": stock_low_ret
                    })

    print(f"\nFound {len(big_movers)} F&O option contracts that moved 50%+ in premium.")
    
    # Analyze common patterns among big movers
    print("\n--- STATISTICAL ANALYSIS OF 50%+ MOVERS ---")
    ce_movers = [m for m in big_movers if m["option_type"] == 'CE']
    pe_movers = [m for m in big_movers if m["option_type"] == 'PE']
    
    print(f"CE Movers: {len(ce_movers)}, PE Movers: {len(pe_movers)}")

    def print_stats(movers, label):
        if not movers:
            print(f"No movers for {label}")
            return
        avg_ci = sum(m["ci"] for m in movers) / len(movers)
        avg_vol = sum(m["vol_ratio"] for m in movers) / len(movers)
        avg_vel = sum(m["velocity"] for m in movers) / len(movers)
        avg_acc = sum(m["acceleration"] for m in movers) / len(movers)
        avg_gex = sum(m["gex_crores"] for m in movers) / len(movers)
        
        ci_lt_30 = sum(1 for m in movers if m["ci"] <= 0.30) / len(movers) * 100
        vol_gt_1 = sum(1 for m in movers if m["vol_ratio"] >= 1.0) / len(movers) * 100
        bbw_expanding = sum(1 for m in movers if m["bbw"] > m["bbw_prev"]) / len(movers) * 100
        
        print(f"\n{label} Metrics:")
        print(f"  Average Compression Index (CI): {avg_ci:.3f} (Squeezed <= 0.30: {ci_lt_30:.1f}%)")
        print(f"  Average Volume Ratio: {avg_vol:.2f} (Expanding >= 1.0: {vol_gt_1:.1f}%)")
        print(f"  Average ROC Velocity: {avg_vel:.3f}")
        print(f"  Average ROC Acceleration: {avg_acc:.3f}")
        print(f"  Average GEX Crores: {avg_gex:.2f}")
        print(f"  Bollinger Bands Expanding: {bbw_expanding:.1f}%")
        
        # Display sample movers
        print("  Sample Movers:")
        for m in movers[:5]:
            print(f"    - {m['tradingsymbol']} on {m['date'].isoformat()} -> Premium {m['premium_entry']:.2f} to {m['premium_max']:.2f} (+{m['gain_pct']:.1f}%) | CI: {m['ci']:.2f}, Vol: {m['vol_ratio']:.2f}, Vel: {m['velocity']:.3f}, GEX: {m['gex_crores']:.2f}")

    print_stats(ce_movers, "CE CALL OPTIONS MOVERS")
    print_stats(pe_movers, "PE PUT OPTIONS MOVERS")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
