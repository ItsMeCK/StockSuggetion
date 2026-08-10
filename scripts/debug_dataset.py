import os
import sys
sys.path.append(os.getcwd())
import scripts.optimize_oi_buildup as oo

def main():
    conn = oo.get_db_conn()
    cur = conn.cursor()
    
    # We will compute the dataset using the logic from optimize_oi_buildup
    cur.execute("SELECT DISTINCT underlying FROM option_oi_daily ORDER BY underlying")
    symbols = [r[0] for r in cur.fetchall()]
    
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
            "close": float(close), "oi": int(oi), "volume": int(volume), "tradingsymbol": trsym
        })

    option_premium_history = {}
    for sym, opt_list in options_by_symbol.items():
        for opt in opt_list:
            option_premium_history.setdefault(opt["tradingsymbol"], {})[opt["date"]] = opt["close"]

    dataset = []
    
    from datetime import date
    
    for sym in symbols[:30]: # just test first 30 for speed
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
                
            p_prev = prices[i-1]
            prev_d = p_prev["date"]
            if prev_d not in opt_by_date:
                continue
                
            day_opts = opt_by_date[d]
            prev_opts = opt_by_date[prev_d]
            
            ce_opts = [o for o in day_opts if o["option_type"] == 'CE']
            pe_opts = [o for o in day_opts if o["option_type"] == 'PE']
            if not ce_opts or not pe_opts:
                continue
                
            spot_price = p_day["close"]
            atm_ce = min(ce_opts, key=lambda o: abs(o["strike"] - spot_price))
            unique_strikes = sorted(list(set(o["strike"] for o in ce_opts)))
            atm_strike = min(unique_strikes, key=lambda s: abs(s - spot_price))
            atm_idx = unique_strikes.index(atm_strike)
            
            otm_ce_strikes = unique_strikes[atm_idx:min(len(unique_strikes), atm_idx+3)]
            
            ce_oi_now = sum(o["oi"] for o in ce_opts if o["strike"] in otm_ce_strikes)
            
            prev_ce_opts = [o for o in prev_opts if o["option_type"] == 'CE']
            ce_oi_prev = sum(o["oi"] for o in prev_ce_opts if o["strike"] in otm_ce_strikes)
            
            ce_oi_change = (ce_oi_now - ce_oi_prev) / ce_oi_prev * 100 if ce_oi_prev > 0 else 0.0
            
            # BBW/CI
            slice_20 = prices[i-19:i+1]
            closes_20 = [r["close"] for r in slice_20]
            sma20 = sum(closes_20) / 20.0
            std_dev = (sum((c - sma20)**2 for c in closes_20)/19.0)**0.5
            bbw = 4.0 * std_dev / sma20 if sma20 > 0 else 0.0
            
            prior_slice = prices[max(0, i-39):i+1]
            prior_bbw = []
            for j in range(len(prior_slice)):
                if j < 19:
                    continue
                sub_closes = [r["close"] for r in prior_slice[j-19:j+1]]
                sub_sma = sum(sub_closes) / 20.0
                sub_var = sum((c - sub_sma) ** 2 for c in sub_closes) / 19.0
                sub_bbw = (4.0 * (sub_var**0.5)) / sub_sma if sub_sma > 0 else 0.0
                prior_bbw.append(sub_bbw)
            min_bbw = min(prior_bbw)
            max_bbw = max(prior_bbw)
            ci = (bbw - min_bbw) / (max_bbw - min_bbw) if (max_bbw - min_bbw) > 0 else 0.5
            
            vol_avg20 = sum(r["volume"] for r in prices[i-20:i]) / 20.0
            vol_ratio = p_day["volume"] / vol_avg20 if vol_avg20 > 0 else 1.0
            
            future_dates = [prices[idx]["date"] for idx in range(i+1, min(len(prices), i+4))]
            
            # CE / PE contract max gain in next 3 days
            trsym_ce = atm_ce["tradingsymbol"]
            ce_prem_t = atm_ce["close"]
            
            # Find future dates in option_premium_history
            ce_future_prems = []
            for fd in future_dates:
                if trsym_ce in option_premium_history and fd in option_premium_history[trsym_ce]:
                    ce_future_prems.append(option_premium_history[trsym_ce][fd])
            ce_gain = (max(ce_future_prems) - ce_prem_t) / ce_prem_t * 100 if ce_future_prems and ce_prem_t > 0 else 0.0
            
            dataset.append({
                "symbol": sym, "date": d, "ci": ci, "ce_oi_change": ce_oi_change, "vol_ratio": vol_ratio, "ce_gain": ce_gain
            })
            
    print(f"Sample dataset points count: {len(dataset)}")
    if dataset:
        ci_vals = [r["ci"] for r in dataset]
        oi_vals = [r["ce_oi_change"] for r in dataset]
        vol_vals = [r["vol_ratio"] for r in dataset]
        ce_gains = [r["ce_gain"] for r in dataset]
        
        print("CI stats: min =", min(ci_vals), ", max =", max(ci_vals), ", avg =", sum(ci_vals)/len(ci_vals))
        print("OI Change stats: min =", min(oi_vals), ", max =", max(oi_vals), ", avg =", sum(oi_vals)/len(oi_vals))
        print("Vol Ratio stats: min =", min(vol_vals), ", max =", max(vol_vals), ", avg =", sum(vol_vals)/len(vol_vals))
        print("CE Gains: count >= 50% =", sum(1 for g in ce_gains if g >= 50.0), ", max =", max(ce_gains), ", avg =", sum(ce_gains)/len(ce_gains))
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
