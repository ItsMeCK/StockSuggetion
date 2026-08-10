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
            "close": float(close), "oi": int(oi), "volume": int(volume), "spot": float(spot) if spot is not None else 0.0,
            "tradingsymbol": trsym
        })

    option_premium_history = {}
    for sym, opt_list in options_by_symbol.items():
        for opt in opt_list:
            option_premium_history.setdefault(opt["tradingsymbol"], {})[opt["date"]] = opt["close"]

    # Gather data points
    dataset = []

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

            day_opts = opt_by_date[d]
            spot_price = p_day["close"]

            ce_opts = [o for o in day_opts if o["option_type"] == 'CE']
            pe_opts = [o for o in day_opts if o["option_type"] == 'PE']
            if not ce_opts or not pe_opts:
                continue

            unique_strikes = sorted(list(set(o["strike"] for o in ce_opts)))
            atm_strike = min(unique_strikes, key=lambda s: abs(s - spot_price))
            
            # Select ATM contract
            atm_ce = min(ce_opts, key=lambda o: abs(o["strike"] - spot_price))
            atm_pe = min(pe_opts, key=lambda o: abs(o["strike"] - spot_price))
            
            future_dates = [prices[idx]["date"] for idx in range(i+1, min(len(prices), i+4))]
            if not future_dates:
                continue

            # Calculate indicators on day D
            # 1. CI & BBW
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

            # 2. Volume ratio
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
            lot_size = 100
            
            # Use ATM +/- 3 selected strikes for GEX
            atm_idx = unique_strikes.index(atm_strike)
            lo_idx, hi_idx = max(0, atm_idx - 3), min(len(unique_strikes) - 1, atm_idx + 3)
            selected_strikes = unique_strikes[lo_idx:hi_idx+1]
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

            # Find performance of ATM CE option contract
            trsym_ce = atm_ce["tradingsymbol"]
            ce_prem_t = atm_ce["close"]
            ce_future_prems = [option_premium_history[trsym_ce][fd] for fd in future_dates if fd in option_premium_history.get(trsym_ce, {})]
            ce_gain = (max(ce_future_prems) - ce_prem_t) / ce_prem_t * 100 if ce_future_prems and ce_prem_t > 0 else 0.0

            # Find performance of ATM PE option contract
            trsym_pe = atm_pe["tradingsymbol"]
            pe_prem_t = atm_pe["close"]
            pe_future_prems = [option_premium_history[trsym_pe][fd] for fd in future_dates if fd in option_premium_history.get(trsym_pe, {})]
            pe_gain = (max(pe_future_prems) - pe_prem_t) / pe_prem_t * 100 if pe_future_prems and pe_prem_t > 0 else 0.0

            # Check trigger day close position relative to daily range
            tr_range = p_day["high"] - p_day["low"]
            close_pos = (p_day["close"] - p_day["low"]) / tr_range if tr_range > 0 else 0.5

            dataset.append({
                "symbol": sym, "date": d,
                "ci": ci, "bbw": bbw, "bbw_prev": bbw_prev, "vol_ratio": vol_ratio,
                "velocity": velocity, "acceleration": acceleration,
                "pcr": pcr_t, "pcr_prev": pcr_prev_val, "gex_crores": gex_crores,
                "close_pos": close_pos,
                "ce_gain": ce_gain, "pe_gain": pe_gain
            })

    print(f"Collected {len(dataset)} evaluation points.")

    # Grid search for parameters that yield Win Rate >= 90%
    # We want to optimize separate rules for BULL (CE) and BEAR (PE)
    
    print("\n--- OPTIMIZING BULLISH (CE) BREAKOUT RULE ---")
    best_ce_rule = None
    best_ce_trades = 0
    
    # We test values for:
    # ci_thresh: 0.15, 0.25, 0.35, 0.45
    # vel_thresh: 0.2, 0.5, 1.0, 1.5
    # vol_thresh: 0.8, 1.0, 1.2, 1.5
    # close_pos_thresh: 0.5, 0.6, 0.7, 0.8
    # pcr_trend: True/False (PCR > PCR_prev)
    
    for ci in [0.20, 0.30, 0.40]:
        for vel in [0.5, 1.0, 1.5, 2.0]:
            for vol in [1.0, 1.2, 1.4]:
                for c_pos in [0.60, 0.70, 0.80]:
                    for pcr_up in [True, False]:
                        # Run backtest with this rule
                        triggered = []
                        for r in dataset:
                            cond = (
                                r["ci"] <= ci and 
                                r["velocity"] >= vel and 
                                r["vol_ratio"] >= vol and 
                                r["close_pos"] >= c_pos
                            )
                            if pcr_up:
                                cond = cond and (r["pcr"] > r["pcr_prev"])
                                
                            if cond:
                                triggered.append(r)
                                
                        n = len(triggered)
                        if n >= 1: # show any trades to find the best rules
                            wins = sum(1 for t in triggered if t["ce_gain"] >= 50.0)
                            wr = wins / n * 100
                            if wr >= 75.0:
                                print(f"  RULE: CI <= {ci}, Vel >= {vel}, Vol >= {vol}, ClosePos >= {c_pos}, PCR_up={pcr_up} -> Trades: {n}, Wins: {wins}, WR: {wr:.2f}%")
                                if wr >= 80.0 and n > best_ce_trades:
                                    best_ce_trades = n
                                    best_ce_rule = (ci, vel, vol, c_pos, pcr_up, wr)

    if best_ce_rule:
        print(f"\nBEST CE RULE FOUND: CI <= {best_ce_rule[0]}, Vel >= {best_ce_rule[1]}, Vol >= {best_ce_rule[2]}, ClosePos >= {best_ce_rule[3]}, PCR_up={best_ce_rule[4]} -> WR: {best_ce_rule[5]:.2f}% ({best_ce_trades} trades)")
    else:
        print("\nNo CE rule achieved threshold.")

    print("\n--- OPTIMIZING BEARISH (PE) BREAKDOWN RULE ---")
    best_pe_rule = None
    best_pe_trades = 0
    
    for ci in [0.20, 0.30, 0.40]:
        for vel in [-0.5, -1.0, -1.5, -2.0]:
            for vol in [1.0, 1.2, 1.4]:
                for c_pos in [0.40, 0.30, 0.20]:
                    for pcr_down in [True, False]:
                        triggered = []
                        for r in dataset:
                            cond = (
                                r["ci"] <= ci and 
                                r["velocity"] <= vel and 
                                r["vol_ratio"] >= vol and 
                                r["close_pos"] <= c_pos
                            )
                            if pcr_down:
                                cond = cond and (r["pcr"] < r["pcr_prev"])
                                
                            if cond:
                                triggered.append(r)
                                
                        n = len(triggered)
                        if n >= 1:
                            wins = sum(1 for t in triggered if t["pe_gain"] >= 50.0)
                            wr = wins / n * 100
                            if wr >= 75.0:
                                print(f"  RULE: CI <= {ci}, Vel <= {vel}, Vol >= {vol}, ClosePos <= {c_pos}, PCR_down={pcr_down} -> Trades: {n}, Wins: {wins}, WR: {wr:.2f}%")
                                if wr >= 80.0 and n > best_pe_trades:
                                    best_pe_trades = n
                                    best_pe_rule = (ci, vel, vol, c_pos, pcr_down, wr)

    if best_pe_rule:
        print(f"\nBEST PE RULE FOUND: CI <= {best_pe_rule[0]}, Vel <= {best_pe_rule[1]}, Vol >= {best_pe_rule[2]}, ClosePos <= {best_pe_rule[3]}, PCR_down={best_pe_rule[4]} -> WR: {best_pe_rule[5]:.2f}% ({best_pe_trades} trades)")
    else:
        print("\nNo PE rule achieved threshold.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
