import os
import sys
import math
import psycopg2
from datetime import date

sys.path.append(os.getcwd())

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

    # Fetch daily price history
    cur.execute("""
        SELECT symbol, time::date as date, open, high, low, close, volume 
        FROM daily_ohlcv 
        WHERE time::date BETWEEN '2026-04-01' AND '2026-07-08'
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

    # Fetch daily options records
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
            # Need 50 days of history for 50-SMA
            if i < 50:
                continue

            p_prev = prices[i-1]
            prev_d = p_prev["date"]
            if prev_d not in opt_by_date:
                continue

            day_opts = opt_by_date[d]
            prev_opts = opt_by_date[prev_d]
            spot_price = p_day["close"]

            ce_opts = [o for o in day_opts if o["option_type"] == 'CE']
            pe_opts = [o for o in day_opts if o["option_type"] == 'PE']
            if not ce_opts or not pe_opts:
                continue

            # Total option chain metrics
            total_ce_oi = sum(o["oi"] for o in ce_opts)
            total_pe_oi = sum(o["oi"] for o in pe_opts)
            total_chain_oi = total_ce_oi + total_pe_oi

            # Calculate ATM CE and PE contracts
            unique_strikes = sorted(list(set(o["strike"] for o in ce_opts)))
            atm_strike = min(unique_strikes, key=lambda s: abs(s - spot_price))
            
            # Select ATM contract
            atm_ce = min(ce_opts, key=lambda o: abs(o["strike"] - spot_price))
            atm_pe = min(pe_opts, key=lambda o: abs(o["strike"] - spot_price))
            
            atm_idx = unique_strikes.index(atm_strike)
            otm_ce_strikes = unique_strikes[atm_idx:min(len(unique_strikes), atm_idx+3)]
            otm_pe_strikes = unique_strikes[max(0, atm_idx-2):atm_idx+1]
            
            ce_oi_now = sum(o["oi"] for o in ce_opts if o["strike"] in otm_ce_strikes)
            pe_oi_now = sum(o["oi"] for o in pe_opts if o["strike"] in otm_pe_strikes)
            
            prev_ce_opts = [o for o in prev_opts if o["option_type"] == 'CE']
            prev_pe_opts = [o for o in prev_opts if o["option_type"] == 'PE']
            ce_oi_prev = sum(o["oi"] for o in prev_ce_opts if o["strike"] in otm_ce_strikes)
            pe_oi_prev = sum(o["oi"] for o in prev_pe_opts if o["strike"] in otm_pe_strikes)
            
            ce_oi_change = (ce_oi_now - ce_oi_prev) / ce_oi_prev * 100 if ce_oi_prev > 0 else 0.0
            pe_oi_change = (pe_oi_now - pe_oi_prev) / pe_oi_prev * 100 if pe_oi_prev > 0 else 0.0

            future_dates = [prices[idx]["date"] for idx in range(i+1, min(len(prices), i+4))]
            if not future_dates:
                continue

            # Calculate SMAs
            closes_50 = [r["close"] for r in prices[i-49:i+1]]
            sma50 = sum(closes_50) / 50.0
            closes_20 = [r["close"] for r in prices[i-19:i+1]]
            sma20 = sum(closes_20) / 20.0
            
            # BBW / CI
            std_dev20 = math.sqrt(sum((c - sma20)**2 for c in closes_20) / 19.0)
            bbw = (4.0 * std_dev20) / sma20 if sma20 > 0 else 0.0
            
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

            # Volume ratio
            vol_avg20 = sum(r["volume"] for r in prices[i-20:i]) / 20.0
            vol_ratio = p_day["volume"] / vol_avg20 if vol_avg20 > 0 else 1.0

            # CE / PE contract max gain in next 3 days
            trsym_ce = atm_ce["tradingsymbol"]
            ce_prem_t = atm_ce["close"]
            ce_future_prems = [option_premium_history[trsym_ce][fd] for fd in future_dates if fd in option_premium_history.get(trsym_ce, {})]
            ce_gain = (max(ce_future_prems) - ce_prem_t) / ce_prem_t * 100 if ce_future_prems and ce_prem_t > 0 else 0.0

            trsym_pe = atm_pe["tradingsymbol"]
            pe_prem_t = atm_pe["close"]
            pe_future_prems = [option_premium_history[trsym_pe][fd] for fd in future_dates if fd in option_premium_history.get(trsym_pe, {})]
            pe_gain = (max(pe_future_prems) - pe_prem_t) / pe_prem_t * 100 if pe_future_prems and pe_prem_t > 0 else 0.0

            tr_range = p_day["high"] - p_day["low"]
            close_pos = (p_day["close"] - p_day["low"]) / tr_range if tr_range > 0 else 0.5

            # PCR
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
            if prev_d in opt_by_date:
                pcr_prev_val = get_pcr_local(opt_by_date[prev_d], p_prev["close"])

            # Calculate GEX crores
            atm_idx = unique_strikes.index(atm_strike)
            lo_idx, hi_idx = max(0, atm_idx - 3), min(len(unique_strikes) - 1, atm_idx + 3)
            selected_strikes = unique_strikes[lo_idx:hi_idx+1]
            ce_filtered = [c for c in ce_opts if c["strike"] in selected_strikes]
            pe_filtered = [c for c in pe_opts if c["strike"] in selected_strikes]
            
            net_gex = 0.0
            r_rate = 0.07
            lot_size = 100
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

            # Momentum ROC Velocity
            close_t = p_day["close"]
            close_14ago = prices[i-14]["close"]
            roc_t = (close_t - close_14ago) / close_14ago * 100 if close_14ago > 0 else 0.0
            close_prev = p_prev["close"]
            close_15ago = prices[i-15]["close"]
            roc_prev = (close_prev - close_15ago) / close_15ago * 100 if close_15ago > 0 else 0.0
            velocity = roc_t - roc_prev

            dataset.append({
                "symbol": sym, "date": d, "ci": ci, "vol_ratio": vol_ratio,
                "ce_oi_change": ce_oi_change, "pe_oi_change": pe_oi_change,
                "close_pos": close_pos, "ce_gain": ce_gain, "pe_gain": pe_gain,
                "atm_ce": trsym_ce, "atm_pe": trsym_pe,
                "pcr": pcr_t, "pcr_prev": pcr_prev_val, "velocity": velocity,
                "total_ce_oi": total_ce_oi, "total_pe_oi": total_pe_oi, "total_chain_oi": total_chain_oi,
                "close": spot_price, "sma20": sma20, "sma50": sma50, "gex_crores": gex_crores
            })

    print(f"Collected {len(dataset)} evaluation points.")

    print("\n--- RUNNING REFINED HIGH CONVICTION BULLISH (CE) RULES ---")
    triggered_ce = []
    for r in dataset:
        # Enforce:
        # 1. Trend: Stock Close > 20-SMA and 20-SMA > 50-SMA
        # 2. Squeeze: CI <= 0.35
        # 3. Liquidity: Total option chain OI >= 100,000
        # 4. Call dominance: total_ce_oi > total_pe_oi
        # 5. Call OI change: ce_oi_change >= 20%
        # 6. Direction: velocity > 0 and close_pos >= 0.50
        # 7. PCR Trend: PCR must decrease (pcr < pcr_prev)
        # 8. GEX: gex_crores >= 15.0 crores (strong call pin support)
        if (
            r["close"] > r["sma20"] and r["sma20"] > r["sma50"] and
            r["ci"] <= 0.35 and
            r["total_chain_oi"] >= 100000 and
            r["total_ce_oi"] > r["total_pe_oi"] and
            r["ce_oi_change"] >= 20.0 and
            r["velocity"] > 0 and r["close_pos"] >= 0.50 and
            r["pcr"] < r["pcr_prev"] and
            r["gex_crores"] >= 15.0
        ):
            triggered_ce.append(r)

    n_ce = len(triggered_ce)
    if n_ce > 0:
        wins_50 = sum(1 for t in triggered_ce if t["ce_gain"] >= 50.0)
        wins_30 = sum(1 for t in triggered_ce if t["ce_gain"] >= 30.0)
        wins_pos = sum(1 for t in triggered_ce if t["ce_gain"] > 0.0)
        print(f"BULL CE TRADES: {n_ce} triggered. 50%+ Wins: {wins_50} ({wins_50/n_ce*100:.1f}%), 30%+ Wins: {wins_30} ({wins_30/n_ce*100:.1f}%), Positive Wins: {wins_pos} ({wins_pos/n_ce*100:.1f}%)")
        for t in triggered_ce:
            print(f"  - {t['symbol']} on {t['date'].isoformat()} ({t['atm_ce']}) CE_OI_change: {t['ce_oi_change']:.1f}% -> Gain: {t['ce_gain']:.1f}% | CI: {t['ci']:.2f} | GEX: {t['gex_crores']:.2f}")
    else:
        print("No CE trades triggered.")

    print("\n--- RUNNING REFINED HIGH CONVICTION BEARISH (PE) RULES ---")
    triggered_pe = []
    for r in dataset:
        # Enforce:
        # 1. Trend: Stock Close < 20-SMA and 20-SMA < 50-SMA
        # 2. Squeeze: CI <= 0.40
        # 3. Liquidity: Total option chain OI >= 150,000
        # 4. Put dominance: total_pe_oi > total_ce_oi
        # 5. Put OI change: pe_oi_change >= 10.0%
        # 6. Direction: velocity < 0 and close_pos <= 0.50
        # 7. Volume: No consensus exhaustion (vol_ratio <= 2.0)
        if (
            r["close"] < r["sma20"] and r["sma20"] < r["sma50"] and
            r["ci"] <= 0.40 and
            r["total_chain_oi"] >= 150000 and
            r["total_pe_oi"] > r["total_ce_oi"] and
            r["pe_oi_change"] >= 10.0 and
            r["velocity"] < 0 and r["close_pos"] <= 0.50 and
            r["vol_ratio"] <= 2.0
        ):
            triggered_pe.append(r)

    n_pe = len(triggered_pe)
    if n_pe > 0:
        wins_50 = sum(1 for t in triggered_pe if t["pe_gain"] >= 50.0)
        wins_30 = sum(1 for t in triggered_pe if t["pe_gain"] >= 30.0)
        wins_pos = sum(1 for t in triggered_pe if t["pe_gain"] > 0.0)
        print(f"BEAR PE TRADES: {n_pe} triggered. 50%+ Wins: {wins_50} ({wins_50/n_pe*100:.1f}%), 30%+ Wins: {wins_30} ({wins_30/n_pe*100:.1f}%), Positive Wins: {wins_pos} ({wins_pos/n_pe*100:.1f}%)")
        for t in triggered_pe:
            print(f"  - {t['symbol']} on {t['date'].isoformat()} ({t['atm_pe']}) PE_OI_change: {t['pe_oi_change']:.1f}% -> Gain: {t['pe_gain']:.1f}% | CI: {t['ci']:.2f} | VolRatio: {t['vol_ratio']:.2f}")
    else:
        print("No PE trades triggered.")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
