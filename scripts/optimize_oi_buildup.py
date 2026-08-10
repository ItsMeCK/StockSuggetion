import os
import csv
import math
import psycopg2
from datetime import datetime, date, timedelta

def solve_iv(market_price, S, K, T, r, opt_type):
    return 0.25 # simplified for speed

def bs_gamma(S, K, T, r, sigma):
    return 0.001 # simplified for speed

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

            p_prev = prices[i-1]
            prev_d = p_prev["date"]
            if prev_d not in opt_by_date:
                continue

            day_opts = opt_by_date[d]
            prev_opts = opt_by_date[prev_d]
            spot_price = p_day["close"]

            # Calculate ATM CE and PE contracts
            ce_opts = [o for o in day_opts if o["option_type"] == 'CE']
            pe_opts = [o for o in day_opts if o["option_type"] == 'PE']
            if not ce_opts or not pe_opts:
                continue

            unique_strikes = sorted(list(set(o["strike"] for o in ce_opts)))
            atm_strike = min(unique_strikes, key=lambda s: abs(s - spot_price))
            
            # Select ATM contract
            atm_ce = min(ce_opts, key=lambda o: abs(o["strike"] - spot_price))
            atm_pe = min(pe_opts, key=lambda o: abs(o["strike"] - spot_price))
            
            # Calculate option OI buildup day-over-day
            # We look at the closest 3 strikes above spot for CE (ATM to OTM)
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

            # Calculate CI
            slice_20 = prices[i-19:i+1]
            closes_20 = [r["close"] for r in slice_20]
            sma20 = sum(closes_20) / 20.0
            std_dev = math.sqrt(sum((c - sma20)**2 for c in closes_20) / 19.0)
            bbw = (4.0 * std_dev) / sma20 if sma20 > 0 else 0.0

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

            dataset.append({
                "symbol": sym, "date": d, "ci": ci, "vol_ratio": vol_ratio,
                "ce_oi_change": ce_oi_change, "pe_oi_change": pe_oi_change,
                "close_pos": close_pos, "ce_gain": ce_gain, "pe_gain": pe_gain,
                "atm_ce": trsym_ce, "atm_pe": trsym_pe
            })

    print(f"Collected {len(dataset)} evaluation points.")

    # Grid search optimized for 50%+ mover win rate
    print("\n--- OPTIMIZING FOR 90%+ WIN RATE CE TRADES ---")
    for ci_t in [0.20, 0.30, 0.40]:
        for ce_oi_t in [10.0, 20.0, 30.0, 50.0]:
            for vol_t in [0.6, 0.8, 1.0, 1.2]:
                triggered = []
                for r in dataset:
                    # Condition: Squeezed, Call OI build, volume not dead
                    if (r["ci"] <= ci_t and 
                        r["ce_oi_change"] >= ce_oi_t and 
                        r["vol_ratio"] >= vol_t):
                        triggered.append(r)
                
                n = len(triggered)
                if n >= 1:
                    wins = sum(1 for t in triggered if t["ce_gain"] >= 50.0)
                    wr = wins / n * 100
                    if wr >= 50.0:
                        print(f"  BULL RULE: CI <= {ci_t}, CE_OI_change >= {ce_oi_t}%, VolRatio >= {vol_t} -> Trades: {n}, Wins: {wins}, WR: {wr:.2f}%")
                        for t in triggered[:3]:
                            print(f"    - {t['symbol']} on {t['date'].isoformat()} ({t['atm_ce']}) CE_OI_change: {t['ce_oi_change']:.1f}% -> Gain: {t['ce_gain']:.1f}%")

    print("\n--- OPTIMIZING FOR 90%+ WIN RATE PE TRADES ---")
    for ci_t in [0.20, 0.30, 0.40]:
        for pe_oi_t in [10.0, 20.0, 30.0, 50.0]:
            for vol_t in [0.6, 0.8, 1.0, 1.2]:
                triggered = []
                for r in dataset:
                    # Condition: Squeezed, Put OI build, volume not dead
                    if (r["ci"] <= ci_t and 
                        r["pe_oi_change"] >= pe_oi_t and 
                        r["vol_ratio"] >= vol_t):
                        triggered.append(r)
                
                n = len(triggered)
                if n >= 1:
                    wins = sum(1 for t in triggered if t["pe_gain"] >= 50.0)
                    wr = wins / n * 100
                    if wr >= 50.0:
                        print(f"  BEAR RULE: CI <= {ci_t}, PE_OI_change >= {pe_oi_t}%, VolRatio >= {vol_t} -> Trades: {n}, Wins: {wins}, WR: {wr:.2f}%")
                        for t in triggered[:3]:
                            print(f"    - {t['symbol']} on {t['date'].isoformat()} ({t['atm_pe']}) PE_OI_change: {t['pe_oi_change']:.1f}% -> Gain: {t['pe_gain']:.1f}%")

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
