import os
import csv
import math
import psycopg2
import polars as pl
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
    # Check boundary
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

# --- DB Connection Helper ---
def get_db_conn():
    return psycopg2.connect(
        host=os.getenv('DB_HOST', 'localhost'),
        port=os.getenv('DB_PORT', '5432'),
        user=os.getenv('POSTGRES_USER', 'quant'),
        password=os.getenv('POSTGRES_PASSWORD', 'quantpassword'),
        dbname=os.getenv('POSTGRES_DB', 'market_data')
    )

# --- Lot Sizes Lookup Helper ---
LOT_SIZES = {}
def load_lot_sizes():
    global LOT_SIZES
    path = "pipeline/fo_universe.csv"
    if os.path.exists(path):
        try:
            with open(path) as f:
                for row in csv.DictReader(f):
                    LOT_SIZES[row["Symbol"]] = int(row.get("LotSize", 1))
        except Exception:
            pass

def get_lot_size(symbol):
    return LOT_SIZES.get(symbol, 100)

def main():
    load_lot_sizes()
    conn = get_db_conn()
    cur = conn.cursor()

    cur.execute("SELECT DISTINCT underlying FROM option_oi_daily ORDER BY underlying")
    symbols = [r[0] for r in cur.fetchall()]
    print(f"Loaded {len(symbols)} option underlyings from database.")

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
    print(f"Stored daily price history for {len(ohlcv_by_symbol)} symbols.")

    cur.execute("""
        SELECT date, underlying, strike, option_type, expiry, close, oi, volume, spot 
        FROM option_oi_daily 
        WHERE date BETWEEN '2026-07-01' AND '2026-07-08'
        ORDER BY underlying, date
    """)
    option_rows = cur.fetchall()
    
    options_by_symbol = {}
    for r in option_rows:
        dt, sym, strike, opt_type, expiry, close, oi, volume, spot = r
        if None in (strike, close, oi, volume):
            continue
        options_by_symbol.setdefault(sym, []).append({
            "date": dt, "strike": float(strike), "option_type": opt_type, "expiry": expiry,
            "close": float(close), "oi": int(oi), "volume": int(volume), "spot": float(spot) if spot is not None else 0.0
        })
    print(f"Stored option chain data for {len(options_by_symbol)} symbols.")

    eval_dates = sorted(list(set(r["date"] for sym in options_by_symbol for r in options_by_symbol[sym])))
    print(f"Option dates found: {[d.isoformat() for d in eval_dates]}")

    print(f"DEBUG: ohlcv_by_symbol size: {len(ohlcv_by_symbol)}, options_by_symbol size: {len(options_by_symbol)}")
    
    results = []

    for sym in symbols:
        prices = ohlcv_by_symbol.get(sym)
        opt_data = options_by_symbol.get(sym)
        if not prices or not opt_data:
            if sym == "ABB" or sym == "ICICIBANK":
                print(f"DEBUG: {sym} missing. prices: {prices is not None}, opt_data: {opt_data is not None}")
            continue
            
        opt_by_date = {}
        for row in opt_data:
            opt_by_date.setdefault(row["date"], []).append(row)

        for i in range(len(prices)):
            p_day = prices[i]
            d = p_day["date"]
            if d not in opt_by_date:
                continue

            if i < 21: # need i-20 for prev BBW
                continue

            # Compute current BBW
            slice_20 = prices[i-19:i+1]
            closes_20 = [r["close"] for r in slice_20]
            sma20 = sum(closes_20) / 20.0
            variance = sum((c - sma20) ** 2 for c in closes_20) / 19.0
            std_dev = math.sqrt(variance)
            bbw = (sma20 + 2.0 * std_dev - (sma20 - 2.0 * std_dev)) / sma20 if sma20 > 0 else 0.0

            # Compute prev BBW
            prev_slice_20 = prices[i-20:i]
            prev_closes_20 = [r["close"] for r in prev_slice_20]
            prev_sma20 = sum(prev_closes_20) / 20.0
            prev_variance = sum((c - prev_sma20) ** 2 for c in prev_closes_20) / 19.0
            prev_std_dev = math.sqrt(prev_variance)
            bbw_prev = (prev_sma20 + 2.0 * prev_std_dev - (prev_sma20 - 2.0 * prev_std_dev)) / prev_sma20 if prev_sma20 > 0 else 0.0

            # Compute trailing 20-day BBW min and max for CI
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
                
            if len(prior_bbw) < 20:
                continue
            min_bbw = min(prior_bbw)
            max_bbw = max(prior_bbw)
            ci = (bbw - min_bbw) / (max_bbw - min_bbw) if (max_bbw - min_bbw) > 0 else 0.5

            # Compute volume ratio
            vol_avg20 = sum(r["volume"] for r in prices[i-20:i]) / 20.0
            vol_ratio = p_day["volume"] / vol_avg20 if vol_avg20 > 0 else 1.0

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

            def calc_pcr(opts, spot):
                ce = [c for c in opts if c["option_type"] == 'CE']
                pe = [c for c in opts if c["option_type"] == 'PE']
                if not ce or not pe:
                    return 1.0, [], []
                strikes = sorted(list(set(c["strike"] for c in ce)))
                atm = min(strikes, key=lambda s: abs(s - spot))
                idx = strikes.index(atm)
                lo, hi = max(0, idx - 3), min(len(strikes) - 1, idx + 3)
                sel = strikes[lo:hi+1]
                ce_f = [c for c in ce if c["strike"] in sel]
                pe_f = [c for c in pe if c["strike"] in sel]
                ce_oi = sum(c["oi"] for c in ce_f)
                pe_oi = sum(c["oi"] for c in pe_f)
                return (pe_oi / ce_oi if ce_oi > 0 else 1.0), ce_f, pe_f

            day_options = opt_by_date[d]
            spot_price = p_day["close"]
            pcr, ce_filtered, pe_filtered = calc_pcr(day_options, spot_price)

            pcr_prev = pcr
            prev_d = p_prev["date"]
            if prev_d in opt_by_date:
                pcr_prev, _, _ = calc_pcr(opt_by_date[prev_d], p_prev["close"])

            net_gex = 0.0
            r = 0.07
            lot_size = get_lot_size(sym)
            
            for opt in ce_filtered + pe_filtered:
                K = opt["strike"]
                T = (opt["expiry"] - d).days / 365.0
                opt_type = opt["option_type"]
                premium = opt["close"]
                oi = opt["oi"]
                
                iv = solve_iv(premium, spot_price, K, T, r, opt_type)
                gamma = bs_gamma(spot_price, K, T, r, iv)
                
                gex_val = gamma * oi * spot_price * lot_size
                if opt_type == 'PE':
                    gex_val *= -1
                net_gex += gex_val

            # Compute ATR14
            tr_list = []
            for j in range(i-13, i+1):
                high_j = prices[j]["high"]
                low_j = prices[j]["low"]
                close_prev_j = prices[j-1]["close"]
                tr = max(high_j - low_j, abs(high_j - close_prev_j), abs(low_j - close_prev_j))
                tr_list.append(tr)
            atr14 = sum(tr_list) / 14.0

            net_gex_crores = net_gex / 10000000.0

            future_prices = prices[i+1:i+4]
            if not future_prices:
                continue
                
            max_high = max(p["high"] for p in future_prices)
            min_low = min(p["low"] for p in future_prices)
            
            high_ret_pct = (max_high - close_t) / close_t * 100
            low_ret_pct = (min_low - close_t) / close_t * 100

            results.append({
                "symbol": sym, "date": d, "close": close_t, "ci": ci,
                "bbw": bbw, "bbw_prev": bbw_prev, "vol_ratio": vol_ratio,
                "velocity": velocity, "acceleration": acceleration,
                "pcr": pcr, "gex_crores": net_gex_crores,
                "high_ret": high_ret_pct, "low_ret": low_ret_pct,
                "atr14": atr14
            })

    print(f"\nCalculated indicators for {len(results)} symbol-days.")
    
    configs = [
        {"name": "Equally Weighted Mesh", "w_shannon": 40, "w_pring": 30, "w_options": 30},
        {"name": "Options Bias Mesh",     "w_shannon": 30, "w_pring": 30, "w_options": 40},
        {"name": "Momentum Bias Mesh",    "w_shannon": 30, "w_pring": 40, "w_options": 30}
    ]
    
    thresholds = [70, 75, 80, 85, 90]
    
    for conf in configs:
        print(f"\n" + "="*50)
        print(f"CONFIGURATION: {conf['name']}")
        print("="*50)
        w_sh = conf["w_shannon"]
        w_pr = conf["w_pring"]
        w_op = conf["w_options"]
        
        for thresh in thresholds:
            bull_trades = []
            bear_trades = []
            
            for r in results:
                sh_score_bull = 0
                sh_score_bear = 0
                # Shannon Squeeze (bottom 20th percentile)
                if r["ci"] <= 0.20:
                    sh_score_bull += w_sh * 0.5
                    sh_score_bear += w_sh * 0.5
                # Shannon Breakout / Band Expansion
                if r["bbw"] > r["bbw_prev"]:
                    sh_score_bull += w_sh * 0.5
                    sh_score_bear += w_sh * 0.5
                    
                pr_score_bull = 0
                pr_score_bear = 0
                # Pring Momentum Velocity & Acceleration
                if r["velocity"] > 0 and r["acceleration"] > 0:
                    pr_score_bull += w_pr * 0.5
                if r["velocity"] < 0 and r["acceleration"] < 0:
                    pr_score_bear += w_pr * 0.5
                # Pring Volume Expansion
                if r["vol_ratio"] >= 1.1:
                    pr_score_bull += w_pr * 0.5
                    pr_score_bear += w_pr * 0.5
                    
                op_score_bull = 0
                op_score_bear = 0
                # Options GEX Break
                if abs(r["gex_crores"]) <= 10.0:
                    op_score_bull += w_op * 0.5
                    op_score_bear += w_op * 0.5
                # Options PCR Skew
                if r["pcr"] >= 1.15:
                    op_score_bull += w_op * 0.5
                elif r["pcr"] <= 0.65:
                    op_score_bear += w_op * 0.5
                    
                total_bull = sh_score_bull + pr_score_bull + op_score_bull
                total_bear = sh_score_bear + pr_score_bear + op_score_bear
                
                # ATR stop loss percent (1.5x ATR)
                stop_pct = (1.5 * r["atr14"] / r["close"]) * 100
                
                # Volatility-adjusted target and stop loss (1.5x ATR target, 1.0x ATR stop loss)
                target_pct = (1.5 * r["atr14"] / r["close"]) * 100
                stop_pct = (1.0 * r["atr14"] / r["close"]) * 100
                
                # Fetch trigger day OHLC to check close range position
                p_day = next(p for p in prices if p["date"] == r["date"])
                tr_range = p_day["high"] - p_day["low"]
                close_pos = (p_day["close"] - p_day["low"]) / tr_range if tr_range > 0 else 0.5
                
                # Enforce strict direction and trigger day close strength
                if total_bull >= thresh and r["velocity"] > 0 and r["pcr"] >= 1.0 and close_pos >= 0.5:
                    is_win = r["high_ret"] >= target_pct and r["low_ret"] > -stop_pct
                    bull_trades.append({"symbol": r["symbol"], "date": r["date"], "score": total_bull, "high_ret": r["high_ret"], "low_ret": r["low_ret"], "target_pct": target_pct, "stop_pct": stop_pct, "is_win": is_win})
                    
                if total_bear >= thresh and r["velocity"] < 0 and r["pcr"] <= 1.0 and close_pos <= 0.5:
                    is_win = r["low_ret"] <= -target_pct and r["high_ret"] < stop_pct
                    bear_trades.append({"symbol": r["symbol"], "date": r["date"], "score": total_bear, "high_ret": r["high_ret"], "low_ret": r["low_ret"], "target_pct": target_pct, "stop_pct": stop_pct, "is_win": is_win})

            def print_summary(trades, t_type, th):
                n = len(trades)
                if n == 0:
                    return
                wins = sum(1 for t in trades if t["is_win"])
                wr = wins / n * 100
                print(f"  Threshold {th} -> {t_type}: {n} trades, {wins} wins, Win Rate: {wr:.2f}%")
                if th >= 80:
                    for t in trades[:5]:
                        print(f"    - {t['symbol']} on {t['date'].isoformat()} (Score: {t['score']:.1f}) -> High: {t['high_ret']:+.2f}% (Tgt: {t['target_pct']:.2f}%), Low: {t['low_ret']:+.2f}% (SL: {t['stop_pct']:.2f}%)")

            print_summary(bull_trades, "BULLISH BREAKOUTS", thresh)
            print_summary(bear_trades, "BEARISH BREAKDOWNS", thresh)
        
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
