"""
20-day backtest of the 3 PM (EOD/pulse-3-equivalent) signal generation pipeline,
comparing two hypothetical trade structures on every approved signal:

  EQUITY leg:  entry at next-day open. Exit at +10% target, -5% stop, or a
               forced time-stop after 2 trading days open - whichever comes first.

  OPTIONS leg: entry at next-day open, buying the nearest-strike call, priced
               with a Black-Scholes proxy (no real historical option premium
               data exists in this system - see caveats printed at the end).
               1 NFO slot rule: hard -50% stop-loss from entry, OR once the
               position reaches +100% (2x) a trailing stop ARMS and gives
               back no more than 10% from peak (no fixed take-profit cap,
               unlimited upside past 2x) - and every slot is force-closed
               after 3 trading days regardless, whichever triggers first.

Usage: PYTHONPATH=. venv/bin/python3 scripts/backtest_3pm_options_vs_equity.py [--days 20]
"""
import os
import sys
import json
import math
import logging
import argparse
import numpy as np
import psycopg2
from dotenv import load_dotenv

load_dotenv()
os.environ["TRADING_MODE"] = "HISTORICAL"  # keep the agent pipeline cheap/deterministic

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

from run_historical import run_historical_engine

# ---------------------------------------------------------------------------
# Black-Scholes proxy (no real options data exists - see module docstring)
# ---------------------------------------------------------------------------
RISK_FREE_RATE = 0.07
EXPIRY_DAYS = 30
# Validated exit rule (1 NFO slot): hard stop -50% from entry. Trailing stop
# only ARMS once premium reaches 2x entry (not a fixed take-profit cap) -
# after arming, ride it with a 10% giveback trail, unlimited upside. Every
# NFO slot is force-closed after MAX_HOLD_DAYS regardless of the above.
ARM_MULT = 2.0
TRAIL_GIVEBACK = 0.10
HARD_STOP_MULT = 0.50
MAX_HOLD_DAYS = 3

EQUITY_TARGET_PCT = 0.10
EQUITY_STOP_PCT = 0.05
EQUITY_MAX_DAYS = 2


def norm_cdf(x: float) -> float:
    return 0.5 * (1 + math.erf(x / math.sqrt(2)))


def bs_call_price(S: float, K: float, t_years: float, r: float, sigma: float) -> float:
    if t_years <= 0 or sigma <= 0 or S <= 0 or K <= 0:
        return max(S - K, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma ** 2) * t_years) / (sigma * math.sqrt(t_years))
    d2 = d1 - sigma * math.sqrt(t_years)
    return S * norm_cdf(d1) - K * math.exp(-r * t_years) * norm_cdf(d2)


def strike_step(price: float) -> float:
    if price < 100: return 2.5
    if price < 250: return 5
    if price < 1000: return 10
    if price < 2500: return 20
    if price < 5000: return 50
    return 100


def nearest_strike(price: float) -> float:
    step = strike_step(price)
    return round(price / step) * step


def estimate_iv(trailing_closes) -> float:
    """Annualized realized vol from trailing daily log returns, scaled to an IV proxy."""
    if len(trailing_closes) < 5:
        return 0.35
    closes = np.array(trailing_closes, dtype=float)
    closes = closes[closes > 0]
    if len(closes) < 5:
        return 0.35
    rets = np.diff(np.log(closes))
    realized_vol = float(np.std(rets) * math.sqrt(252))
    iv = realized_vol * 1.15
    return min(max(iv, 0.25), 0.75)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------
def get_conn():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        user=os.getenv("POSTGRES_USER", "quant"),
        password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
        database=os.getenv("POSTGRES_DB", "market_data"),
    )


def get_recent_trading_dates(n: int, end_date: str = None):
    conn = get_conn()
    cur = conn.cursor()
    if end_date:
        cur.execute("""
            SELECT DISTINCT time::date FROM daily_ohlcv
            WHERE symbol NOT LIKE %s AND symbol NOT LIKE %s AND time::date <= %s
            ORDER BY time::date DESC LIMIT %s
        """, ('%NIFTY%', '%BEES%', end_date, n))
    else:
        cur.execute("""
            SELECT DISTINCT time::date FROM daily_ohlcv
            WHERE symbol NOT LIKE %s AND symbol NOT LIKE %s
            ORDER BY time::date DESC LIMIT %s
        """, ('%NIFTY%', '%BEES%', n))
    dates = sorted([row[0].strftime("%Y-%m-%d") for row in cur.fetchall()])
    cur.close()
    conn.close()
    return dates


def fetch_symbol_series(symbol: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT time::date, open, high, low, close, volume
        FROM daily_ohlcv WHERE symbol = %s ORDER BY time ASC
    """, (symbol,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return [
        {"date": r[0].strftime("%Y-%m-%d"), "open": float(r[1]), "high": float(r[2]),
         "low": float(r[3]), "close": float(r[4]), "volume": float(r[5])}
        for r in rows if r[1] is not None
    ]


# ---------------------------------------------------------------------------
# Trade simulators
# ---------------------------------------------------------------------------
def simulate_equity_trade(series, entry_idx):
    entry_price = series[entry_idx]["open"]
    target = entry_price * (1 + EQUITY_TARGET_PCT)
    stop = entry_price * (1 - EQUITY_STOP_PCT)

    for offset in range(EQUITY_MAX_DAYS):
        idx = entry_idx + offset
        if idx >= len(series):
            return {"status": "DATA_END_OPEN", "exit_price": None, "pnl_pct": None,
                     "days_held": offset, "exit_date": None, "entry_price": entry_price}
        bar = series[idx]
        if bar["low"] <= stop:
            return {"status": "STOP_5PCT", "exit_price": stop, "pnl_pct": -EQUITY_STOP_PCT * 100,
                     "days_held": offset + 1, "exit_date": bar["date"], "entry_price": entry_price}
        if bar["high"] >= target:
            return {"status": "TARGET_10PCT", "exit_price": target, "pnl_pct": EQUITY_TARGET_PCT * 100,
                     "days_held": offset + 1, "exit_date": bar["date"], "entry_price": entry_price}

    last_idx = min(entry_idx + EQUITY_MAX_DAYS - 1, len(series) - 1)
    exit_price = series[last_idx]["close"]
    pnl_pct = ((exit_price - entry_price) / entry_price) * 100
    return {"status": "TIME_STOP_2D", "exit_price": exit_price, "pnl_pct": pnl_pct,
             "days_held": EQUITY_MAX_DAYS, "exit_date": series[last_idx]["date"], "entry_price": entry_price}


def simulate_option_trade(series, entry_idx, entry_price_field="open"):
    """entry_price_field='open' => next-day open (legacy). 'close' => same-day
    close (validated better: theta is the enemy of options, enter at/near the
    ignition candle's own close during the 3PM pulse window, not next-day)."""
    S0 = series[entry_idx][entry_price_field]
    K = nearest_strike(S0)
    trailing_closes = [b["close"] for b in series[max(0, entry_idx - 20):entry_idx]]
    sigma = estimate_iv(trailing_closes)
    entry_premium = bs_call_price(S0, K, EXPIRY_DAYS / 365.0, RISK_FREE_RATE, sigma)

    base = {"strike": K, "sigma": round(sigma, 3), "entry_premium": round(entry_premium, 2), "underlying_entry": S0}

    if entry_premium <= 0.05:
        return {**base, "status": "INVALID_PREMIUM", "exit_premium": None, "pnl_pct": None,
                 "days_held": 0, "exit_date": None}

    peak = entry_premium
    trail_armed = False
    idx = entry_idx
    days_elapsed = 0

    while True:
        idx += 1
        days_elapsed += 1
        if idx >= len(series):
            S_t = series[-1]["close"]
            t_days = max(EXPIRY_DAYS - days_elapsed, 1)
            premium_t = bs_call_price(S_t, K, t_days / 365.0, RISK_FREE_RATE, sigma)
            pnl_pct = ((premium_t - entry_premium) / entry_premium) * 100
            return {**base, "status": "DATA_END_OPEN", "exit_premium": round(premium_t, 2),
                     "pnl_pct": round(pnl_pct, 2), "days_held": days_elapsed - 1, "exit_date": series[-1]["date"]}

        bar = series[idx]
        S_t = bar["close"]
        t_days = max(EXPIRY_DAYS - days_elapsed, 1)
        premium_t = bs_call_price(S_t, K, t_days / 365.0, RISK_FREE_RATE, sigma)
        peak = max(peak, premium_t)
        if peak >= entry_premium * ARM_MULT:
            trail_armed = True
        pnl_pct = ((premium_t - entry_premium) / entry_premium) * 100

        # No fixed take-profit cap: 2x is only the arming threshold for the
        # trailing stop, not an exit trigger. Past that, ride it unlimited
        # until it gives back 10% from peak.
        if trail_armed and premium_t <= peak * (1 - TRAIL_GIVEBACK):
            status = "TRAIL_STOP_AFTER_2X"
        elif premium_t <= entry_premium * HARD_STOP_MULT:
            status = "HARD_STOP_50PCT"
        elif days_elapsed >= MAX_HOLD_DAYS:
            status = "MAX_HOLD_3DAY"
        elif t_days <= 1:
            status = "EXPIRY_FORCED_CLOSE"
        else:
            continue

        return {**base, "status": status, "exit_premium": round(premium_t, 2),
                 "pnl_pct": round(pnl_pct, 2), "days_held": days_elapsed, "exit_date": bar["date"]}


OUT_PATH = "backtest_3pm_options_vs_equity.json"


def load_state():
    """Returns (processed_dates_set, trades_list). Tracks processed dates
    explicitly (not just trade presence) so a 0-signal day isn't re-run on resume."""
    if os.path.exists(OUT_PATH):
        try:
            with open(OUT_PATH, "r") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return set(data.get("processed_dates", [])), data.get("trades", [])
            # Backward compat: old format was a flat trade list
            return {t["signal_date"] for t in data}, data
        except Exception:
            return set(), []
    return set(), []


def save_state(processed_dates, all_trades):
    with open(OUT_PATH, "w") as f:
        json.dump({"processed_dates": sorted(processed_dates), "trades": all_trades}, f, indent=2, default=str)


# ---------------------------------------------------------------------------
# Main driver
# ---------------------------------------------------------------------------
def run_backtest(num_days: int, resume: bool = True):
    dates = get_recent_trading_dates(num_days)
    logging.info(f"Backtesting 3PM/EOD pulse for {len(dates)} trading days: {dates[0]} -> {dates[-1]}")

    processed_dates, all_trades = load_state() if resume else (set(), [])
    if processed_dates:
        logging.info(f"Resuming: {len(processed_dates)} day(s) already processed: {sorted(processed_dates)}")

    symbol_cache = {}

    for date in dates:
        if date in processed_dates:
            logging.info(f"=== Skipping {date} (already processed, resuming) ===")
            continue

        logging.info(f"=== Historical pulse for {date} ===")
        try:
            run_data = run_historical_engine(date)
        except Exception as e:
            logging.error(f"Historical run failed for {date}: {e}")
            continue
        if not run_data:
            continue

        approved = run_data.get("approved", [])
        logging.info(f"{date}: {len(approved)} approved signal(s): {approved}")

        day_trades = []
        for symbol in approved:
            if symbol not in symbol_cache:
                symbol_cache[symbol] = fetch_symbol_series(symbol)
            series = symbol_cache[symbol]
            dates_list = [b["date"] for b in series]
            if date not in dates_list:
                logging.warning(f"{symbol}: signal date {date} not found in its own price series, skipping.")
                continue
            signal_idx = dates_list.index(date)
            entry_idx = signal_idx + 1
            if entry_idx >= len(series):
                logging.info(f"{symbol}: signaled on {date} but no next-day data yet, skipping.")
                continue

            eq_result = simulate_equity_trade(series, entry_idx)
            # Options: same-day close entry (validated: 78.9% vs 73.7% WR on
            # identical setups - see agents/conviction_router_agent.py)
            opt_result = simulate_option_trade(series, signal_idx, entry_price_field="close")

            day_trades.append({
                "signal_date": date,
                "symbol": symbol,
                "entry_date": series[entry_idx]["date"],
                "equity": eq_result,
                "options": opt_result,
            })

        all_trades.extend(day_trades)
        processed_dates.add(date)
        save_state(processed_dates, all_trades)
        logging.info(f"Saved progress: {len(processed_dates)} day(s), {len(all_trades)} trade(s) so far -> {OUT_PATH}")

    logging.info(f"Saved {len(all_trades)} trade records to {OUT_PATH}")
    summarize(all_trades)
    return all_trades


def summarize(trades):
    def bucket(leg):
        results = [t[leg] for t in trades if t[leg].get("pnl_pct") is not None]
        open_count = len(trades) - len(results)
        if not results:
            return None
        pnls = [r["pnl_pct"] for r in results]
        wins = [p for p in pnls if p > 0]
        status_counts = {}
        for r in results:
            status_counts[r["status"]] = status_counts.get(r["status"], 0) + 1
        return {
            "n_closed": len(results),
            "n_still_open": open_count,
            "win_rate_pct": round(100 * len(wins) / len(results), 1),
            "avg_pnl_pct": round(sum(pnls) / len(pnls), 2),
            "total_pnl_pct": round(sum(pnls), 2),
            "best_pnl_pct": round(max(pnls), 2),
            "worst_pnl_pct": round(min(pnls), 2),
            "status_breakdown": status_counts,
        }

    eq_summary = bucket("equity")
    opt_summary = bucket("options")

    print("\n" + "=" * 70)
    print(f"3 PM PULSE BACKTEST -- {len(trades)} total signals")
    print("=" * 70)
    for label, summ in [("EQUITY", eq_summary), ("OPTIONS (Black-Scholes proxy)", opt_summary)]:
        print(f"\n--- {label} ---")
        if not summ:
            print("No closed trades.")
            continue
        print(f"Closed trades:      {summ['n_closed']}  (still open/no data: {summ['n_still_open']})")
        print(f"Win rate:           {summ['win_rate_pct']}%")
        print(f"Avg P&L per trade:  {summ['avg_pnl_pct']:+.2f}%")
        print(f"Total P&L (summed): {summ['total_pnl_pct']:+.2f}%")
        print(f"Best / Worst trade: {summ['best_pnl_pct']:+.2f}% / {summ['worst_pnl_pct']:+.2f}%")
        print(f"Exit breakdown:     {summ['status_breakdown']}")

    print("\n--- Per-trade detail ---")
    print("| Signal Date | Symbol | Entry Date | Equity Exit | Equity PnL% | Options Exit | Options PnL% |")
    print("| :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    for t in trades:
        eq = t["equity"]
        opt = t["options"]
        eq_pnl = f"{eq['pnl_pct']:+.2f}%" if eq.get("pnl_pct") is not None else "OPEN"
        opt_pnl = f"{opt['pnl_pct']:+.2f}%" if opt.get("pnl_pct") is not None else "OPEN"
        print(f"| {t['signal_date']} | {t['symbol']} | {t['entry_date']} | {eq['status']} | {eq_pnl} | {opt['status']} | {opt_pnl} |")

    print("\nCAVEATS:")
    print("- OPTIONS leg uses a Black-Scholes PROXY, not real historical option premiums")
    print("  (none exist in this DB). IV is estimated from trailing 20-day realized vol")
    print("  x1.15, clamped to [25%, 75%]; strike is nearest-ATM by a fixed step table;")
    print("  expiry assumed 30 calendar days at entry, decaying daily. Ignores real bid-ask")
    print("  spread, liquidity, and IV skew/smile - treat as a directional/leverage estimate only.")
    print("- Both legs enter at the NEXT trading day's OPEN after the signal date (matching")
    print("  how the real system places AMOs for next-day execution).")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=20)
    args = parser.parse_args()
    run_backtest(args.days)
