import os
import sys
import math
import logging
import psycopg2
import polars as pl
from datetime import datetime, timezone
from dotenv import load_dotenv

# Add workspace root to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

load_dotenv()

# Enforce Historical mode to bypass freshness gate
os.environ["TRADING_MODE"] = "HISTORICAL"

from core.state import SovereignState
from pipeline.screener import SovereignScreener
from graph.builder import build_sovereign_graph

logging.basicConfig(level=logging.WARNING)  # Mute noisy logs

def get_next_day_candle(symbol: str, target_utc_str: str) -> dict:
    """
    Given a symbol and a target daily candle timestamp, fetches the target candle
    and the immediately following daily candle in the database to get next-day metrics.
    """
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
            user=os.getenv("POSTGRES_USER", "quant"),
            password=os.getenv("POSTGRES_PASSWORD", "quantpassword"),
            database=os.getenv("POSTGRES_DB", "market_data")
        )
        cur = conn.cursor()
        
        # Get target day's candle
        cur.execute(
            "SELECT time, open, high, low, close, volume FROM daily_ohlcv WHERE symbol = %s AND time = %s",
            (symbol, target_utc_str)
        )
        target_row = cur.fetchone()
        
        # Get next day's candle (the first row with time > target_utc_str)
        cur.execute(
            "SELECT time, open, high, low, close, volume FROM daily_ohlcv WHERE symbol = %s AND time > %s ORDER BY time ASC LIMIT 1",
            (symbol, target_utc_str)
        )
        next_row = cur.fetchone()
        
        cur.close()
        conn.close()
        
        if not target_row:
            return None
            
        res = {
            "entry_time": target_row[0].strftime("%Y-%m-%d"),
            "entry_price": float(target_row[4]),
        }
        
        if next_row:
            res.update({
                "next_time": next_row[0].strftime("%Y-%m-%d"),
                "next_open": float(next_row[1]),
                "next_high": float(next_row[2]),
                "next_low": float(next_row[3]),
                "next_close": float(next_row[4]),
            })
        else:
            res.update({
                "next_time": None,
                "next_open": None,
                "next_high": None,
                "next_low": None,
                "next_close": None,
            })
        return res
    except Exception as e:
        print(f"Error fetching candles for {symbol}: {e}")
        return None

def run_audit_for_date(target_date: str, target_utc_str: str):
    print(f"\n==========================================================================================")
    print(f"📅 AUDITING 3 PM RUN FOR: {target_date} (DB Timestamp: {target_utc_str})")
    print(f"==========================================================================================")
    
    # 0. Pre-Breakout Catalyst Screener
    from pipeline.catalyst_screener import EventCatalystScreener
    catalyst_screener = EventCatalystScreener()
    injected_catalysts = catalyst_screener.run(target_date=target_date)
    
    # 1. Screener
    screener = SovereignScreener()
    candidates, incubator, flagged_momentum, base_scores, macro_regime = screener.run_pipeline(target_date=target_date, injected_catalysts=injected_catalysts)
    
    # Combined candidates list
    all_candidates = list(set(candidates + incubator))
    
    print(f"Screener stage 2/incubator candidates: {all_candidates}")
    print(f"Screener flagged momentum candidates (stealth setups): {flagged_momentum}")
    print(f"Market Regime: {macro_regime.get('regime')}")
    
    if not all_candidates and not flagged_momentum:
        print("No candidates found by screener for this day.")
        return
        
    # 2. Build SovereignState
    initial_state = SovereignState(
        target_date=target_date,
        pulse=3,
        macro_regime=macro_regime.get("regime", "BULLISH"),
        candidates=all_candidates,
        incubator=incubator,
        flagged_momentum_candidates=flagged_momentum,
        breakouts=[],
        base_scores=base_scores,
        heuristic_flags={},
        experience_warnings={},
        vision_validations={},
        news_catalysts={},
        approved_allocations={},
        execution_telemetry={},
        error_log=[],
        debate_count=0
    )
    
    # 3. Compile and Run LangGraph Engine
    app = build_sovereign_graph({})
    final_state = app.invoke(initial_state)
    
    approved_allocs = final_state.get("approved_allocations", {})
    news_cats = final_state.get("news_catalysts", {})
    
    if not approved_allocs:
        print("❌ NO TRADES APPROVED BY THE ENGINE FOR THIS DAY.")
        return
        
    print("\n✅ APPROVED SUGGESTIONS AUDIT:")
    print(f"{'Ticker':<12} | {'Catalyst Type':<15} | {'Boost':<5} | {'Entry (3PM)':<10} | {'Next Open':<10} | {'Gap %':<7} | {'Next High':<10} | {'Max %':<7} | {'Next Close':<10} | {'Close %':<7}")
    print("-" * 115)
    
    for symbol, alloc in approved_allocs.items():
        # Get news catalyst details
        cat = news_cats.get(symbol, {})
        cat_type = cat.get("catalyst_type", "NONE")
        score_boost = cat.get("score_boost", 0.0)
        
        # Get candles
        candles = get_next_day_candle(symbol, target_utc_str)
        if not candles or not candles.get("next_open"):
            print(f"{symbol:<12} | {cat_type:<15} | {score_boost:<5} | No next-day market data found.")
            continue
            
        entry_price = candles["entry_price"]
        next_open = candles["next_open"]
        next_high = candles["next_high"]
        next_close = candles["next_close"]
        
        gap_pct = (next_open - entry_price) / entry_price * 100
        max_pct = (next_high - entry_price) / entry_price * 100
        close_pct = (next_close - entry_price) / entry_price * 100
        
        print(f"{symbol:<12} | {cat_type:<15} | {score_boost:<5.1f} | {entry_price:<10.2f} | {next_open:<10.2f} | {gap_pct:<+6.2f}% | {next_high:<10.2f} | {max_pct:<+6.2f}% | {next_close:<10.2f} | {close_pct:<+6.2f}%")
        
        # Calculate capital sizing scenarios
        # Scenario A: Exactly 1 Share
        pnl_1_share = next_close - entry_price
        
        # Scenario B: 5k Max or 1 Share Whichever is Lower
        # shares = min(floor(5000 / entry_price), 1)
        qty_b = min(math.floor(5000.0 / entry_price), 1)
        pnl_qty_b = qty_b * (next_close - entry_price)
        cost_qty_b = qty_b * entry_price
        
        print(f"   ↳ [1 Share Fixed] Cost: ₹{entry_price:.2f} | Close P&L: ₹{pnl_1_share:+.2f} ({close_pct:+.2f}%)")
        print(f"   ↳ [₹5k Max or 1 Share] Qty: {qty_b} | Cost: ₹{cost_qty_b:.2f} | Close P&L: ₹{pnl_qty_b:+.2f}")
        if cat.get("summary"):
            print(f"   ↳ Catalyst Summary: {cat.get('summary')}")
            print(f"   ↳ Expectations: {cat.get('meets_expectations')} | Priced-in: {cat.get('priced_in_status')}")

def main():
    # Last 4 trading days as strings, mapped to their specific database timestamp representations
    # Date mapping:
    # 2026-05-15 (Friday) -> UTC '2026-05-14 18:30:00+00'
    # 2026-05-18 (Monday) -> UTC '2026-05-17 18:30:00+00'
    # 2026-05-19 (Tuesday) -> UTC '2026-05-18 18:30:00+00'
    # 2026-05-20 (Wednesday) -> UTC '2026-05-19 18:30:00+00'
    
    historical_days = [
        ("2026-05-15", "2026-05-14 18:30:00+00"),
        ("2026-05-18", "2026-05-17 18:30:00+00"),
        ("2026-05-19", "2026-05-18 18:30:00+00")
    ]
    
    for date, utc_str in historical_days:
        run_audit_for_date(date, utc_str)

if __name__ == "__main__":
    main()
