import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from kiteconnect import KiteConnect

from core.db_manager import get_active_daily_focus_watchlist, get_daily_trades_count
from core.trigger_engine_5m import evaluate_5m_trigger, FiveMinuteBar
from core.live_trading import execute_trade
from core.watchlist_manager import WatchlistManager

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def run_5m_trigger_cycle(now_dt=None):
    """
    Executes the 5-Minute Precision Trigger Cycle:
    - Enforces max 5 trades per day hard ceiling.
    - Evaluates closed 5-minute bars on Active Focus Watchlist tickers.
    - Rate Limits: Batches quote requests and spaces historical data calls (0.35s sleep) to respect Zerodha limits.
    - Session Routing:
        * 09:30 - 12:14 PM: Morning Scalp Session (ENGINE_1_HOURLY -> 100% Paper Simulation, ₹0 live risk).
        * 12:15 - 15:15 PM: Afternoon Live Session (ENGINE_2_BTST -> Live Capital Authorized for overnight holds).
    - Dispatches confirmed momentum ignitions / breakdowns to execute_trade.
    """
    now = now_dt or datetime.now()
    logger.info(f"⚡ Running 5-Minute Precision Trigger Cycle at {now.strftime('%Y-%m-%d %H:%M:%S IST')}...")

    # 1. Hard Daily Trade Cap Check (Max 5 trades per day)
    trades_today = get_daily_trades_count()
    max_daily_trades = 5
    if trades_today >= max_daily_trades:
        logger.info(f"🛑 DAILY TRADES CEILING: Already executed {trades_today}/{max_daily_trades} trades today. No further entries permitted.")
        return

    logger.info(f"Daily Trades Count: {trades_today}/{max_daily_trades} slots consumed.")

    # 2. Fetch Active Daily Focus Watchlist
    active_stocks = get_active_daily_focus_watchlist(min_score=50.0)
    if not active_stocks:
        logger.info("Daily Focus Watchlist is currently empty. Run morning watchlist builder at 09:08 AM.")
        return

    logger.info(f"Evaluating {len(active_stocks)} Active Focus tickers: {[s['symbol'] for s in active_stocks]}")

    # 3. Initialize Kite Data Client
    api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    
    if not api_key or not access_token:
        logger.warning("Kite API credentials not set. Running in dry-run simulation mode.")
        return

    kite = KiteConnect(api_key=api_key)
    kite.set_access_token(access_token)

    # 4. Session Determination: Morning Paper Scalp (09:30-12:14) vs Afternoon Live BTST (12:15-15:15)
    import time
    from datetime import time as dt_time
    now_time = now.time()
    is_afternoon_session = (now_time >= dt_time(12, 15) and now_time <= dt_time(15, 15))

    holding_type = "TYPE_C_BTST" if is_afternoon_session else "TYPE_A_SCALP"
    engine_type = "ENGINE_2_BTST" if is_afternoon_session else "ENGINE_1_HOURLY"
    session_label = "LIVE AFTERNOON BTST" if is_afternoon_session else "MORNING PAPER SCALP (₹0 Live Risk)"
    logger.info(f"Session Profile: {session_label} | Engine: {engine_type} | Holding: {holding_type}")

    # 5. Batch Quote Fetching (Respect Zerodha single-call limit of 500 symbols)
    quote_keys = [f"NSE:{s['symbol']}" for s in active_stocks]
    try:
        batch_quotes = kite.quote(quote_keys)
    except Exception as e:
        logger.error(f"Error fetching batch quotes from Zerodha: {e}")
        batch_quotes = {}

    from_date = (now - timedelta(days=2)).strftime("%Y-%m-%d")
    to_date = now.strftime("%Y-%m-%d")

    # 6. Evaluate each symbol on closed 5-minute bars
    for idx, stock in enumerate(active_stocks):
        symbol = stock["symbol"]
        direction = stock.get("direction", "LONG")

        # Re-check daily cap before every trade
        if get_daily_trades_count() >= max_daily_trades:
            logger.info("Daily trades limit reached during execution loop. Halting further triggers.")
            break

        try:
            quote_key = f"NSE:{symbol}"
            quote_info = batch_quotes.get(quote_key, {})
            inst_token = quote_info.get("instrument_token")
            if not inst_token:
                logger.debug(f"{symbol}: No instrument token resolved from quote. Skipping.")
                continue

            # Respect Zerodha 3 req/sec limit on historical data API
            if idx > 0:
                time.sleep(0.35)

            # Fetch 5-minute candles (2 days = ~150 bars, optimal lookback)
            candles = kite.historical_data(inst_token, from_date, to_date, "5minute")
            if len(candles) < 22:
                continue

            # Target the last completed 5m bar (exclude in-progress bar)
            closed_bar_data = candles[-2]
            
            # Compute 20-period rolling metrics over previous 20 bars
            prev_bars = candles[-22:-2]
            sma_20 = sum(b["close"] for b in prev_bars) / len(prev_bars)
            vol_sma_20 = sum(b["volume"] for b in prev_bars) / len(prev_bars)

            bar = FiveMinuteBar(
                symbol=symbol,
                timestamp=closed_bar_data["date"],
                open=float(closed_bar_data["open"]),
                high=float(closed_bar_data["high"]),
                low=float(closed_bar_data["low"]),
                close=float(closed_bar_data["close"]),
                volume=int(closed_bar_data["volume"]),
                vol_sma_20=vol_sma_20,
                sma_20=sma_20
            )

            # Evaluate 5-minute trigger
            signal = evaluate_5m_trigger(bar)
            
            if signal.triggered:
                logger.info(f"🚀 5-MINUTE TRIGGER FIRED for {symbol}: {signal.direction} ({signal.instrument_type}) | {session_label} | Reason: {signal.reason}")
                
                # Execute Trade through dynamic risk framework
                execute_trade(
                    symbol=symbol,
                    score=int(stock.get("conviction_score", 70)),
                    catalyst=f"[{session_label}] {signal.reason}",
                    entry_time=now,
                    holding_type=holding_type,
                    engine_type=engine_type,
                    direction=signal.direction,
                    instrument_type=signal.instrument_type
                )
            else:
                logger.debug(f"{symbol}: No trigger ({signal.reason})")

        except Exception as e:
            logger.error(f"Error evaluating 5m candle for {symbol}: {e}")

if __name__ == "__main__":
    run_5m_trigger_cycle()
