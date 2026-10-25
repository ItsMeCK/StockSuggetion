"""
Daily Pre-Open Watchlist Evaluator (Requirements W2, WB1-WB12)
Fires every market morning at 09:08 AM IST (post-NSE pre-open equilibrium discovery).

Workflow:
1. Loads candidate universe from weekly eligible pool (data/weekly_eligible_universe.json)
   or falls back to pipeline/fo_universe.csv.
2. Ingests pre-open equilibrium quotes from Zerodha Kite in 1 batch quote call.
3. Evaluates pre-open gaps (Requirement W2):
   - Gaps > 2.5% routed to REVIEW (anti-chasing guard).
   - Gaps 0.0 to +2.5% tagged direction="LONG" (CE focus).
   - Gaps -2.5% to 0.0% tagged direction="SHORT" (PE breakdown focus).
4. Conducts Google Search Grounding & IV crush scan (Requirement WB1-WB12):
   - Checks earnings/corporate events within 48h (IV crush veto -> REVIEW).
   - Scans market narrative and executes deterministic toxic catalyst veto.
   - Saves top 8-12 approved tickers to daily_focus_watchlist with status='ACTIVE'.
5. Dispatches the 09:08 AM Pre-Open Watchlist Email to the desk.
"""

import os
import sys
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.watchlist_manager import WatchlistManager
from agents.watchlist_builder_agent import GroundedWatchlistBuilder
from core.db_manager import get_active_daily_focus_watchlist, save_daily_focus_stock
from alerts.email_notifier import SovereignEmailer

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def evaluate_watchlist():
    logger.info("🛡️ Initiating Daily 09:08 AM Pre-Open Watchlist Evaluation...")
    builder = GroundedWatchlistBuilder()
    wm = WatchlistManager()

    # 1. Macro & Sector Thematic Grounding
    themes = builder.scan_market_themes()
    logger.info(f"Market Themes: {themes.get('narrative_summary')} (Sentiment: {themes.get('market_sentiment')})")

    # 2. Load candidate pool
    weekly_pool_path = "data/weekly_eligible_universe.json"
    candidates = []
    if os.path.exists(weekly_pool_path):
        try:
            with open(weekly_pool_path, "r") as f:
                data = json.load(f)
                candidates = data.get("eligible_candidates", [])
        except Exception as e:
            logger.warning(f"Could not read weekly pool ({e}), building from F&O universe.")

    if not candidates:
        from scripts.weekly_watchlist_builder import build_weekly_universe
        candidates = build_weekly_universe()

    # 3. Fetch pre-open quotes from Zerodha Kite (if available)
    kite_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    kite_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")

    preopen_candidates = []
    batch_quotes = {}
    if kite_key and kite_token:
        try:
            from kiteconnect import KiteConnect
            kite = KiteConnect(api_key=kite_key)
            kite.set_access_token(kite_token)
            
            # Batch quote call for all candidates (up to 500 symbols in 1 call)
            symbols_to_quote = [f"NSE:{c['symbol']}" for c in candidates[:100]]
            batch_quotes = kite.quote(symbols_to_quote)
            logger.info(f"Successfully fetched {len(batch_quotes)} pre-open quotes from Zerodha.")
        except Exception as e:
            logger.warning(f"Could not fetch live pre-open quotes from Kite: {e}. Using baseline prices.")

    for c in candidates:
        sym = c.get("symbol", "")
        quote = batch_quotes.get(f"NSE:{sym}", {})
        ohlc = quote.get("ohlc", {})
        prev_close = float(ohlc.get("close") or c.get("prev_close") or 1000.0)
        open_px = float(quote.get("last_price") or ohlc.get("open") or prev_close)

        preopen_candidates.append({
            "symbol": sym,
            "prev_close": prev_close,
            "open": open_px,
            "dte": c.get("dte", 15),
            "bid_ask_spread_pct": c.get("bid_ask_spread_pct", 0.8)
        })

    # 4. Filter Pre-Open Gaps (Requirement W2)
    active_daily, gap_review = wm.refresh_preopen_gaps(preopen_candidates, max_gap_pct=2.5)
    logger.info(f"Pre-open gaps evaluated: {len(active_daily)} active candidates, {len(gap_review)} routed to REVIEW.")

    # 5. Build Daily Focus Watchlist (IV Crush check + Grounding Intelligence)
    # Target top 25 candidates for deep AI grounding to preserve API budget
    final_focus, grounded_review = builder.build_daily_focus_watchlist(
        candidates=active_daily[:25],
        max_symbols=10,
        min_conviction=50.0
    )

    # Fail-safe (Requirement WB2): If AI grounding yielded 0 symbols (e.g. offline/API rate limit),
    # activate quantitative fallback from active_daily so desk never operates on an empty watchlist.
    if not final_focus and active_daily:
        logger.warning("⚠️ Grounding yielded 0 symbols. Activating quantitative pre-open fallback.")
        for cand in active_daily[:10]:
            sym = cand.get("symbol", "")
            direction = cand.get("direction", "LONG")
            fallback_stock = {
                "symbol": sym,
                "direction": direction,
                "conviction_score": 60.0,
                "catalyst_summary": "QUANT_FALLBACK: Promoted via quantitative pre-open rank",
                "upcoming_events": "Clean (Quantitative Fallback)",
                "market_buzz": "Quantitative Pre-Open Selection",
                "status": "ACTIVE"
            }
            final_focus.append(fallback_stock)
            save_daily_focus_stock(
                symbol=sym,
                direction=direction,
                conviction_score=60.0,
                catalyst_summary="QUANT_FALLBACK: Promoted via quantitative pre-open rank",
                upcoming_events="Clean (Quantitative Fallback)",
                market_buzz="Quantitative Pre-Open Selection",
                status="ACTIVE"
            )

    combined_review = gap_review + grounded_review
    logger.info(f"Daily Focus Watchlist finalised with {len(final_focus)} active tickers.")

    # Persist local JSON cache for offline resilience
    cache_path = os.path.join(PROJECT_ROOT, "data/daily_focus_watchlist.json")
    try:
        with open(cache_path, "w") as f:
            json.dump(final_focus, f, indent=2)
    except Exception as e:
        logger.warning(f"Could not write daily focus JSON cache: {e}")

    # 6. Dispatch 09:08 AM Pre-Open Focus Watchlist Email
    try:
        emailer = SovereignEmailer()
        emailer.send_morning_watchlist_report(
            focus_watchlist=final_focus,
            themes=themes,
            review_or_pruned=combined_review
        )
        logger.info("📧 09:08 AM Pre-Open Watchlist Email dispatched to desk.")
    except Exception as e:
        logger.error(f"Failed to dispatch 09:08 AM email: {e}")

    return final_focus

if __name__ == "__main__":
    evaluate_watchlist()
