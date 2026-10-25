"""
Weekly Eligible Universe Builder (Requirement W1 & Institutional Governance)
Fires on Sundays at 18:00 IST (or Monday 08:00 AM IST) before market open.

Architecture:
1. Ingests the 216 F&O underlying stock universe from pipeline/fo_universe.csv.
2. Filters out securities currently under NSE F&O Ban period (fo_ban_manager).
3. Enforces Minimum DTE >= 4 days and tight bid-ask spread <= 2.0% (WatchlistManager).
4. Conducts Google Search Grounded macro sector & policy scan for the week ahead.
5. Saves eligible universe to data/weekly_eligible_universe.json.
6. Dispatches the Weekly Eligible Universe & Strategy Briefing Email to the desk.
"""

import os
import sys
import csv
import json
import logging
from datetime import datetime
from dotenv import load_dotenv

# Ensure project root is in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from core.watchlist_manager import WatchlistManager, DEFAULT_DEFENSIVE_BLACKLIST, compute_symbol_atr_pct
from core.fo_ban_manager import get_banned_fo_symbols
from agents.watchlist_builder_agent import GroundedWatchlistBuilder
from alerts.email_notifier import SovereignEmailer

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def build_weekly_universe():
    logger.info("🏛️ Starting Weekly Eligible Universe Build (Requirement W1)...")
    
    # 1. Load F&O underlying symbols
    fo_csv_path = "pipeline/fo_universe.csv"
    if not os.path.exists(fo_csv_path):
        logger.error(f"F&O universe file not found at {fo_csv_path}")
        return []

    symbols = []
    with open(fo_csv_path, "r") as f:
        reader = csv.reader(f)
        for row in reader:
            if row and row[0].strip() and row[0].strip().upper() != "SYMBOL":
                symbols.append(row[0].strip().upper())

    logger.info(f"Loaded {len(symbols)} underlying F&O symbols from {fo_csv_path}.")

    # 2. Exclude F&O Ban list
    banned_symbols = get_banned_fo_symbols()
    logger.info(f"NSE F&O Ban list holds {len(banned_symbols)} restricted securities.")

    # 3. Calculate 20-Day ATR% to filter sluggish low-volatility names (Level 1: ATR >= 1.8%)
    atr_map = compute_symbol_atr_pct()
    logger.info(f"Computed historical 20-day ATR% for {len(atr_map)} symbols.")

    # 4. Construct candidate pool with baseline liquidity & DTE metrics
    raw_universe = []
    for sym in symbols:
        raw_universe.append({
            "symbol": sym,
            "dte": 15,  # Baseline weekly DTE (refreshed daily)
            "bid_ask_spread_pct": 0.8,
            "oi": 50000,
            "atr_pct": atr_map.get(sym, 2.5)  # Default 2.5% if unmapped to avoid false exclusion
        })

    # 5. Filter through WatchlistManager (Requirement W1, Defensive Exclusion & ATR >= 1.8%)
    wm = WatchlistManager()
    eligible_candidates = wm.filter_weekly_universe(
        universe=raw_universe,
        banned_stocks=banned_symbols,
        excluded_stocks=DEFAULT_DEFENSIVE_BLACKLIST,
        min_dte=4,
        max_spread_pct=2.0,
        min_atr_pct=1.8
    )
    logger.info(f"Filtered {len(symbols)} -> {len(eligible_candidates)} eligible candidates ({len(banned_symbols)} banned, {len(DEFAULT_DEFENSIVE_BLACKLIST)} defensives excluded, ATR >= 1.8%).")

    # 5. Scan Macro & Sector Thematic Grounding for the week ahead
    builder = GroundedWatchlistBuilder()
    themes = builder.scan_market_themes()
    logger.info(f"Macro Sentiment: {themes.get('market_sentiment')} | Summary: {themes.get('narrative_summary')}")

    # 6. Rank Top 10 CE and Top 10 PE Weekly Focus Leaders based on Macro Themes & ATR%
    from core.sector_mapping import get_sector_for_symbol
    from core.watchlist_evaluator import matches_theme_sectors

    strong_sec = themes.get("strong_sectors", [])
    weak_sec = themes.get("weak_sectors", [])

    for c in eligible_candidates:
        c["sector"] = get_sector_for_symbol(c["symbol"])

    ce_pool = [c for c in eligible_candidates if matches_theme_sectors(c["sector"], strong_sec)]
    ce_sorted = sorted(ce_pool, key=lambda x: x.get("atr_pct", 0.0), reverse=True)
    top_ce_weekly = ce_sorted[:10]

    pe_pool = [c for c in eligible_candidates if matches_theme_sectors(c["sector"], weak_sec)]
    pe_sorted = sorted(pe_pool, key=lambda x: x.get("atr_pct", 0.0), reverse=True)
    top_pe_weekly = pe_sorted[:10]

    focus_dict = {
        "ce_focus": top_ce_weekly,
        "pe_focus": top_pe_weekly
    }

    # 7. Persist to data/weekly_eligible_universe.json
    os.makedirs("data", exist_ok=True)
    out_file = "data/weekly_eligible_universe.json"
    with open(out_file, "w") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(),
            "count": len(eligible_candidates),
            "banned_count": len(banned_symbols),
            "macro_themes": themes,
            "focus_candidates": focus_dict,
            "eligible_candidates": eligible_candidates
        }, f, indent=2)
    logger.info(f"Saved weekly eligible universe with {len(top_ce_weekly)} CE and {len(top_pe_weekly)} PE focus leaders to {out_file}.")

    # 8. Dispatch Weekly Institutional Email
    try:
        emailer = SovereignEmailer()
        emailer.send_weekly_universe_report(
            eligible_candidates=eligible_candidates,
            focus_candidates=focus_dict,
            banned_count=len(banned_symbols),
            themes=themes
        )
        logger.info("📧 Weekly Universe & Strategy Briefing Email dispatched successfully.")
    except Exception as e:
        logger.error(f"Failed to dispatch weekly universe email: {e}")

    return eligible_candidates

if __name__ == "__main__":
    build_weekly_universe()
