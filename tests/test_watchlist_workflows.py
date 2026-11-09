import pytest
import os
import json
from unittest.mock import MagicMock, patch
from scripts.weekly_watchlist_builder import build_weekly_universe
from core.watchlist_evaluator import evaluate_watchlist
from alerts.email_notifier import SovereignEmailer

@patch("scripts.weekly_watchlist_builder.SovereignEmailer")
@patch("scripts.weekly_watchlist_builder.GroundedWatchlistBuilder")
@patch("scripts.weekly_watchlist_builder.get_banned_fo_symbols", return_value={"SAIL", "GMRINFRA"})
def test_weekly_watchlist_builder_filters_bans_and_dispatches_email(mock_bans, mock_builder_cls, mock_emailer_cls):
    """
    Test Weekly Cron (Sunday 18:00 IST):
    Filters banned stocks, builds candidate universe, and dispatches weekly email.
    """
    mock_emailer = MagicMock()
    mock_emailer_cls.return_value = mock_emailer

    mock_builder = MagicMock()
    mock_builder_cls.return_value = mock_builder
    mock_builder.scan_market_themes.return_value = {
        "strong_sectors": ["Auto", "Pharma"],
        "weak_sectors": ["IT"],
        "market_sentiment": "BULLISH",
        "narrative_summary": "Strong industrial demand."
    }

    result = build_weekly_universe()
    symbols = [r["symbol"] for r in result]

    # Invariants:
    assert "SAIL" not in symbols
    assert "GMRINFRA" not in symbols
    assert len(symbols) > 100
    mock_emailer.send_weekly_universe_report.assert_called_once()
    assert os.path.exists("data/weekly_eligible_universe.json")

@patch("core.watchlist_evaluator.SovereignEmailer")
@patch("core.watchlist_evaluator.GroundedWatchlistBuilder")
@patch("core.watchlist_evaluator.WatchlistManager")
def test_daily_watchlist_evaluator_evaluates_gaps_and_dispatches_email(mock_wm_cls, mock_builder_cls, mock_emailer_cls):
    """
    Test Daily Cron (Mon-Fri 09:08 AM IST):
    Ingests pre-open quotes, evaluates gaps, filters IV crush / toxic catalysts, and sends morning email.
    """
    mock_emailer = MagicMock()
    mock_emailer_cls.return_value = mock_emailer

    mock_builder = MagicMock()
    mock_builder_cls.return_value = mock_builder
    mock_builder.scan_market_themes.return_value = {
        "strong_sectors": ["Auto", "Defense"],
        "weak_sectors": ["IT"],
        "market_sentiment": "BULLISH",
        "narrative_summary": "Auto earnings beat."
    }
    mock_builder.build_daily_focus_watchlist.return_value = (
        [
            {"symbol": "BHARTIARTL", "direction": "LONG", "conviction_score": 85.0, "catalyst_summary": "5G expansion"},
            {"symbol": "TATAMOTORS", "direction": "LONG", "conviction_score": 82.0, "catalyst_summary": "Commercial vehicle demand"}
        ],
        [
            {"symbol": "INFY", "status": "REVIEW", "route_reason": "Excessive pre-open gap 3.2%"}
        ]
    )

    mock_wm = MagicMock()
    mock_wm_cls.return_value = mock_wm
    mock_wm.refresh_preopen_gaps.return_value = (
        [{"symbol": "BHARTIARTL", "open": 1600.0, "prev_close": 1590.0, "dte": 15, "bid_ask_spread_pct": 0.8}],
        [{"symbol": "INFY", "open": 1860.0, "prev_close": 1800.0, "dte": 15, "bid_ask_spread_pct": 0.8}]
    )

    final_focus = evaluate_watchlist()
    assert len(final_focus) == 2
    assert final_focus[0]["symbol"] == "BHARTIARTL"
    mock_emailer.send_morning_watchlist_report.assert_called_once()

@patch("core.watchlist_evaluator.SovereignEmailer")
@patch("core.watchlist_evaluator.GroundedWatchlistBuilder")
@patch("core.watchlist_evaluator.WatchlistManager")
def test_daily_watchlist_evaluator_fallback_ranks_quantitatively_not_alphabetically(mock_wm_cls, mock_builder_cls, mock_emailer_cls):
    """
    Test that when AI grounding yields 0 symbols, the fallback promotes candidates
    ranked by sector alignment and ATR%, NEVER by alphabetical order.
    """
    mock_emailer_cls.return_value = MagicMock()
    mock_builder = MagicMock()
    mock_builder_cls.return_value = mock_builder
    mock_builder.scan_market_themes.return_value = {
        "strong_sectors": ["Auto"],
        "weak_sectors": ["IT"],
        "market_sentiment": "NEUTRAL",
        "narrative_summary": "Auto demand firm."
    }
    # Simulate grounding yielding 0 symbols
    mock_builder.build_daily_focus_watchlist.return_value = ([], [])

    mock_wm = MagicMock()
    mock_wm_cls.return_value = mock_wm
    # Alphabetical candidates with varying ATR and sectors
    mock_wm.refresh_preopen_gaps.return_value = (
        [
            {"symbol": "A_LOW_ATR", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 1.0, "sector": "NIFTY FMCG"},
            {"symbol": "B_AUTO_HIGH_ATR", "open": 200.0, "prev_close": 200.0, "dte": 15, "atr_pct": 3.8, "sector": "NIFTY AUTO"},
            {"symbol": "C_IT_HIGH_ATR", "open": 300.0, "prev_close": 300.0, "dte": 15, "atr_pct": 3.5, "sector": "NIFTY IT"},
            {"symbol": "Z_AUTO_TOP_ATR", "open": 400.0, "prev_close": 400.0, "dte": 15, "atr_pct": 4.5, "sector": "NIFTY AUTO"},
        ],
        []
    )

    final_focus = evaluate_watchlist()
    assert len(final_focus) > 0
    # Top candidate should be the highest ATR sector-aligned stock ('Z_AUTO_TOP_ATR'), NOT 'A_LOW_ATR'!
    first_sym = final_focus[0]["symbol"]
    assert first_sym != "A_LOW_ATR", "Fallback must not pick alphabetically!"
    assert first_sym in ["Z_AUTO_TOP_ATR", "B_AUTO_HIGH_ATR"]

def test_email_notifier_renders_active_watchlist():
    """
    Test that SovereignEmailer builds an institutional HTML card for the active focus watchlist.
    """
    emailer = SovereignEmailer()
    watchlist_sample = [
        {"symbol": "HAL", "direction": "LONG", "conviction_score": 92.0, "catalyst_summary": "Major defense contract"},
        {"symbol": "TCS", "direction": "SHORT", "conviction_score": 75.0, "catalyst_summary": "Margin compression"}
    ]
    html = emailer.build_watchlist_html(watchlist_sample)
    
    assert "Active Focus Watchlist" in html
    assert "HAL" in html
    assert "TCS" in html
    assert "LONG &bull; CE" in html
    assert "SHORT &bull; PE" in html
    assert "92" in html

@patch("core.watchlist_evaluator.SovereignEmailer")
@patch("core.watchlist_evaluator.GroundedWatchlistBuilder")
@patch("core.watchlist_evaluator.WatchlistManager")
def test_daily_watchlist_evaluator_guarantees_5_longs_and_5_shorts(mock_wm_cls, mock_builder_cls, mock_emailer_cls):
    """
    Test that when AI grounding returns fewer than 5 longs or 5 shorts,
    the engine pads with the top clean quantitative sector leaders to maintain 5+5 balance.
    """
    mock_emailer_cls.return_value = MagicMock()
    mock_builder = MagicMock()
    mock_builder_cls.return_value = mock_builder
    mock_builder.scan_market_themes.return_value = {
        "strong_sectors": ["Auto"],
        "weak_sectors": ["IT"],
        "market_sentiment": "NEUTRAL",
        "narrative_summary": "Auto firm, IT weak"
    }
    # Simulate AI approving 2 longs and 1 short, pruning 1 toxic long
    mock_builder.build_daily_focus_watchlist.return_value = (
        [
            {"symbol": "MARUTI", "direction": "LONG", "conviction_score": 75.0, "catalyst_summary": "Auto beat"},
            {"symbol": "HEROMOTOCO", "direction": "LONG", "conviction_score": 70.0, "catalyst_summary": "Auto beat 2"},
            {"symbol": "WIPRO", "direction": "SHORT", "conviction_score": 65.0, "catalyst_summary": "IT miss"},
        ],
        [
            {"symbol": "TIINDIA", "status": "PRUNED", "route_reason": "ABSOLUTE_VETO: Toxic regulatory fraud"}
        ]
    )

    mock_wm = MagicMock()
    mock_wm_cls.return_value = mock_wm
    mock_wm.refresh_preopen_gaps.return_value = (
        [
            {"symbol": "MARUTI", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 3.0, "sector": "NIFTY AUTO"},
            {"symbol": "HEROMOTOCO", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 2.9, "sector": "NIFTY AUTO"},
            {"symbol": "TVSMOTOR", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 2.8, "sector": "NIFTY AUTO"},
            {"symbol": "BAJAJ-AUTO", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 2.7, "sector": "NIFTY AUTO"},
            {"symbol": "EICHERMOT", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 2.6, "sector": "NIFTY AUTO"},
            {"symbol": "TIINDIA", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 3.5, "sector": "NIFTY AUTO"},
            {"symbol": "WIPRO", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 3.0, "sector": "NIFTY IT"},
            {"symbol": "HCLTECH", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 2.9, "sector": "NIFTY IT"},
            {"symbol": "TECHM", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 2.8, "sector": "NIFTY IT"},
            {"symbol": "LTM", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 2.7, "sector": "NIFTY IT"},
            {"symbol": "PERSISTENT", "open": 100.0, "prev_close": 100.0, "dte": 15, "atr_pct": 2.6, "sector": "NIFTY IT"},
        ],
        []
    )

    final_focus = evaluate_watchlist()
    assert len(final_focus) == 10
    longs = [c for c in final_focus if c["direction"] == "LONG"]
    shorts = [c for c in final_focus if c["direction"] == "SHORT"]
    assert len(longs) == 5
    assert len(shorts) == 5
    # TIINDIA must be excluded due to PRUNED / ABSOLUTE_VETO
    assert "TIINDIA" not in [c["symbol"] for c in final_focus]
    # AI approved take priority
    assert final_focus[0]["symbol"] == "MARUTI"

