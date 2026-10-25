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
