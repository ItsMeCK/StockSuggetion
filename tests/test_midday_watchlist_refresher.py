import pytest
import os
import json
from unittest.mock import MagicMock, patch
from scripts.midday_watchlist_refresher import refresh_midday_watchlist

@patch("scripts.midday_watchlist_refresher.SovereignEmailer")
@patch("scripts.midday_watchlist_refresher.save_daily_focus_stock")
def test_midday_watchlist_refresher_selects_and_dispatches(mock_save, mock_emailer_cls):
    """
    Tests Level 3 Mid-Day Accumulation Sweep at 12:00 PM:
    Selects top candidates, tags direction (LONG/SHORT), saves to focus table, and dispatches email.
    """
    mock_emailer = MagicMock()
    mock_emailer_cls.return_value = mock_emailer

    result = refresh_midday_watchlist(top_n=10)

    assert len(result) > 0
    assert len(result) <= 10
    
    # Check structure
    for cand in result:
        assert "symbol" in cand
        assert cand["direction"] in ("LONG", "SHORT")
        assert cand["conviction_score"] > 0
        assert "catalyst_summary" in cand

    # Verify email dispatch was triggered
    mock_emailer.send_morning_watchlist_report.assert_called_once()
    assert os.path.exists("data/daily_focus_watchlist.json")
