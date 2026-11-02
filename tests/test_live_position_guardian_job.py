import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime
from scripts.live_position_guardian_job import run_hourly_guardian_audit

@patch("scripts.live_position_guardian_job.KiteConnect")
@patch("scripts.live_position_guardian_job.get_all_active_positions")
@patch("scripts.live_position_guardian_job.get_active_daily_focus_watchlist")
@patch("scripts.live_position_guardian_job.SovereignEmailer")
def test_run_hourly_guardian_audit_execution(mock_emailer_cls, mock_focus, mock_positions, mock_kite_cls, monkeypatch):
    monkeypatch.setenv("KITE_API_KEY", "dummy_key")
    monkeypatch.setenv("KITE_ACCESS_TOKEN", "dummy_token")
    mock_positions.return_value = [
        {
            "symbol": "CHOLAFIN",
            "direction": "LONG",
            "entry_price": 1500.0,
            "engine_type": "ENGINE_2_BTST",
            "instrument_token": "CHOLAFIN26NOV1500CE"
        }
    ]
    mock_focus.return_value = [
        {"symbol": "OFSS", "direction": "SHORT"}
    ]

    mock_kite = MagicMock()
    mock_kite_cls.return_value = mock_kite

    mock_emailer = MagicMock()
    mock_emailer_cls.return_value = mock_emailer

    mock_kite.quote.return_value = {
        "NSE:CHOLAFIN": {"instrument_token": 1001, "last_price": 1530.0},
        "NSE:OFSS": {"instrument_token": 1002, "last_price": 9900.0}
    }

    mock_kite.historical_data.return_value = [
        {
            "date": datetime(2026, 11, 2, 9, 15 + i),
            "open": 1500.0 + i, "high": 1535.0 + i, "low": 1495.0 + i, "close": 1530.0 + i,
            "volume": 25000 + i * 100
        }
        for i in range(10)
    ]

    run_hourly_guardian_audit(send_mail=True)

    assert mock_emailer.send_live_alert.called
    subject = mock_emailer.send_live_alert.call_args[0][0]
    assert "Hourly Live Position Guardian Briefing" in subject
    html = mock_emailer.send_live_alert.call_args[0][1]
    assert "CHOLAFIN" in html
    assert "OFSS" in html
