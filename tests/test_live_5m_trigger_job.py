import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
from scripts.live_5m_trigger_job import run_5m_trigger_cycle

@patch("scripts.live_5m_trigger_job.get_daily_trades_count")
@patch("scripts.live_5m_trigger_job.get_active_daily_focus_watchlist")
def test_5m_trigger_job_aborts_when_daily_cap_reached(mock_focus, mock_count):
    # Already 5 trades executed today
    mock_count.return_value = 5
    
    with patch("scripts.live_5m_trigger_job.execute_trade") as mock_exec:
        run_5m_trigger_cycle()
        mock_exec.assert_not_called()
        mock_focus.assert_not_called()

@patch("scripts.live_5m_trigger_job.get_daily_trades_count")
@patch("scripts.live_5m_trigger_job.get_active_daily_focus_watchlist")
@patch("scripts.live_5m_trigger_job.KiteConnect")
def test_5m_trigger_job_fires_and_executes(mock_kite_cls, mock_focus, mock_count):
    mock_count.return_value = 2  # 2 trades today, 3 slots open
    mock_focus.return_value = [
        {"symbol": "BHARTIARTL", "direction": "LONG", "conviction_score": 85.0}
    ]

    mock_kite = MagicMock()
    mock_kite_cls.return_value = mock_kite
    mock_kite.quote.return_value = {"NSE:BHARTIARTL": {"instrument_token": 12345}}
    
    # 25 candles with volume expansion and close near HOD
    candles = []
    base_time = datetime(2026, 10, 25, 11, 0)
    for i in range(25):
        candles.append({
            "date": base_time,
            "open": 1600.0,
            "high": 1605.0,
            "low": 1595.0,
            "close": 1602.0,
            "volume": 10000
        })
    # Target closed candle (-2): massive volume expansion 2.5x, close at high
    candles[-2] = {
        "date": base_time,
        "open": 1605.0,
        "high": 1630.0,
        "low": 1604.0,
        "close": 1629.0, # close_to_high > 0.85
        "volume": 25000  # 2.5x vol_sma_20
    }
    mock_kite.historical_data.return_value = candles

    with patch("scripts.live_5m_trigger_job.execute_trade") as mock_exec:
        run_5m_trigger_cycle()
        assert mock_exec.called
        args, kwargs = mock_exec.call_args
        assert kwargs["symbol"] == "BHARTIARTL"
        assert kwargs["direction"] == "LONG"
        assert kwargs["instrument_type"] == "CE"

@patch("scripts.live_5m_trigger_job.get_daily_trades_count", return_value=1)
@patch("scripts.live_5m_trigger_job.get_active_daily_focus_watchlist")
@patch("scripts.live_5m_trigger_job.KiteConnect")
def test_5m_trigger_job_morning_routes_to_paper_scalp(mock_kite_cls, mock_focus, mock_count):
    """
    Session Invariant:
    At 10:30 AM (Morning session), 5m triggers MUST route to:
    - engine_type: ENGINE_1_HOURLY (100% Paper Simulation)
    - holding_type: TYPE_A_SCALP
    - ₹0 live capital risk
    """
    mock_focus.return_value = [{"symbol": "INFY", "direction": "LONG", "conviction_score": 80.0}]
    mock_kite = MagicMock()
    mock_kite_cls.return_value = mock_kite
    mock_kite.quote.return_value = {"NSE:INFY": {"instrument_token": 999}}
    
    candles = []
    base_time = datetime(2026, 10, 25, 10, 25)
    for _ in range(25):
        candles.append({"date": base_time, "open": 1800.0, "high": 1805.0, "low": 1795.0, "close": 1802.0, "volume": 10000})
    candles[-2] = {"date": base_time, "open": 1805.0, "high": 1825.0, "low": 1804.0, "close": 1824.0, "volume": 30000}
    mock_kite.historical_data.return_value = candles

    with patch("scripts.live_5m_trigger_job.execute_trade") as mock_exec:
        run_5m_trigger_cycle(now_dt=datetime(2026, 10, 25, 10, 30))
        assert mock_exec.called
        _, kwargs = mock_exec.call_args
        assert kwargs["engine_type"] == "ENGINE_1_HOURLY"
        assert kwargs["holding_type"] == "TYPE_A_SCALP"
        assert "MORNING PAPER SCALP (₹0 Live Risk)" in kwargs["catalyst"]

@patch("scripts.live_5m_trigger_job.get_daily_trades_count", return_value=1)
@patch("scripts.live_5m_trigger_job.get_active_daily_focus_watchlist")
@patch("scripts.live_5m_trigger_job.KiteConnect")
def test_5m_trigger_job_afternoon_routes_to_live_btst(mock_kite_cls, mock_focus, mock_count):
    """
    Session Invariant:
    At 13:30 PM (Afternoon session), 5m triggers MUST route to:
    - engine_type: ENGINE_2_BTST (Live Capital Authorized)
    - holding_type: TYPE_C_BTST (Captures the +₹2.13L 80-trade overnight gap edge)
    """
    mock_focus.return_value = [{"symbol": "RELIANCE", "direction": "LONG", "conviction_score": 90.0}]
    mock_kite = MagicMock()
    mock_kite_cls.return_value = mock_kite
    mock_kite.quote.return_value = {"NSE:RELIANCE": {"instrument_token": 888}}
    
    candles = []
    base_time = datetime(2026, 10, 25, 13, 25)
    for _ in range(25):
        candles.append({"date": base_time, "open": 2800.0, "high": 2805.0, "low": 2795.0, "close": 2802.0, "volume": 10000})
    candles[-2] = {"date": base_time, "open": 2805.0, "high": 2835.0, "low": 2804.0, "close": 2834.0, "volume": 35000}
    mock_kite.historical_data.return_value = candles

    with patch("scripts.live_5m_trigger_job.execute_trade") as mock_exec:
        run_5m_trigger_cycle(now_dt=datetime(2026, 10, 25, 13, 30))
        assert mock_exec.called
        _, kwargs = mock_exec.call_args
        assert kwargs["engine_type"] == "ENGINE_2_BTST"
        assert kwargs["holding_type"] == "TYPE_C_BTST"
        assert "LIVE AFTERNOON BTST" in kwargs["catalyst"]
