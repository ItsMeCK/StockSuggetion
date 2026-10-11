import pytest
from datetime import datetime, time
from unittest.mock import MagicMock, patch

from scripts.btst_liquidator import round_to_tick, is_btst_position, run_btst_liquidator

def test_round_to_tick():
    assert round_to_tick(100.02) == 100.00
    assert round_to_tick(100.03) == 100.05
    assert round_to_tick(100.07) == 100.05
    assert round_to_tick(100.08) == 100.10

def test_is_btst_position():
    # Tagged with holding_type
    pos_tagged = {"holding_type": "TYPE_C_BTST", "symbol": "RELIANCE"}
    assert is_btst_position(pos_tagged) is True

    # Timestamp in 15:15 window
    pos_time = {"entry_time": "2026-10-10T15:15:30", "symbol": "INFY"}
    assert is_btst_position(pos_time) is True

    # Intraday 10:15 trade
    pos_intraday = {"entry_time": "2026-10-10T10:15:30", "symbol": "TCS"}
    assert is_btst_position(pos_intraday) is False

@patch("scripts.btst_liquidator.get_kite_data_client")
@patch("scripts.btst_liquidator.get_kite_exec_client")
@patch("scripts.btst_liquidator.SovereignEmailer")
@patch("scripts.btst_liquidator.get_all_active_positions")
@patch("scripts.btst_liquidator.close_position")
def test_btst_liquidator_profit_target(mock_close, mock_positions, mock_emailer, mock_exec, mock_data):
    # Setup mock active BTST position
    mock_positions.return_value = [{
        "order_id": "ORD123",
        "sl_order_id": "SL123",
        "holding_type": "TYPE_C_BTST",
        "symbol": "TATASTEEL",
        "option_symbol": "TATASTEEL26OCT150CE",
        "entry_premium": 10.0,
        "qty": 500
    }]
    
    # Mock live quote at 12.0 (+20% gap profit)
    kite_data_mock = MagicMock()
    kite_data_mock.quote.return_value = {
        "NFO:TATASTEEL26OCT150CE": {"last_price": 12.0}
    }
    mock_data.return_value = kite_data_mock

    kite_exec_mock = MagicMock()
    kite_exec_mock.positions.return_value = {"net": [{"tradingsymbol": "TATASTEEL26OCT150CE", "quantity": 500}]}
    kite_exec_mock.place_order.return_value = "EXIT_ORD_1"
    mock_exec.return_value = kite_exec_mock

    # Run at 09:16 AM IST
    run_time = datetime(2026, 10, 12, 9, 16, 0)
    exited = run_btst_liquidator(now=run_time, dry_run=True)

    assert len(exited) == 1
    assert exited[0]["symbol"] == "TATASTEEL"
    assert exited[0]["pnl_pct"] == 20.0
    assert "BTST Gap Profit Lock" in exited[0]["exit_reason"]
    mock_close.assert_called_once()

@patch("scripts.btst_liquidator.get_kite_data_client")
@patch("scripts.btst_liquidator.get_kite_exec_client")
@patch("scripts.btst_liquidator.SovereignEmailer")
@patch("scripts.btst_liquidator.get_all_active_positions")
@patch("scripts.btst_liquidator.close_position")
def test_btst_liquidator_0930_decay_exit(mock_close, mock_positions, mock_emailer, mock_exec, mock_data):
    # Flat position (+2% gain) evaluated at 09:30 AM
    mock_positions.return_value = [{
        "order_id": "ORD456",
        "sl_order_id": "SL456",
        "holding_type": "TYPE_C_BTST",
        "symbol": "SBIN",
        "option_symbol": "SBIN26OCT800CE",
        "entry_premium": 20.0,
        "qty": 750
    }]
    
    kite_data_mock = MagicMock()
    kite_data_mock.quote.return_value = {
        "NFO:SBIN26OCT800CE": {"last_price": 20.4} # +2%
    }
    mock_data.return_value = kite_data_mock

    kite_exec_mock = MagicMock()
    kite_exec_mock.positions.return_value = {"net": [{"tradingsymbol": "SBIN26OCT800CE", "quantity": 750}]}
    mock_exec.return_value = kite_exec_mock

    # Run at 09:30 AM IST (mandatory cutoff before edge decays to flat)
    run_time = datetime(2026, 10, 12, 9, 30, 0)
    exited = run_btst_liquidator(now=run_time, dry_run=True)

    assert len(exited) == 1
    assert exited[0]["symbol"] == "SBIN"
    assert "09:30 AM Edge Decay Cut" in exited[0]["exit_reason"]
    mock_close.assert_called_once()
