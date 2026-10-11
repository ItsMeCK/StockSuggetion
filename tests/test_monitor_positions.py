import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

from scripts.monitor_positions import monitor_positions

@patch("scripts.monitor_positions.get_all_active_positions")
@patch("scripts.monitor_positions.get_kite_data_client")
@patch("scripts.monitor_positions.get_kite_exec_client")
@patch("scripts.monitor_positions.SovereignEmailer")
def test_monitor_positions_empty(mock_emailer, mock_exec, mock_data, mock_get_positions):
    mock_get_positions.return_value = []
    # Should exit cleanly without calling quote or orders
    monitor_positions()
    mock_data.return_value.quote.assert_not_called()

@patch("scripts.monitor_positions.update_highest_oi")
@patch("scripts.monitor_positions.close_position")
@patch("scripts.monitor_positions.get_all_active_positions")
@patch("scripts.monitor_positions.get_kite_data_client")
@patch("scripts.monitor_positions.get_kite_exec_client")
@patch("scripts.monitor_positions.SovereignEmailer")
def test_monitor_positions_triggers_hard_stop(mock_emailer, mock_exec, mock_data, mock_get_positions, mock_close, mock_update_oi):
    pos = {
        "order_id": "ORD_101",
        "sl_order_id": "SL_101",
        "option_symbol": "RELIANCE26OCT2800CE",
        "symbol": "RELIANCE",
        "entry_premium": 100.0,
        "qty": 250,
        "current_sl": 85.0,
        "highest_high": 100.0,
        "trailing_active": False,
        "profit_locked": False,
        "holding_type": "TYPE_A_SCALP"
    }
    mock_get_positions.return_value = [pos]
    
    mock_exec.return_value.orders.return_value = []
    mock_exec.return_value.positions.return_value = {"net": [{"tradingsymbol": "RELIANCE26OCT2800CE", "quantity": 250}]}
    
    # Premium drops to 84.0 (-16% loss, breaches -15% floor)
    mock_data.return_value.quote.return_value = {
        "NFO:RELIANCE26OCT2800CE": {"last_price": 84.0, "oi": 50000},
        "NSE:RELIANCE": {"last_price": 2780.0, "average_price": 2790.0}
    }

    monitor_positions()

    # close_position must be called with HARD_STOP
    mock_close.assert_called_once()
    args, kwargs = mock_close.call_args
    assert args[0] == pos
    assert args[1] == 84.0
    assert args[2] == -16.0
    assert "HARD_STOP" in args[3]

@patch("scripts.monitor_positions.update_highest_oi")
@patch("scripts.monitor_positions.update_position_sl")
@patch("scripts.monitor_positions.update_position_profit_locked")
@patch("scripts.monitor_positions.get_all_active_positions")
@patch("scripts.monitor_positions.get_kite_data_client")
@patch("scripts.monitor_positions.get_kite_exec_client")
@patch("scripts.monitor_positions.SovereignEmailer")
def test_monitor_positions_triggers_profit_lock(mock_emailer, mock_exec, mock_data, mock_get_positions, mock_update_lock, mock_update_sl, mock_update_oi):
    pos = {
        "order_id": "ORD_102",
        "sl_order_id": "SL_102",
        "option_symbol": "TCS26OCT3500CE",
        "symbol": "TCS",
        "entry_premium": 100.0,
        "qty": 175,
        "current_sl": 85.0,
        "highest_high": 100.0,
        "trailing_active": False,
        "profit_locked": False,
        "holding_type": "TYPE_C_BTST"
    }
    mock_get_positions.return_value = [pos]

    mock_exec.return_value.orders.return_value = []
    mock_exec.return_value.positions.return_value = {"net": [{"tradingsymbol": "TCS26OCT3500CE", "quantity": 175}]}

    # Premium surges to 122.0 (+22% gain, triggers +20% Tier 1)
    mock_data.return_value.quote.return_value = {
        "NFO:TCS26OCT3500CE": {"last_price": 122.0, "oi": 60000},
        "NSE:TCS": {"last_price": 3520.0, "average_price": 3500.0}
    }

    monitor_positions()

    # SL must be ratcheted to 115.0 (+15%) and profit_locked set to True
    mock_update_sl.assert_called_once()
    sl_args, sl_kwargs = mock_update_sl.call_args
    assert sl_args[0] == "ORD_102"
    assert sl_args[1] == 115.0 # Ratcheted SL
    assert sl_kwargs.get("trailing_active") is True or (len(sl_args) > 3 and sl_args[3] is True)

    mock_update_lock.assert_called_once_with("ORD_102", profit_locked=True)
