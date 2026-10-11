import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime
import pytz

from scripts.btst_fill_verifier import run_btst_fill_verifier


@pytest.fixture
def mock_ist_time():
    ist = pytz.timezone("Asia/Kolkata")
    return ist.localize(datetime(2026, 10, 11, 15, 26, 0))


def test_btst_fill_verifier_skips_outside_window():
    ist = pytz.timezone("Asia/Kolkata")
    outside_time = ist.localize(datetime(2026, 10, 11, 14, 0, 0))
    res = run_btst_fill_verifier(now=outside_time, force_check=False)
    assert res == []


@patch("scripts.btst_fill_verifier.get_kite_data_client")
@patch("scripts.btst_fill_verifier.get_kite_exec_client")
@patch("scripts.btst_fill_verifier.SovereignEmailer")
@patch("scripts.btst_fill_verifier.get_all_active_positions")
@patch("scripts.btst_fill_verifier.update_entry_fill")
def test_btst_fill_verifier_live_order_filled_with_slippage_and_missing_sl(
    mock_update_fill,
    mock_get_positions,
    mock_emailer_cls,
    mock_kite_exec_fn,
    mock_kite_data_fn,
    mock_ist_time
):
    mock_emailer = MagicMock()
    mock_emailer_cls.return_value = mock_emailer

    mock_exec = MagicMock()
    mock_exec.order_history.return_value = [
        {"order_id": "2410110001", "status": "COMPLETE", "average_price": 105.0}
    ]
    mock_exec.place_order.return_value = "SL_ORDER_999"
    mock_kite_exec_fn.return_value = mock_exec

    pos = {
        "order_id": "2410110001",
        "sl_order_id": "FAILED",
        "status": "LIVE_TRADED",
        "entry_time": "2026-10-11T15:15:00",
        "symbol": "RELIANCE",
        "option_symbol": "RELIANCE26OCT2600CE",
        "option_token": 12345,
        "spot_entry": 2590.0,
        "entry_premium": 100.0,
        "qty": 250,
        "score": 92,
        "catalyst": "INSIDER_ACCUMULATION",
        "current_sl": 85.0,
        "holding_type": "TYPE_C_BTST"
    }
    mock_get_positions.return_value = [pos]

    res = run_btst_fill_verifier(now=mock_ist_time, force_check=True)
    assert len(res) == 1
    assert res[0]["action"] == "VERIFIED_FILLED"
    assert res[0]["fill_price"] == 105.0
    assert res[0]["slippage_pct"] == 5.0
    assert res[0]["sl_order_id"] == "SL_ORDER_999"

    # Verify high slippage email alert triggered
    assert any(
        "HIGH SLIPPAGE ALERT: RELIANCE" in call[0][0] and "105.00" in call[0][1]
        for call in mock_emailer.send_live_alert.call_args_list
    )

    # Verify protective SL order placed on broker
    mock_exec.place_order.assert_called_once()
    assert mock_update_fill.called


@patch("scripts.btst_fill_verifier.get_kite_data_client")
@patch("scripts.btst_fill_verifier.get_kite_exec_client")
@patch("scripts.btst_fill_verifier.SovereignEmailer")
@patch("scripts.btst_fill_verifier.get_all_active_positions")
@patch("scripts.btst_fill_verifier.close_position")
def test_btst_fill_verifier_live_order_unfilled_cancelled(
    mock_close_pos,
    mock_get_positions,
    mock_emailer_cls,
    mock_kite_exec_fn,
    mock_kite_data_fn,
    mock_ist_time
):
    mock_emailer = MagicMock()
    mock_emailer_cls.return_value = mock_emailer

    mock_exec = MagicMock()
    mock_exec.order_history.return_value = [
        {"order_id": "2410110002", "status": "TRIGGER PENDING"}
    ]
    mock_kite_exec_fn.return_value = mock_exec

    pos = {
        "order_id": "2410110002",
        "sl_order_id": None,
        "status": "LIVE_TRADED",
        "entry_time": "2026-10-11T15:15:00",
        "symbol": "TCS",
        "option_symbol": "TCS26OCT3500CE",
        "option_token": 67890,
        "spot_entry": 3480.0,
        "entry_premium": 80.0,
        "qty": 175,
        "score": 90,
        "catalyst": "INSIDER_ACCUMULATION",
        "current_sl": 68.0,
        "holding_type": "TYPE_C_BTST"
    }
    mock_get_positions.return_value = [pos]

    res = run_btst_fill_verifier(now=mock_ist_time, force_check=True)
    assert len(res) == 1
    assert res[0]["action"] == "CANCELLED_UNFILLED"

    # Verify broker cancel order called
    mock_exec.cancel_order.assert_called_once_with(
        variety=mock_exec.VARIETY_REGULAR,
        order_id="2410110002"
    )

    # Verify position closed in DB
    mock_close_pos.assert_called_once_with(
        pos,
        exit_price=80.0,
        pnl_pct=0.0,
        status_override="CANCELLED_UNFILLED_AT_326"
    )

    # Verify email alert triggered
    assert mock_emailer.send_live_alert.called


@patch("scripts.btst_fill_verifier.get_kite_data_client")
@patch("scripts.btst_fill_verifier.get_kite_exec_client")
@patch("scripts.btst_fill_verifier.SovereignEmailer")
@patch("scripts.btst_fill_verifier.get_all_active_positions")
@patch("scripts.btst_fill_verifier.update_entry_fill")
def test_btst_fill_verifier_paper_trade_audit(
    mock_update_fill,
    mock_get_positions,
    mock_emailer_cls,
    mock_kite_exec_fn,
    mock_kite_data_fn,
    mock_ist_time
):
    mock_data = MagicMock()
    mock_data.quote.return_value = {
        "NFO:INFY26OCT1800CE": {"last_price": 52.5}
    }
    mock_kite_data_fn.return_value = mock_data

    pos = {
        "order_id": "PAPER_1728640500",
        "sl_order_id": "PAPER_SL_1728640500",
        "status": "PAPER_TRADED",
        "entry_time": "2026-10-11T15:15:00",
        "symbol": "INFY",
        "option_symbol": "INFY26OCT1800CE",
        "option_token": 11111,
        "spot_entry": 1800.0,
        "entry_premium": 50.0,
        "qty": 300,
        "score": 94,
        "catalyst": "INSIDER_ACCUMULATION",
        "current_sl": 0.0,
        "holding_type": "TYPE_C_BTST"
    }
    mock_get_positions.return_value = [pos]

    res = run_btst_fill_verifier(now=mock_ist_time, force_check=True)
    assert len(res) == 1
    assert res[0]["action"] == "VERIFIED_PAPER"
    assert res[0]["ltp"] == 52.5
    assert res[0]["current_sl"] == 42.5  # 50.0 * 0.85 = 42.5
    assert mock_update_fill.called
