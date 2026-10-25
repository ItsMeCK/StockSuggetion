import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from core.db_manager import close_position, get_cumulative_fy_turnover

def test_close_position_calculates_and_stores_turnover_and_statutory_charges():
    """
    Verifies that close_position calculates Section 43(5) turnover = |Exit - Entry| * qty,
    computes exact statutory friction, and records them in the database update.
    """
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    pos = {
        "order_id": "TEST_ORDER_123",
        "entry_premium": 100.0,
        "qty": 500,
        "symbol": "RELIANCE"
    }
    exit_price = 120.0
    pnl_pct = 20.0

    with patch("core.db_manager.get_db_connection", return_value=mock_conn):
        close_position(pos, exit_price, pnl_pct, status_override="CLOSED")

    assert mock_cursor.execute.called
    query, params = mock_cursor.execute.call_args[0]
    assert "UPDATE trades" in query
    assert "turnover = %s" in query
    assert "statutory_charges = %s" in query
    assert "net_pnl = %s" in query

    # Expected calculations:
    # Turnover = |120 - 100| * 500 = 20 * 500 = 10000.0
    # Gross PnL = 10000.0
    # Charges: Brokerage=40, STT = 120 * 500 * 0.0015 = 90.0, etc.
    turnover_param = params[4]
    charges_param = params[5]
    net_pnl_param = params[6]

    assert turnover_param == 10000.0
    assert charges_param > 130.0  # Brokerage (40) + STT (90) + GST + NSE fees
    assert net_pnl_param < 10000.0
    assert net_pnl_param == pytest.approx(10000.0 - charges_param, abs=0.01)

def test_get_cumulative_fy_turnover():
    """
    Verifies that get_cumulative_fy_turnover queries the trades table for the correct
    Financial Year dates (April 1 to March 31) and calculates the percentage of the
    Section 44AB ₹10 Crore threshold.
    """
    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    
    # Mock row returned from DB
    mock_cursor.fetchone.return_value = {
        "total_turnover": 5000000.0,  # ₹50 Lakhs
        "total_charges": 15000.0,
        "total_net_pnl": 120000.0,
        "total_closed_trades": 45
    }

    test_date = datetime(2026, 10, 25)
    with patch("core.db_manager.get_db_connection", return_value=mock_conn):
        ledger = get_cumulative_fy_turnover(as_of_date=test_date)

    assert ledger["fy_start"] == "2026-04-01"
    assert ledger["fy_end"] == "2027-03-31"
    assert ledger["cumulative_turnover"] == 5000000.0
    assert ledger["statutory_charges"] == 15000.0
    assert ledger["net_pnl"] == 120000.0
    assert ledger["closed_trades"] == 45
    assert ledger["audit_threshold"] == 100000000.0
    assert ledger["turnover_pct_of_threshold"] == 5.0
    assert ledger["requires_audit"] is False
