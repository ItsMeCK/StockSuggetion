import pytest
from unittest.mock import MagicMock, patch
from core.sovereign_execution_gateway import SovereignExecutionGateway

def test_gateway_enforces_lot_size_ceiling_under_600():
    gateway = SovereignExecutionGateway()
    
    # 500 qty (< 600) -> Allowed
    ok, msg = gateway.check_lot_size(500)
    assert ok is True
    
    # 600 qty -> Rejected
    ok, msg = gateway.check_lot_size(600)
    assert ok is False
    assert "exceeds automated ceiling" in msg
    
    # 1875 qty (National Aluminium) -> Rejected
    ok, msg = gateway.check_lot_size(1875)
    assert ok is False
    assert "exceeds automated ceiling" in msg
    
    # 3125 qty (Nykaa) -> Rejected
    ok, msg = gateway.check_lot_size(3125)
    assert ok is False

def test_gateway_calculates_marketable_limit_price_with_tick_rounding():
    gateway = SovereignExecutionGateway()
    
    # Entry premium 28.50, best ask 28.60 -> min(28.65, 28.50 * 1.015 = 28.9275) -> 28.65
    price = gateway.calculate_marketable_limit_price(28.50, best_ask=28.60)
    assert price == 28.65
    # Must be rounded to 0.05
    assert round(price % 0.05, 4) in (0.0, 0.05)

def test_gateway_margin_shortfall_triggers_rejection_and_email():
    mock_kite = MagicMock()
    mock_emailer = MagicMock()
    
    # Available cash ₹10,000, required ₹25,000
    mock_kite.margins.return_value = {
        "available": {"live_balance": 10000.0, "cash": 10000.0}
    }
    
    gateway = SovereignExecutionGateway(kite_exec=mock_kite, emailer=mock_emailer)
    
    result = gateway.execute_option_order(
        symbol="RELIANCE",
        option_symbol="RELIANCE26NOV1300CE",
        lot_size=250,
        entry_premium=100.0  # Required: ₹25,000
    )
    
    assert result["success"] is False
    assert result["error"] == "INSUFFICIENT_FUNDS"
    mock_kite.place_order.assert_not_called()
    mock_emailer.send_live_alert.assert_called_once()
    
    call_args = mock_emailer.send_live_alert.call_args[0]
    subject = call_args[0]
    assert "[ORDER FAILED] [INSUFFICIENT FUNDS]" in subject
    assert "RELIANCE" in subject

def test_gateway_order_unfilled_after_10s_cancels_and_notifies():
    mock_kite = MagicMock()
    mock_emailer = MagicMock()
    
    # Sufficient margin
    mock_kite.margins.return_value = {
        "available": {"live_balance": 50000.0}
    }
    mock_kite.place_order.return_value = "ORD_UNFILLED_123"
    
    # Order remains OPEN during 10s check
    mock_kite.order_history.return_value = [
        {"status": "OPEN", "status_message": "Pending in market"}
    ]
    
    gateway = SovereignExecutionGateway(kite_exec=mock_kite, emailer=mock_emailer)
    
    # Run with short timeout for fast unit testing
    with patch.object(gateway, "verify_order_fill_after_10s") as mock_verify:
        mock_verify.return_value = {
            "status": "OPEN",
            "average_price": 0.0,
            "status_message": "",
            "unfilled_reason": "Order remained unfilled after 10-second verification window"
        }
        
        result = gateway.execute_option_order(
            symbol="INFY",
            option_symbol="INFY26NOV1800CE",
            lot_size=400,
            entry_premium=25.0
        )
        
        assert result["success"] is False
        assert result["status"] == "CANCELLED_TIMEOUT"
        mock_kite.cancel_order.assert_called_once_with(
            variety=mock_kite.VARIETY_REGULAR,
            order_id="ORD_UNFILLED_123"
        )
        mock_emailer.send_live_alert.assert_called_once()
        subject = mock_emailer.send_live_alert.call_args[0][0]
        assert "[ORDER FAILED]" in subject
        assert "UNFILLED_10S_TIMEOUT" in subject

def test_gateway_order_filled_places_sl_and_sends_executed_email():
    mock_kite = MagicMock()
    mock_emailer = MagicMock()
    
    mock_kite.margins.return_value = {
        "available": {"live_balance": 50000.0}
    }
    mock_kite.place_order.side_effect = ["ORD_BUY_456", "ORD_SL_456"]
    
    gateway = SovereignExecutionGateway(kite_exec=mock_kite, emailer=mock_emailer)
    
    with patch.object(gateway, "verify_order_fill_after_10s") as mock_verify, \
         patch("core.sovereign_execution_gateway.save_active_position") as mock_save:
        
        mock_verify.return_value = {
            "status": "COMPLETE",
            "average_price": 25.20,
            "status_message": "",
            "unfilled_reason": ""
        }
        
        result = gateway.execute_option_order(
            symbol="BHARATFORG",
            option_symbol="BHARATFORG26NOV1720CE",
            lot_size=500,
            entry_premium=25.0
        )
        
        assert result["success"] is True
        assert result["status"] == "COMPLETE"
        assert result["fill_price"] == 25.20
        assert result["order_id"] == "ORD_BUY_456"
        assert result["sl_order_id"] == "ORD_SL_456"
        
        # Invariant: place_order called twice (BUY, then SL upon complete)
        assert mock_kite.place_order.call_count == 2
        mock_save.assert_called_once()
        mock_emailer.send_live_alert.assert_called_once()
        
        subject = mock_emailer.send_live_alert.call_args[0][0]
        assert "[ORDER EXECUTED]" in subject
        assert "BHARATFORG" in subject

def test_gateway_amo_order_stays_open_without_cancellation():
    mock_kite = MagicMock()
    mock_kite.VARIETY_AMO = "amo"
    mock_kite.margins.return_value = {
        "available": {"live_balance": 50000.0}
    }
    mock_kite.place_order.return_value = "ORD_AMO_789"
    
    gateway = SovereignExecutionGateway(kite_exec=mock_kite)
    
    result = gateway.execute_option_order(
        symbol="NIFTY",
        option_symbol="NIFTY26N0323000PE",
        lot_size=65,
        entry_premium=0.5,
        is_amo=True
    )
    
    assert result["success"] is True
    assert result["status"] == "OPEN"
    assert result["order_id"] == "ORD_AMO_789"
    assert result["variety"] == "amo"
    mock_kite.place_order.assert_called_once_with(
        variety="amo",
        exchange=mock_kite.EXCHANGE_NFO,
        tradingsymbol="NIFTY26N0323000PE",
        transaction_type=mock_kite.TRANSACTION_TYPE_BUY,
        quantity=65,
        product=mock_kite.PRODUCT_NRML,
        order_type=mock_kite.ORDER_TYPE_LIMIT,
        price=pytest.approx(0.5, 0.05),
        tag=pytest.approx(mock_kite.place_order.call_args[1]["tag"])
    )
    mock_kite.cancel_order.assert_not_called()

def test_gateway_partial_fill_places_sl_for_filled_quantity_and_persists():
    mock_kite = MagicMock()
    mock_emailer = MagicMock()
    
    mock_kite.margins.return_value = {"available": {"live_balance": 50000.0}}
    mock_kite.place_order.side_effect = ["ORD_BUY_PARTIAL", "ORD_SL_PARTIAL"]
    mock_kite.orders.return_value = []
    
    # 200 out of 500 shares filled when timeout occurs
    mock_kite.order_history.return_value = [
        {"status": "CANCELLED", "filled_quantity": 200, "average_price": 24.50}
    ]
    
    gateway = SovereignExecutionGateway(kite_exec=mock_kite, emailer=mock_emailer)
    
    with patch.object(gateway, "verify_order_fill_after_10s") as mock_verify, \
         patch("core.sovereign_execution_gateway.save_active_position") as mock_save:
        
        mock_verify.return_value = {
            "status": "OPEN",
            "average_price": 24.50,
            "filled_quantity": 200,
            "pending_quantity": 300,
            "quantity": 500,
            "status_message": "Partially filled"
        }
        
        result = gateway.execute_option_order(
            symbol="KPITTECH",
            option_symbol="KPITTECH26NOV520CE",
            lot_size=500,
            entry_premium=24.0
        )
        
        assert result["success"] is True
        assert result["status"] == "PARTIAL_FILL"
        assert result["filled_quantity"] == 200
        assert result["fill_price"] == 24.50
        assert result["order_id"] == "ORD_BUY_PARTIAL"
        assert result["sl_order_id"] == "ORD_SL_PARTIAL"
        
        # Verify remainder cancelled
        mock_kite.cancel_order.assert_called_once_with(
            variety=mock_kite.VARIETY_REGULAR,
            order_id="ORD_BUY_PARTIAL"
        )
        
        # Verify protective SL placed for exactly 200 qty (not 500)
        sl_call = mock_kite.place_order.call_args_list[1]
        assert sl_call[1]["quantity"] == 200
        assert sl_call[1]["order_type"] == mock_kite.ORDER_TYPE_SL
        
        # Verify saved to database ledger
        mock_save.assert_called_once()
        saved_pos = mock_save.call_args[0][0]
        assert saved_pos["quantity"] == 200
        assert saved_pos["entry_price"] == 24.50
        
        # Verify partial fill email dispatched
        mock_emailer.send_live_alert.assert_called_once()
        subject = mock_emailer.send_live_alert.call_args[0][0]
        assert "[ORDER PARTIALLY EXECUTED]" in subject
        assert "200/500" in subject

def test_gateway_idempotency_prevents_duplicate_submission():
    mock_kite = MagicMock()
    mock_emailer = MagicMock()
    mock_kite.margins.return_value = {"available": {"live_balance": 50000.0}}
    
    # Existing active buy order already in book
    mock_kite.orders.return_value = [
        {
            "order_id": "ORD_EXISTING_111",
            "tradingsymbol": "JSWSTEEL26NOV1280CE",
            "transaction_type": mock_kite.TRANSACTION_TYPE_BUY,
            "status": "OPEN"
        }
    ]
    
    gateway = SovereignExecutionGateway(kite_exec=mock_kite, emailer=mock_emailer)
    
    result = gateway.execute_option_order(
        symbol="JSWSTEEL",
        option_symbol="JSWSTEEL26NOV1280CE",
        lot_size=350,
        entry_premium=35.0
    )
    
    assert result["order_id"] == "ORD_EXISTING_111"
    assert "Idempotent" in result["message"]
    mock_kite.place_order.assert_not_called()

def test_gateway_rejects_sub_tick_entry_premium():
    gateway = SovereignExecutionGateway()
    result = gateway.execute_option_order(
        symbol="TATACHEM",
        option_symbol="TATACHEM26NOV1000CE",
        lot_size=400,
        entry_premium=0.02
    )
    assert result["success"] is False
    assert result["error"] == "INVALID_PREMIUM"


