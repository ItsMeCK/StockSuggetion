import os
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

# Set environment variables for testing
os.environ["LIVE_BUY"] = "True"
os.environ["EXEC_KITE_API_KEY"] = "dummy_key"
os.environ["EXEC_KITE_ACCESS_TOKEN"] = "dummy_token"
os.environ["EXEC_KITE_API_SECRET"] = "dummy_secret"

# Mock DB Storage
mock_active_positions = {}
mock_closed_positions = {}

def mock_save_active_position(pos):
    mock_active_positions[pos['order_id']] = pos

def mock_get_all_active_positions():
    return list(mock_active_positions.values())

def mock_update_position_sl(order_id, highest_high, current_sl, trailing_active):
    if order_id in mock_active_positions:
        mock_active_positions[order_id]['highest_high'] = highest_high
        mock_active_positions[order_id]['current_sl'] = current_sl
        mock_active_positions[order_id]['trailing_active'] = trailing_active

def mock_close_position(pos, exit_price, pnl_pct, exit_reason):
    if pos['order_id'] in mock_active_positions:
        mock_closed_positions[pos['order_id']] = pos
        del mock_active_positions[pos['order_id']]

class TestLiveEngine(unittest.TestCase):
    def setUp(self):
        mock_active_positions.clear()
        mock_closed_positions.clear()
        
    @patch('core.live_trading.get_kite_client')
    @patch('core.db_manager.save_active_position', side_effect=mock_save_active_position)
    def test_execute_trade_places_slm_order(self, mock_save, mock_get_kite):
        from core.live_trading import execute_trade
        
        # Setup Kite Mock
        kite_mock = MagicMock()
        mock_get_kite.return_value = kite_mock
        
        # Mock quotes
        def quote_mock(instruments):
            if "NSE:TCS" in instruments:
                return {"NSE:TCS": {"last_price": 4000}}
            if "NFO:TCS24AUG4040CE" in instruments:
                return {"NFO:TCS24AUG4040CE": {"last_price": 100.0}}
            return {}
        kite_mock.quote.side_effect = quote_mock
        
        # Mock instruments
        kite_mock.instruments.return_value = [
            {'name': 'TCS', 'instrument_type': 'CE', 'expiry': '2024-08-29', 'strike': 4040, 'instrument_token': 1234, 'tradingsymbol': 'TCS24AUG4040CE', 'lot_size': 175}
        ]
        
        # Mock place_order
        kite_mock.place_order.side_effect = ["BUY_ORDER_123", "SLM_ORDER_456"]
        
        # Execute Trade
        execute_trade("TCS", score=90, catalyst="Math Breakout", entry_time=datetime.now())
        
        # Verify Kite API calls
        self.assertEqual(kite_mock.place_order.call_count, 2)
        
        # Verify BUY Order
        buy_call = kite_mock.place_order.call_args_list[0][1]
        self.assertEqual(buy_call['transaction_type'], kite_mock.TRANSACTION_TYPE_BUY)
        self.assertEqual(buy_call['order_type'], kite_mock.ORDER_TYPE_MARKET)
        
        # Verify SL-M Order
        sl_call = kite_mock.place_order.call_args_list[1][1]
        self.assertEqual(sl_call['transaction_type'], kite_mock.TRANSACTION_TYPE_SELL)
        self.assertEqual(sl_call['order_type'], kite_mock.ORDER_TYPE_SLM)
        self.assertEqual(sl_call['trigger_price'], 50.0) # 50% of 100.0 entry premium
        
        # Verify DB Insert
        self.assertTrue("BUY_ORDER_123" in mock_active_positions)
        pos = mock_active_positions["BUY_ORDER_123"]
        self.assertEqual(pos['sl_order_id'], "SLM_ORDER_456")
        self.assertEqual(pos['current_sl'], 50.0)
        
    @patch('scripts.monitor_positions.get_kite_client')
    @patch('core.db_manager.get_all_active_positions', side_effect=mock_get_all_active_positions)
    @patch('core.db_manager.update_position_sl', side_effect=mock_update_position_sl)
    @patch('core.db_manager.close_position', side_effect=mock_close_position)
    def test_monitor_trailing_sl_logic(self, mock_close, mock_update, mock_get_all, mock_get_kite):
        from scripts.monitor_positions import monitor_positions
        
        # Inject Fake Position
        pos = {
            "order_id": "BUY_1",
            "sl_order_id": "SL_1",
            "option_symbol": "TCS24AUG4040CE",
            "entry_premium": 100.0,
            "current_sl": 50.0,
            "highest_high": 100.0,
            "trailing_active": False,
            "qty": 175
        }
        mock_active_positions["BUY_1"] = pos
        
        # Setup Kite Mock
        kite_mock = MagicMock()
        mock_get_kite.return_value = kite_mock
        
        # 1. Test No Action (Price slightly up)
        kite_mock.orders.return_value = [{"order_id": "SL_1", "status": "OPEN"}]
        kite_mock.quote.return_value = {"NFO:TCS24AUG4040CE": {"last_price": 120.0}}
        monitor_positions()
        
        # Verify no modification and highest_high updated
        self.assertEqual(kite_mock.modify_order.call_count, 0)
        self.assertEqual(mock_active_positions["BUY_1"]["highest_high"], 120.0)
        self.assertFalse(mock_active_positions["BUY_1"]["trailing_active"])
        
        # 2. Test Trailing Activation (Price spikes to 160.0, which is > 1.5x entry)
        kite_mock.quote.return_value = {"NFO:TCS24AUG4040CE": {"last_price": 160.0}}
        monitor_positions()
        
        # Verify modification called with new SL
        self.assertEqual(kite_mock.modify_order.call_count, 1)
        expected_new_sl = round(160.0 - (160.0 * 0.10), 1) # 144.0
        
        mod_call = kite_mock.modify_order.call_args[1]
        self.assertEqual(mod_call['order_id'], 'SL_1')
        self.assertEqual(mod_call['trigger_price'], expected_new_sl)
        
        # Verify DB updated
        self.assertTrue(mock_active_positions["BUY_1"]["trailing_active"])
        self.assertEqual(mock_active_positions["BUY_1"]["current_sl"], expected_new_sl)
        
        # 3. Test SL Hit (Exchange marks SL order as COMPLETE)
        kite_mock.orders.return_value = [{"order_id": "SL_1", "status": "COMPLETE", "average_price": 140.0}]
        monitor_positions()
        
        # Verify trade closed via exchange
        self.assertTrue("BUY_1" not in mock_active_positions)
        self.assertTrue("BUY_1" in mock_closed_positions)

if __name__ == '__main__':
    unittest.main()
