import pytest
from unittest.mock import MagicMock, patch
from core.smart_volume_reversal_engine import SmartVolumeReversalEngine

def test_smart_volume_reversal_skips_when_zero_positions():
    mock_kite = MagicMock()
    mock_kite.positions.return_value = {"net": []}
    
    with patch("core.smart_volume_reversal_engine.get_all_active_positions", return_value=[]):
        engine = SmartVolumeReversalEngine(kite_exec=mock_kite)
        results = engine.run_20m_cycle()
        assert results == []

def test_smart_volume_reversal_microstructure_diagnostics():
    engine = SmartVolumeReversalEngine()
    
    # Construct 25 5m candles: last candle is high-volume red candle with distribution wick
    candles = []
    base_price = 1000.0
    for i in range(25):
        candles.append({
            "open": base_price + i,
            "high": base_price + i + 2,
            "low": base_price + i - 1,
            "close": base_price + i + 1,
            "volume": 10000
        })
    
    # 25th candle: upper wick rejection and massive volume (distribution)
    candles[-1] = {
        "open": 1025.0,
        "high": 1035.0,
        "low": 1018.0,
        "close": 1020.0,  # Red candle, upper wick = 1035 - 1025 = 10 (range 17, ratio 58.8%)
        "volume": 35000   # 3.5x average
    }
    
    metrics = engine.calculate_microstructure_metrics(candles)
    assert metrics["distribution_candle"] is True
    assert metrics["selling_volume_spike"] is True
    assert metrics["volume_ratio"] >= 2.5
    assert metrics["upper_wick_ratio"] >= 0.50

def test_smart_volume_reversal_executes_exit_and_emails():
    mock_kite_data = MagicMock()
    mock_kite_exec = MagicMock()
    mock_emailer = MagicMock()
    
    # Active open position in Bharat Forge CE
    mock_kite_exec.positions.return_value = {
        "net": [{
            "tradingsymbol": "BHARATFORG26NOV1720CE",
            "quantity": 500,
            "average_price": 84.15,
            "last_price": 75.0,
            "instrument_token": 12345
        }]
    }
    mock_kite_exec.orders.return_value = [
        {"tradingsymbol": "BHARATFORG26NOV1720CE", "order_id": "RESTING_SL_999", "status": "TRIGGER PENDING"}
    ]
    
    # Historical data showing breakdown
    mock_kite_data.quote.return_value = {
        "NSE:BHARATFORG": {"instrument_token": 9999},
        "NFO:BHARATFORG26NOV1720CE": {"last_price": 75.0}
    }
    
    breakdown_candles = []
    for i in range(25):
        breakdown_candles.append({
            "open": 1750.0 - i,
            "high": 1752.0 - i,
            "low": 1740.0 - i,
            "close": 1742.0 - i,
            "volume": 20000
        })
    mock_kite_data.historical_data.return_value = breakdown_candles
    
    mock_llm = MagicMock()
    # Mock LLM decision: EXIT with 90% confidence
    mock_response = MagicMock()
    mock_response.text = '{"decision": "EXIT", "confidence": 90, "volume_reversal_confirmed": true, "reversal_mechanism": "VWAP_BREAKDOWN", "concise_rationale": "Massive institutional selling broke VWAP."}'
    mock_llm.models.generate_content.return_value = mock_response
    
    engine = SmartVolumeReversalEngine(
        kite_data=mock_kite_data,
        kite_exec=mock_kite_exec,
        emailer=mock_emailer,
        gemini_client=mock_llm
    )
    
    with patch("core.smart_volume_reversal_engine.close_position") as mock_db_exit:
        results = engine.run_20m_cycle()
        
        assert len(results) == 1
        res = results[0]
        assert res["decision"] == "EXIT"
        assert res["action_taken"] == "AUTO_EXITED"
        
        # Invariant: Resting SL cancelled before liquidation
        mock_kite_exec.cancel_order.assert_called_once_with(
            variety=mock_kite_exec.VARIETY_REGULAR,
            order_id="RESTING_SL_999"
        )
        
        # Invariant: Market exit placed
        mock_kite_exec.place_order.assert_called_once()
        call_kwargs = mock_kite_exec.place_order.call_args[1]
        assert call_kwargs["tradingsymbol"] == "BHARATFORG26NOV1720CE"
        assert call_kwargs["quantity"] == 500
        assert call_kwargs["transaction_type"] == mock_kite_exec.TRANSACTION_TYPE_SELL
        
        # Invariant: Email alert dispatched with [ORDER EXECUTED] [AUTO-EXIT]
        mock_emailer.send_live_alert.assert_called_once()
        subject = mock_emailer.send_live_alert.call_args[0][0]
        assert "[ORDER EXECUTED] [AUTO-EXIT]" in subject
        assert "BHARATFORG26NOV1720CE" in subject

def test_smart_volume_reversal_respects_15m_gestation_window():
    from datetime import datetime, timedelta
    mock_kite_data = MagicMock()
    mock_kite_exec = MagicMock()
    now_dt = datetime(2026, 11, 9, 11, 25, 0)
    
    # Position entered only 4 minutes ago
    entry_dt = now_dt - timedelta(minutes=4)
    mock_kite_exec.positions.return_value = {
        "net": [{
            "tradingsymbol": "AUROPHARMA26NOV1820CE",
            "quantity": 550,
            "average_price": 50.7,
            "last_price": 51.5,
            "instrument_token": 9999
        }]
    }
    
    mock_db_pos = [{
        "option_symbol": "AUROPHARMA26NOV1820CE",
        "entry_time": entry_dt,
        "order_id": "ORD_123"
    }]
    
    with patch("core.smart_volume_reversal_engine.get_all_active_positions", return_value=mock_db_pos):
        engine = SmartVolumeReversalEngine(
            kite_data=mock_kite_data,
            kite_exec=mock_kite_exec
        )
        results = engine.run_20m_cycle(now=now_dt)
        
        assert len(results) == 1
        assert results[0]["mechanism"] == "GESTATION_HOLD"
        assert results[0]["action_taken"] == "HOLD"
        # Must NOT attempt to place order
        mock_kite_exec.place_order.assert_not_called()

