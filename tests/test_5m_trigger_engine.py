import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from core.trigger_engine_5m import (
    FiveMinuteBar,
    evaluate_5m_trigger,
    FiveMinuteTriggerEngine
)

def test_evaluate_5m_trigger_bullish_ignition():
    """Tests that a 5m bar with volume surge >= 1.8x, close near high, and non-extended price triggers LONG."""
    bar = FiveMinuteBar(
        symbol="RELIANCE",
        timestamp=datetime(2026, 10, 25, 10, 20),
        open=2800.0,
        high=2810.0,
        low=2798.0,
        close=2809.0, # Close near high (2809 - 2798) / (2810 - 2798) = 11 / 12 = 0.916 > 0.85
        volume=50000,
        vol_sma_20=25000, # 2.0x volume surge >= 1.8x
        sma_20=2802.0 # Extension = (2809 - 2802) / 2802 = +0.25% <= 2.5%
    )
    signal = evaluate_5m_trigger(bar)
    assert signal.triggered is True
    assert signal.direction == "LONG"
    assert signal.instrument_type == "CE"
    assert signal.symbol == "RELIANCE"

def test_evaluate_5m_trigger_bearish_breakdown():
    """Tests that a 5m bar with volume surge >= 1.8x and close near low triggers SHORT."""
    bar = FiveMinuteBar(
        symbol="INFY",
        timestamp=datetime(2026, 10, 25, 11, 35),
        open=1820.0,
        high=1822.0,
        low=1808.0,
        close=1809.0, # Close near low (1822 - 1809) / (1822 - 1808) = 13 / 14 = 0.928 > 0.85
        volume=60000,
        vol_sma_20=30000, # 2.0x volume surge
        sma_20=1815.0 # Extension = -0.33% >= -2.5%
    )
    signal = evaluate_5m_trigger(bar)
    assert signal.triggered is True
    assert signal.direction == "SHORT"
    assert signal.instrument_type == "PE"
    assert signal.symbol == "INFY"

def test_evaluate_5m_trigger_rejects_overextended_candle():
    """Anti-FOMO: Rejects candle if extension from 20-SMA exceeds 2.5%."""
    bar = FiveMinuteBar(
        symbol="TCS",
        timestamp=datetime(2026, 10, 25, 13, 10),
        open=3500.0,
        high=3600.0,
        low=3495.0,
        close=3595.0,
        volume=100000,
        vol_sma_20=30000,
        sma_20=3480.0 # Extension = (3595 - 3480) / 3480 = +3.30% > 2.5%
    )
    signal = evaluate_5m_trigger(bar)
    assert signal.triggered is False
    assert "extension" in signal.reason.lower()

def test_5m_trigger_engine_batches_watchlist():
    """FiveMinuteTriggerEngine queries only active watchlist symbols in a single batch."""
    engine = FiveMinuteTriggerEngine()
    mock_kite = MagicMock()
    # Mock quote response for 2 symbols in single call
    mock_kite.quote.return_value = {
        "NSE:RELIANCE": {"last_price": 2810.0, "volume": 50000, "ohlc": {"open": 2800.0, "high": 2812.0, "low": 2798.0, "close": 2809.0}},
        "NSE:INFY": {"last_price": 1810.0, "volume": 10000, "ohlc": {"open": 1810.0, "high": 1812.0, "low": 1809.0, "close": 1810.0}}
    }
    
    active_watchlist = ["RELIANCE", "INFY"]
    signals = engine.scan_active_watchlist(mock_kite, active_watchlist)
    
    # Invariant: Single batch quote call
    mock_kite.quote.assert_called_once_with(["NSE:RELIANCE", "NSE:INFY"])
    assert isinstance(signals, list)
