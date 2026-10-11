import pytest
import os
from unittest.mock import MagicMock, patch
from datetime import datetime

from core.live_trading import execute_trade

@patch.dict(os.environ, {"LIVE_BUY": "true"})
@patch("core.risk_gatekeeper.get_all_active_positions", return_value=[])
@patch("core.live_trading.save_active_position")
@patch("core.live_trading.get_kite_data_client")
@patch("core.live_trading.get_kite_exec_client")
def test_engine_1_never_places_live_order_even_if_live_buy_true(mock_exec, mock_data, mock_save_pos, mock_get_pos):
    """
    Critical Invariant:
    Even when LIVE_BUY=true in production environment, Engine 1 Hourly
    MUST NEVER place real orders on Zerodha Exec account. It must be forced to PAPER_TRADED.
    """
    mock_data_client = MagicMock()
    mock_exec_client = MagicMock()
    mock_data.return_value = mock_data_client
    mock_exec.return_value = mock_exec_client

    # Mock Data Quotes & Instruments
    mock_data_client.quote.side_effect = [
        {"NSE:INFY": {"last_price": 1800.0}},
        {"NFO:INFY26OCT1820CE": {"last_price": 25.0, "oi": 50000, "volume": 12000}}
    ]
    mock_data_client.instruments.return_value = [
        {"name": "INFY", "instrument_type": "CE", "expiry": datetime(2026, 10, 29).date(),
         "strike": 1820.0, "lot_size": 400, "tradingsymbol": "INFY26OCT1820CE", "instrument_token": 12345}
    ]

    # Mock Exec margins with ample cash
    mock_exec_client.margins.return_value = {
        "available": {"cash": 50000.0},
        "net": 50000.0
    }

    # Execute trade for Engine 1
    execute_trade(
        symbol="INFY",
        score=85,
        catalyst="Test Catalyst",
        entry_time=datetime.now(),
        holding_type="TYPE_A_SCALP",
        engine_type="ENGINE_1_HOURLY"
    )

    # Invariant: place_order MUST NOT be called on Exec account!
    mock_exec_client.place_order.assert_not_called()

    # Invariant: Must be saved as PAPER_TRADED
    mock_save_pos.assert_called_once()
    saved_position = mock_save_pos.call_args[0][0]
    assert saved_position["status"] == "PAPER_TRADED"
    assert "PAPER_" in saved_position["order_id"]
    assert saved_position["holding_type"] == "TYPE_A_SCALP"

@patch.dict(os.environ, {"LIVE_BUY": "true"})
@patch("core.risk_gatekeeper.get_all_active_positions", return_value=[])
@patch("core.live_trading.save_active_position")
@patch("core.live_trading.get_kite_data_client")
@patch("core.live_trading.get_kite_exec_client")
def test_unknown_or_typo_engine_never_places_live_order(mock_exec, mock_data, mock_save_pos, mock_get_pos):
    """
    Critical Invariant:
    Fail-closed behavior: Any typo or unrecognized engine name
    must fail-closed to PAPER_TRADED without placing live orders.
    """
    mock_data_client = MagicMock()
    mock_exec_client = MagicMock()
    mock_data.return_value = mock_data_client
    mock_exec.return_value = mock_exec_client

    mock_data_client.quote.side_effect = [
        {"NSE:TCS": {"last_price": 3500.0}},
        {"NFO:TCS26OCT3550CE": {"last_price": 30.0, "oi": 40000, "volume": 8000}}
    ]
    mock_data_client.instruments.return_value = [
        {"name": "TCS", "instrument_type": "CE", "expiry": datetime(2026, 10, 29).date(),
         "strike": 3550.0, "lot_size": 175, "tradingsymbol": "TCS26OCT3550CE", "instrument_token": 67890}
    ]

    mock_exec_client.margins.return_value = {
        "available": {"cash": 50000.0},
        "net": 50000.0
    }

    # Execute trade with unknown engine typo
    execute_trade(
        symbol="TCS",
        score=80,
        catalyst="Test Catalyst",
        entry_time=datetime.now(),
        holding_type="TYPE_A_SCALP",
        engine_type="UNKNOWN_OR_TYPO_ENGINE"
    )

    # Invariant: place_order MUST NOT be called!
    mock_exec_client.place_order.assert_not_called()
    mock_save_pos.assert_called_once()
    saved_position = mock_save_pos.call_args[0][0]
    assert saved_position["status"] == "PAPER_TRADED"

@patch.dict(os.environ, {"LIVE_BUY": "true"})
@patch("core.risk_gatekeeper.get_all_active_positions", return_value=[])
@patch("core.live_trading.save_active_position")
@patch("core.live_trading.get_kite_data_client")
@patch("core.live_trading.get_kite_exec_client")
def test_engine_2_zero_cash_hard_veto_blocks_live_order(mock_exec, mock_data, mock_save_pos, mock_get_pos):
    """
    Critical Invariant:
    Account HU0092 currently has ₹0 free cash.
    Engine 2 BTST must NOT attempt order placement if free cash < ₹2,500.
    """
    mock_data_client = MagicMock()
    mock_exec_client = MagicMock()
    mock_data.return_value = mock_data_client
    mock_exec.return_value = mock_exec_client

    mock_data_client.quote.side_effect = [
        {"NSE:RELIANCE": {"last_price": 2800.0}},
        {"NFO:RELIANCE26OCT2820CE": {"last_price": 40.0, "oi": 60000, "volume": 15000}}
    ]
    mock_data_client.instruments.return_value = [
        {"name": "RELIANCE", "instrument_type": "CE", "expiry": datetime(2026, 10, 29).date(),
         "strike": 2820.0, "lot_size": 250, "tradingsymbol": "RELIANCE26OCT2820CE", "instrument_token": 11111}
    ]

    # Account HU0092 reality: ₹0 cash, ₹27,216 equity
    mock_exec_client.margins.return_value = {
        "available": {"cash": 0.0},
        "net": 27216.0
    }

    execute_trade(
        symbol="RELIANCE",
        score=95,
        catalyst="Insider Accumulation",
        entry_time=datetime.now(),
        holding_type="TYPE_C_BTST",
        engine_type="ENGINE_2_BTST"
    )

    # Invariant: place_order MUST NOT be called!
    mock_exec_client.place_order.assert_not_called()
    # Hard veto blocks entry completely to prevent broker margin errors
    mock_save_pos.assert_not_called()

@patch.dict(os.environ, {"LIVE_BUY": "true"})
@patch("core.risk_gatekeeper.get_all_active_positions", return_value=[])
@patch("core.live_trading.save_active_position")
@patch("core.live_trading.get_kite_data_client")
@patch("core.live_trading.get_kite_exec_client")
def test_engine_2_authorized_places_live_order_with_adequate_cash(mock_exec, mock_data, mock_save_pos, mock_get_pos):
    """
    Engine 2 BTST with adequate cash (> ₹2,500) successfully places live BUY and SL orders.
    """
    mock_data_client = MagicMock()
    mock_exec_client = MagicMock()
    mock_data.return_value = mock_data_client
    mock_exec.return_value = mock_exec_client

    mock_data_client.quote.side_effect = [
        {"NSE:HAL": {"last_price": 4500.0}},
        {"NFO:HAL26OCT4550CE": {"last_price": 50.0, "oi": 80000, "volume": 20000}}
    ]
    mock_data_client.instruments.return_value = [
        {"name": "HAL", "instrument_type": "CE", "expiry": datetime(2026, 10, 29).date(),
         "strike": 4550.0, "lot_size": 150, "tradingsymbol": "HAL26OCT4550CE", "instrument_token": 22222}
    ]

    # ₹100,000 cash, ₹100,000 equity (25% allocation = ₹25,000, lot cost = 50 * 150 = ₹7,500)
    mock_exec_client.margins.return_value = {
        "available": {"cash": 100000.0},
        "net": 100000.0
    }
    mock_exec_client.place_order.side_effect = ["LIVE_ORD_123", "LIVE_SL_123"]

    execute_trade(
        symbol="HAL",
        score=92,
        catalyst="Defense Order Win",
        entry_time=datetime.now(),
        holding_type="TYPE_C_BTST",
        engine_type="ENGINE_2_BTST"
    )

    # Invariant: place_order MUST be called twice (BUY and SL)
    assert mock_exec_client.place_order.call_count == 2
    mock_save_pos.assert_called_once()
    saved_position = mock_save_pos.call_args[0][0]
    assert saved_position["status"] == "LIVE_TRADED"
    assert saved_position["order_id"] == "LIVE_ORD_123"
    assert saved_position["holding_type"] == "TYPE_C_BTST"

@patch("core.risk_gatekeeper.get_all_active_positions", return_value=[])
@patch.dict(os.environ, {"LIVE_BUY": "true", "EXEC_KITE_API_KEY": "fake_key", "EXEC_KITE_ACCESS_TOKEN": "fake_token"})
def test_legacy_order_manager_blocked_by_gatekeeper(mock_active_pos):
    """
    Critical Invariant:
    Even if legacy SovereignExecutionEngine.buy_top_candidates is executed with LIVE_BUY=true,
    CapitalRiskGatekeeper must intercept and block all order placement on Zerodha.
    """
    from execution.order_manager import SovereignExecutionEngine
    with patch("execution.order_manager.KiteConnect") as mock_kite_cls:
        mock_kite = MagicMock()
        mock_kite_cls.return_value = mock_kite
        engine = SovereignExecutionEngine()
        engine.kite = mock_kite
        engine.live = True
        engine.live_buy = True

        signals = [{"ticker": "RELIANCE", "score": 90, "price": 2800.0}]
        engine.buy_top_candidates(signals)

        # Invariant: place_order MUST NEVER be called
        mock_kite.place_order.assert_not_called()

@patch("core.risk_gatekeeper.get_all_active_positions", return_value=[])
@patch.dict(os.environ, {"LIVE_BUY": "true", "TRADING_MODE": "LIVE", "EXEC_KITE_API_KEY": "fake_key", "EXEC_KITE_ACCESS_TOKEN": "fake_token"})
def test_legacy_execution_agent_blocked_by_gatekeeper(mock_active_pos):
    """
    Critical Invariant:
    Even if legacy ZerodhaExecutionModule is executed with LIVE_BUY=true,
    CapitalRiskGatekeeper must intercept and block CNC and NFO orders.
    """
    from agents.execution_agent import ZerodhaExecutionModule
    with patch("agents.execution_agent.KiteConnect") as mock_kite_cls:
        mock_kite = MagicMock()
        mock_kite_cls.return_value = mock_kite
        module = ZerodhaExecutionModule(is_historical=False)
        module.kite = mock_kite
        module.live = True
        module.live_buy = True

        # CNC Limit Order
        order_id_cnc = module.place_cnc_limit_order("INFY", 10, 1800.0)
        assert mock_kite.place_order.call_count == 0
        assert "BLOCKED_BY_GATEKEEPER" in order_id_cnc

        # NFO Limit Order
        order_id_nfo = module.place_nfo_limit_order("INFY26OCT1820CE", 400, 25.0)
        assert mock_kite.place_order.call_count == 0
        assert "BLOCKED_BY_GATEKEEPER" in order_id_nfo
