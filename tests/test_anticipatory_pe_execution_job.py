import pytest
from unittest.mock import MagicMock, patch
import polars as pl
from datetime import datetime, timezone
import pytz

from scripts.anticipatory_pe_execution_job import (
    evaluate_pe_macro_regime,
    run_anticipatory_pe_execution
)

def test_evaluate_pe_macro_regime_bull_expansion_blocks_pe():
    mock_kite = MagicMock()
    mock_kite.historical_data.return_value = [
        {"open": 24000.0, "close": 24000.0},
        {"open": 24000.0, "close": 24150.0}  # +0.625% strong bull day
    ]
    dummy_df = pl.DataFrame({"time": [datetime.now(timezone.utc)]})
    allowed, reason = evaluate_pe_macro_regime(dummy_df, mock_kite)
    assert not allowed
    assert "strong Bull Expansion" in reason

def test_evaluate_pe_macro_regime_neutral_allows_pe():
    mock_kite = MagicMock()
    mock_kite.historical_data.return_value = [
        {"open": 24000.0, "close": 24000.0},
        {"open": 24000.0, "close": 23980.0}  # -0.08% chop/red day
    ]
    dummy_df = pl.DataFrame({"time": [datetime.now(timezone.utc)]})
    allowed, reason = evaluate_pe_macro_regime(dummy_df, mock_kite)
    assert allowed
    assert "allows PE" in reason

def test_anticipatory_pe_filters_downside_overextension_and_selects_breakdown():
    # Build synthetic 30-candle dataset for 2 symbols:
    # 1. OVEREXTENDED_CRASH: Down 5.0x ATR below 20 SMA -> must be filtered
    # 2. CLEAN_BREAKDOWN: Closing at LOD, vol surge 2.5x, 2.0x ATR below SMA -> must qualify
    now_utc = datetime(2026, 11, 13, 8, 45, tzinfo=timezone.utc)
    times = [datetime(2026, 11, 10, 3, 45, tzinfo=timezone.utc) for _ in range(29)] + [now_utc]

    data = []
    # Symbol 1: OVEREXTENDED_CRASH (downside_atr_dist > 4.0)
    for i in range(29):
        data.append({
            "symbol": "OVEREXTENDED_CRASH",
            "time": times[i],
            "open": 1000.0,
            "high": 1010.0,
            "low": 990.0,
            "close": 1000.0,
            "volume": 10000
        })
    # Bar 30: massive plunge, ATR distance ~ 5.0x
    data.append({
        "symbol": "OVEREXTENDED_CRASH",
        "time": now_utc,
        "open": 980.0,
        "high": 980.0,
        "low": 850.0,
        "close": 855.0, # at low of day, but down 145 pts on 20 pt ATR -> >5.0x ATR
        "volume": 50000
    })

    # Symbol 2: CLEAN_BREAKDOWN (downside_atr_dist <= 4.0, close_to_low > 0.85, red candle)
    for i in range(29):
        data.append({
            "symbol": "CLEAN_BREAKDOWN",
            "time": times[i],
            "open": 500.0,
            "high": 505.0,
            "low": 495.0,
            "close": 500.0,
            "volume": 10000
        })
    # Bar 30: fresh breakdown below SMA 20 (SMA=500, close=475, ATR=10 -> dist=2.5x)
    data.append({
        "symbol": "CLEAN_BREAKDOWN",
        "time": now_utc,
        "open": 495.0,
        "high": 495.0,
        "low": 473.0,
        "close": 475.0, # near low (2/22 from low -> close_to_low=0.909 > 0.85)
        "volume": 30000 # vol_surge = 3.0x
    })

    test_df = pl.DataFrame(data)

    with patch("scripts.anticipatory_pe_execution_job.pl.read_parquet", return_value=test_df), \
         patch("scripts.anticipatory_pe_execution_job.get_all_active_positions", return_value=[]), \
         patch("scripts.anticipatory_pe_execution_job.evaluate_pe_macro_regime", return_value=(True, "Neutral regime")), \
         patch("scripts.anticipatory_pe_execution_job.IntradayIngestionEngine"), \
         patch("scripts.anticipatory_pe_execution_job.get_db_connection") as mock_conn, \
         patch("scripts.anticipatory_pe_execution_job.HardenedGroundingAgent") as mock_grounding_cls, \
         patch("scripts.anticipatory_pe_execution_job.execute_trade") as mock_exec, \
         patch("scripts.anticipatory_pe_execution_job.send_email_report"):

        # Mock DB
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_conn.return_value.cursor.return_value = mock_cursor

        # Mock Grounding
        mock_agent = MagicMock()
        mock_agent.evaluate_symbol.return_value = (True, 72.0, "Institutional selling footprint", {})
        mock_grounding_cls.return_value = mock_agent

        run_anticipatory_pe_execution()

        # OVEREXTENDED_CRASH must be filtered out by downside_atr_dist <= 4.0
        # CLEAN_BREAKDOWN must be executed as SHORT PE
        assert mock_exec.call_count == 1
        call_kwargs = mock_exec.call_args[1]
        assert call_kwargs["symbol"] == "CLEAN_BREAKDOWN"
        assert call_kwargs["direction"] == "SHORT"
        assert call_kwargs["instrument_type"] == "PE"
        assert call_kwargs["holding_type"] == "TYPE_C_BTST"

def test_anticipatory_pe_toxic_catalyst_enhances_short_score():
    now_utc = datetime(2026, 11, 13, 8, 45, tzinfo=timezone.utc)
    times = [datetime(2026, 11, 10, 3, 45, tzinfo=timezone.utc) for _ in range(29)] + [now_utc]

    data = []
    for i in range(29):
        data.append({
            "symbol": "TOXIC_DOWNGRADE_CORP",
            "time": times[i],
            "open": 500.0,
            "high": 505.0,
            "low": 495.0,
            "close": 500.0,
            "volume": 10000
        })
    data.append({
        "symbol": "TOXIC_DOWNGRADE_CORP",
        "time": now_utc,
        "open": 495.0,
        "high": 495.0,
        "low": 473.0,
        "close": 475.0,
        "volume": 25000
    })
    test_df = pl.DataFrame(data)

    with patch("scripts.anticipatory_pe_execution_job.pl.read_parquet", return_value=test_df), \
         patch("scripts.anticipatory_pe_execution_job.get_all_active_positions", return_value=[]), \
         patch("scripts.anticipatory_pe_execution_job.evaluate_pe_macro_regime", return_value=(True, "Neutral regime")), \
         patch("scripts.anticipatory_pe_execution_job.IntradayIngestionEngine"), \
         patch("scripts.anticipatory_pe_execution_job.get_db_connection") as mock_conn, \
         patch("scripts.anticipatory_pe_execution_job.HardenedGroundingAgent") as mock_grounding_cls, \
         patch("scripts.anticipatory_pe_execution_job.execute_trade") as mock_exec, \
         patch("scripts.anticipatory_pe_execution_job.send_email_report"):

        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (0,)
        mock_conn.return_value.cursor.return_value = mock_cursor

        # Mock Grounding returning toxic catalyst detected (which in CE would be rejected)
        mock_agent = MagicMock()
        mock_agent.evaluate_symbol.return_value = (
            False, 
            0.0, 
            "Toxic debt rating downgrade detected", 
            {"parsed_dimensions": {"toxic_catalyst": {"detected": True, "summary": "CRISIL downgrade"}}}
        )
        mock_grounding_cls.return_value = mock_agent

        run_anticipatory_pe_execution()

        # For PE, toxic catalyst is a bearish accelerator -> score boosted to >= 75.0 and executed!
        assert mock_exec.call_count == 1
        call_kwargs = mock_exec.call_args[1]
        assert call_kwargs["symbol"] == "TOXIC_DOWNGRADE_CORP"
        assert call_kwargs["direction"] == "SHORT"
        assert call_kwargs["instrument_type"] == "PE"
        assert call_kwargs["score"] >= 75.0
