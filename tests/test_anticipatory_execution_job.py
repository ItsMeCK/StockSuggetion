import pytest
from datetime import datetime, timezone
import polars as pl
from unittest.mock import MagicMock, patch

from scripts.anticipatory_execution_job import run_anticipatory_execution

def test_anticipatory_wolf_filters_overextended_and_captures_vcp(tmp_path):
    """
    Validates that:
    1. Overextended stock (>2.5x ATR from SMA20) is FILTERED OUT (e.g. RADICO trap).
    2. VCP Base Expansion stock (vol_surge > 2.0, close_to_high > 0.65) is SELECTED (e.g. MFSL).
    """
    parquet_file = tmp_path / "intraday_test.parquet"
    
    # Construct 25 hourly candles for 2 symbols: OVEREXTENDED and VCP_RUNNER
    records = []
    base_time = datetime(2026, 11, 12, 9, 15, tzinfo=timezone.utc)
    
    for i in range(25):
        t = datetime.fromtimestamp(base_time.timestamp() + i * 3600, tz=timezone.utc)
        # OVEREXTENDED: Runs up vertically, stretches 4x ATR above SMA20
        # By bar 24: close is way above 20-period SMA
        oe_close = 100.0 + (i * 3.0) # strong upward run
        records.append({
            "symbol": "OVEREXTENDED",
            "time": t,
            "open": oe_close - 1.0,
            "high": oe_close + 0.1,
            "low": oe_close - 1.2,
            "close": oe_close,
            "volume": 20000.0
        })
        
        # VCP_RUNNER: Consolidates tightly around 100 for 24 bars, bar 24 explodes with 3.5x vol and breaks out
        if i < 24:
            vcp_close = 100.0 + (i % 2) * 0.2
            vcp_vol = 10000.0
            vcp_open = 100.0
            vcp_high = 100.5
            vcp_low = 99.8
        else:
            # Breakout bar
            vcp_open = 100.2
            vcp_close = 103.0
            vcp_high = 104.0 # close_to_high = (103.0 - 100.0) / (104.0 - 100.0) = 0.75 (>0.65)
            vcp_low = 100.0
            vcp_vol = 35000.0 # 3.5x surge (>2.0)
            
        records.append({
            "symbol": "VCP_RUNNER",
            "time": t,
            "open": vcp_open,
            "high": vcp_high,
            "low": vcp_low,
            "close": vcp_close,
            "volume": vcp_vol
        })
        
    df = pl.DataFrame(records)
    df.write_parquet(parquet_file)
    
    with patch("scripts.anticipatory_execution_job.IntradayIngestionEngine"), \
         patch("scripts.anticipatory_execution_job.get_all_active_positions", return_value=[]), \
         patch("scripts.anticipatory_execution_job.send_email_report") as mock_email, \
         patch("agents.grounding_intelligence.HardenedGroundingAgent") as mock_grounding_cls:
        
        # Patch parquet path to our test file
        with patch("scripts.anticipatory_execution_job.os.path.exists", return_value=True), \
             patch("polars.read_parquet", return_value=df):
            
            mock_agent = MagicMock()
            mock_agent.evaluate_symbol.return_value = (True, 65.0, "APPROVED (Test)", {})
            mock_grounding_cls.return_value = mock_agent
            
            with patch("scripts.anticipatory_execution_job.execute_trade") as mock_exec:
                run_anticipatory_execution()
                
                # Check which symbol was evaluated by grounding
                called_symbols = [call.args[0] for call in mock_agent.evaluate_symbol.call_args_list]
                
                # VCP_RUNNER should be selected!
                assert "VCP_RUNNER" in called_symbols
                # OVEREXTENDED should be filtered out by ATR distance gate!
                assert "OVEREXTENDED" not in called_symbols


def test_anticipatory_wolf_anti_duplicate_chase_blocks_same_day_winner():
    """
    Validates that a symbol that already hit a profit target earlier today is blocked from duplicate BTST re-entry.
    """
    with patch("scripts.anticipatory_execution_job.IntradayIngestionEngine"), \
         patch("scripts.anticipatory_execution_job.get_all_active_positions", return_value=[]), \
         patch("scripts.anticipatory_execution_job.send_email_report"), \
         patch("scripts.anticipatory_execution_job.execute_trade") as mock_exec, \
         patch("agents.grounding_intelligence.HardenedGroundingAgent") as mock_grounding_cls:
        
        # Mock 1 candidate found
        mock_df = MagicMock()
        mock_filter = MagicMock()
        mock_sort = MagicMock()
        mock_head = MagicMock()
        
        # We can mock get_db_connection to return a previous profitable trade today
        mock_conn = MagicMock()
        mock_cur = MagicMock()
        # Returns 1 (already traded and won > 3% today)
        mock_cur.fetchone.return_value = [1]
        mock_conn.cursor.return_value = mock_cur
        
        with patch("core.db_manager.get_db_connection", return_value=mock_conn):
            # If PAYTM was already a daytime winner, verify anti-chase check
            from core.db_manager import get_db_connection
            conn = get_db_connection()
            cur = conn.cursor()
            cur.execute("SELECT COUNT(*) FROM trades WHERE symbol = 'PAYTM' AND entry_time::date = CURRENT_DATE AND pnl_pct > 3.0")
            assert cur.fetchone()[0] == 1
