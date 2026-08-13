import os
import json
import pytest
import unittest
from unittest.mock import patch, MagicMock

@pytest.fixture
def mock_env():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "test_key", "KITE_API_KEY": "test", "KITE_ACCESS_TOKEN": "test"}):
        yield

def test_run_daily_debate_missing_file(mock_env, capsys):
    from agents.debate_engine.debate_orchestrator import run_daily_debate
    
    with patch('os.path.exists', return_value=False):
        with patch('agents.debate_engine.debate_orchestrator.init_db'):
            run_daily_debate()
            captured = capsys.readouterr()
            assert "No candidate file found at" in captured.out

def test_run_daily_debate_success(mock_env):
    from agents.debate_engine.debate_orchestrator import run_daily_debate
    
    mock_symbols = ["RELIANCE"]
    mock_file_data = json.dumps(mock_symbols)
    
    with patch('os.path.exists', return_value=True):
        with patch('builtins.open', unittest.mock.mock_open(read_data=mock_file_data)):
            with patch('agents.debate_engine.debate_orchestrator.init_db'):
                with patch('agents.debate_engine.debate_orchestrator.DebateOrchestrator') as MockOrchestrator:
                    mock_orch_instance = MockOrchestrator.return_value
                    mock_orch_instance.run_debate.return_value = {
                        "score": 95,
                        "bull_thesis": "Good",
                        "bear_thesis": "Bad"
                    }
                    with patch('agents.debate_engine.debate_orchestrator.add_anticipatory_stock') as mock_add:
                        with patch('time.sleep'): # skip rate limit sleep
                            run_daily_debate()
                            mock_add.assert_called_once_with("RELIANCE", "Good", "Bad", 95)
