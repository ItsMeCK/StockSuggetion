import os
import json
import pytest
from unittest.mock import patch, MagicMock

@pytest.fixture
def mock_env():
    with patch.dict(os.environ, {"GEMINI_API_KEY": "test_key"}):
        yield

def test_scout_market_catalysts_success(mock_env):
    from agents.debate_engine.intelligence_scout import IntelligenceScout
    
    with patch('agents.debate_engine.intelligence_scout.genai.Client') as MockClient:
        mock_client_instance = MockClient.return_value
        mock_response = MagicMock()
        # The code looks for JSON between [ and ]
        mock_response.text = 'Here is the data: ["RELIANCE", "TCS", "INFY"]'
        mock_client_instance.models.generate_content.return_value = mock_response
        
        scout = IntelligenceScout()
        symbols = scout.scout_market_catalysts()
        
        assert symbols == ["RELIANCE", "TCS", "INFY"]
        mock_client_instance.models.generate_content.assert_called_once()

def test_scout_market_catalysts_failure(mock_env):
    from agents.debate_engine.intelligence_scout import IntelligenceScout
    
    with patch('agents.debate_engine.intelligence_scout.genai.Client') as MockClient:
        mock_client_instance = MockClient.return_value
        # No brackets in response
        mock_response = MagicMock()
        mock_response.text = 'I am sorry, I cannot fulfill this request.'
        mock_client_instance.models.generate_content.return_value = mock_response
        
        scout = IntelligenceScout()
        symbols = scout.scout_market_catalysts()
        
        assert symbols == []
