import pytest
import os
import json
from unittest.mock import MagicMock, patch
from core.token_manager import (
    sync_tokens_from_server,
    fetch_data_token_from_server,
    get_data_token,
    get_exec_token,
    get_kite_data_client,
    get_kite_exec_client,
    update_env_variable
)

def test_update_env_variable_modifies_file_and_environ(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("KITE_API_KEY='test_key'\nKITE_ACCESS_TOKEN='old_token'\n")
    
    update_env_variable("KITE_ACCESS_TOKEN", "new_token_123", env_path=str(env_file))
    
    content = env_file.read_text()
    assert "KITE_ACCESS_TOKEN='new_token_123'" in content
    assert os.environ.get("KITE_ACCESS_TOKEN") == "new_token_123"

def test_sync_tokens_from_server_updates_environment(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("KITE_API_KEY='key1'\nEXEC_KITE_API_KEY='key2'\n")
    
    with patch("requests.get") as mock_get:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "KITE_ACCESS_TOKEN": "fresh_data_token",
            "EXEC_KITE_ACCESS_TOKEN": "fresh_exec_token",
            "updated_at": "2026-11-02T23:00:00"
        }
        mock_get.return_value = mock_resp
        
        tokens = sync_tokens_from_server(
            server_url="http://mock-server:5000",
            auth_key="test_auth",
            env_path=str(env_file)
        )
        
        assert tokens["KITE_ACCESS_TOKEN"] == "fresh_data_token"
        assert tokens["EXEC_KITE_ACCESS_TOKEN"] == "fresh_exec_token"
        assert os.environ["KITE_ACCESS_TOKEN"] == "fresh_data_token"
        assert os.environ["EXEC_KITE_ACCESS_TOKEN"] == "fresh_exec_token"
        
        content = env_file.read_text()
        assert "KITE_ACCESS_TOKEN='fresh_data_token'" in content
        assert "EXEC_KITE_ACCESS_TOKEN='fresh_exec_token'" in content

def test_fetch_data_token_from_server():
    with patch("requests.get") as mock_get, patch("core.token_manager.update_env_variable") as mock_update:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "account": "YF4472",
            "access_token": "data_only_token_456"
        }
        mock_get.return_value = mock_resp
        
        token = fetch_data_token_from_server(server_url="http://mock-server:5000", auth_key="key")
        assert token == "data_only_token_456"
        mock_update.assert_called_once_with("KITE_ACCESS_TOKEN", "data_only_token_456")

def test_get_kite_data_client_auto_syncs_when_missing():
    with patch.dict(os.environ, {"KITE_API_KEY": "my_api_key", "KITE_ACCESS_TOKEN": ""}), \
         patch("core.token_manager.sync_tokens_from_server") as mock_sync, \
         patch("core.token_manager.KiteConnect") as mock_kite_cls:
        
        def set_token(*args, **kwargs):
            os.environ["KITE_ACCESS_TOKEN"] = "synced_access_token"
            return {"KITE_ACCESS_TOKEN": "synced_access_token"}
            
        mock_sync.side_effect = set_token
        
        client = get_kite_data_client(auto_refresh=True)
        mock_sync.assert_called_once()
        mock_kite_cls.assert_called_once_with(api_key="my_api_key", timeout=25)
        mock_kite_cls.return_value.set_access_token.assert_called_once_with("synced_access_token")
