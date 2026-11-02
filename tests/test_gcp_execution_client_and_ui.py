import pytest
from unittest.mock import MagicMock, patch
from ui.token_app import extract_request_token, app
from core.gcp_execution_client import GCPExecutionClient

def test_extract_request_token_variants():
    # 1. Raw token string
    assert extract_request_token("fNKIEHASu6Bc6BM2oXRM6llYimmHiCVo") == "fNKIEHASu6Bc6BM2oXRM6llYimmHiCVo"
    
    # 2. Complete redirect URL
    url = "https://www.google.com/?action=login&type=login&status=success&request_token=fNKIEHASu6Bc6BM2oXRM6llYimmHiCVo"
    assert extract_request_token(url) == "fNKIEHASu6Bc6BM2oXRM6llYimmHiCVo"
    
    # 3. URL with trailing parameters or spaces
    url_trailing = "  https://127.0.0.1:5000/?request_token=myToken999&action=done  "
    assert extract_request_token(url_trailing) == "myToken999"

def test_gcp_execution_client_dispatches_payload():
    client = GCPExecutionClient(base_url="http://35.231.246.98:5000", auth_key="secret123")
    
    with patch("requests.post") as mock_post:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "success": True,
            "status": "COMPLETE",
            "order_id": "GCP_ORD_123",
            "fill_price": 28.50
        }
        mock_post.return_value = mock_resp
        
        res = client.execute_order(
            symbol="RELIANCE",
            option_symbol="RELIANCE26NOV1300CE",
            lot_size=250,
            entry_premium=28.50
        )
        
        assert res["success"] is True
        assert res["order_id"] == "GCP_ORD_123"
        mock_post.assert_called_once()
        
        # Verify headers sent
        call_kwargs = mock_post.call_args[1]
        assert call_kwargs["headers"]["X-Sovereign-Auth"] == "secret123"
        assert call_kwargs["json"]["symbol"] == "RELIANCE"
        assert call_kwargs["json"]["lot_size"] == 250

def test_flask_api_authentication_gates():
    client = app.test_client()
    
    # 1. Unauthenticated request to /api/orders/execute -> 401
    resp = client.post("/api/orders/execute", json={"symbol": "INFY"})
    assert resp.status_code == 401
    
    # 2. Unauthenticated request to /api/tokens/sync -> 401
    resp = client.get("/api/tokens/sync")
    assert resp.status_code == 401
    
    # 3. Authenticated request to /api/tokens/sync -> 200
    with patch("ui.token_app.load_persistent_tokens", return_value={"KITE_ACCESS_TOKEN": "token_abc"}):
        resp = client.get("/api/tokens/sync", headers={"X-Sovereign-Auth": "midnight123"})
        assert resp.status_code == 200
        data = resp.get_json()
        assert data["KITE_ACCESS_TOKEN"] == "token_abc"
