import os
import logging
import requests
from typing import Dict, Any, Optional
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

class GCPExecutionClient:
    """
    Laptop Client for Sovereign GCP Execution Gateway (35.231.246.98:5000).
    Dispatches automated orders from local crons to the GCP VM.
    """

    def __init__(self, base_url: Optional[str] = None, auth_key: Optional[str] = None):
        load_dotenv()
        self.base_url = (base_url or os.getenv("GCP_GATEWAY_URL", "http://35.231.246.98:5000")).rstrip("/")
        self.auth_key = auth_key or os.getenv("SOVEREIGN_AUTH_KEY", os.getenv("UI_PASSWORD", "midnight123"))

    def execute_order(
        self,
        symbol: str,
        option_symbol: str,
        lot_size: int,
        entry_premium: float,
        direction: str = "LONG",
        instrument_type: str = "CE",
        best_ask: Optional[float] = None,
        catalyst: str = "",
        score: float = 85.0,
        holding_type: str = "TYPE_C_BTST",
        is_amo: bool = False
    ) -> Dict[str, Any]:
        """
        Dispatches order execution payload to GCP Gateway with timeout & error handling.
        """
        payload = {
            "symbol": symbol,
            "option_symbol": option_symbol,
            "lot_size": lot_size,
            "entry_premium": entry_premium,
            "direction": direction,
            "instrument_type": instrument_type,
            "best_ask": best_ask,
            "catalyst": catalyst,
            "score": score,
            "holding_type": holding_type,
            "is_amo": is_amo
        }

        url = f"{self.base_url}/api/orders/execute"
        headers = {
            "Content-Type": "application/json",
            "X-Sovereign-Auth": self.auth_key
        }

        logger.info(f"🚀 [GCP CLIENT] Dispatching order to {url} for {symbol} ({option_symbol})...")
        try:
            # 25-second timeout to allow the 10-second verification window on GCP
            resp = requests.post(url, json=payload, headers=headers, timeout=25)
            if resp.status_code == 200:
                data = resp.json()
                logger.info(f"✅ [GCP CLIENT] Order response received: {data}")
                return data
            else:
                logger.error(f"❌ [GCP CLIENT] HTTP {resp.status_code}: {resp.text}")
                return {
                    "success": False,
                    "error": f"HTTP_{resp.status_code}",
                    "message": resp.text
                }
        except Exception as e:
            logger.error(f"❌ [GCP CLIENT] Connection failure to GCP Gateway: {e}")
            return {
                "success": False,
                "error": "CONNECTION_FAILED",
                "message": str(e)
            }

    def sync_tokens_from_gcp(self, update_local_env: bool = True) -> Dict[str, Any]:
        """
        Pulls refreshed access tokens from GCP Gateway to local laptop.
        """
        url = f"{self.base_url}/api/tokens/sync"
        headers = {"X-Sovereign-Auth": self.auth_key}

        try:
            resp = requests.get(url, headers=headers, timeout=10)
            if resp.status_code == 200:
                tokens = resp.json()
                logger.info(f"✅ [GCP CLIENT] Successfully pulled tokens from GCP: {list(tokens.keys())}")
                if update_local_env:
                    self._update_env_file("KITE_ACCESS_TOKEN", tokens.get("KITE_ACCESS_TOKEN", ""))
                    self._update_env_file("EXEC_KITE_ACCESS_TOKEN", tokens.get("EXEC_KITE_ACCESS_TOKEN", ""))
                return tokens
            else:
                logger.warning(f"Could not sync tokens: HTTP {resp.status_code} - {resp.text}")
                return {}
        except Exception as e:
            logger.warning(f"Failed to connect to GCP token sync endpoint: {e}")
            return {}

    def _update_env_file(self, key: str, value: str, filepath: str = ".env"):
        if not value:
            return
        if not os.path.exists(filepath):
            with open(filepath, "w") as f:
                f.write(f"{key}='{value}'\n")
            return

        with open(filepath, "r") as f:
            lines = f.readlines()
        
        updated = False
        with open(filepath, "w") as f:
            for line in lines:
                if line.startswith(f"{key}="):
                    f.write(f"{key}='{value}'\n")
                    updated = True
                else:
                    f.write(line)
            if not updated:
                f.write(f"{key}='{value}'\n")
