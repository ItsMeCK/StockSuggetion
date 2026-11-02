import os
import json
import logging
import requests
from typing import Dict, Any, Optional
from dotenv import load_dotenv
from kiteconnect import KiteConnect

logger = logging.getLogger(__name__)

DEFAULT_SERVER_URL = "http://35.231.246.98:5000"
DEFAULT_AUTH_KEY = "midnight123"
LOCAL_TOKENS_FILE = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "kite_tokens.json")

def update_env_variable(key: str, value: str, env_path: str = ".env"):
    """Atomically updates a single key in the local .env file."""
    if not value:
        return
    # Also update in-memory os.environ immediately
    os.environ[key] = value

    if not os.path.exists(env_path):
        with open(env_path, "w") as f:
            f.write(f"{key}='{value}'\n")
        return

    with open(env_path, "r") as f:
        lines = f.readlines()

    updated = False
    with open(env_path, "w") as f:
        for line in lines:
            if line.startswith(f"{key}="):
                f.write(f"{key}='{value}'\n")
                updated = True
            else:
                f.write(line)
        if not updated:
            f.write(f"{key}='{value}'\n")

def sync_tokens_from_server(
    server_url: Optional[str] = None,
    auth_key: Optional[str] = None,
    env_path: str = ".env"
) -> Dict[str, str]:
    """
    Pulls fresh tokens (Data and Exec) from the GCP Gateway server API
    and updates both local .env and runtime os.environ.
    """
    load_dotenv(dotenv_path=env_path)
    base_url = (server_url or os.getenv("GCP_GATEWAY_URL", DEFAULT_SERVER_URL)).rstrip("/")
    key = auth_key or os.getenv("SOVEREIGN_AUTH_KEY", os.getenv("UI_PASSWORD", DEFAULT_AUTH_KEY))

    url = f"{base_url}/api/tokens/sync"
    headers = {"X-Sovereign-Auth": key}

    try:
        logger.info(f"🔄 Syncing access tokens from GCP Gateway: {url}...")
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            tokens = resp.json()
            data_token = tokens.get("KITE_ACCESS_TOKEN", "").strip()
            exec_token = tokens.get("EXEC_KITE_ACCESS_TOKEN", "").strip()

            if data_token:
                update_env_variable("KITE_ACCESS_TOKEN", data_token, env_path=env_path)
                logger.info(f"✅ Data Account Token updated: {data_token[:6]}...{data_token[-4:]}")
            if exec_token:
                update_env_variable("EXEC_KITE_ACCESS_TOKEN", exec_token, env_path=env_path)
                logger.info(f"✅ Exec Account Token updated: {exec_token[:6]}...{exec_token[-4:]}")

            # Persist to local data/kite_tokens.json
            try:
                os.makedirs(os.path.dirname(LOCAL_TOKENS_FILE), exist_ok=True)
                with open(LOCAL_TOKENS_FILE, "w") as f:
                    json.dump(tokens, f, indent=2)
            except Exception as e:
                logger.warning(f"Could not write to local tokens file: {e}")

            return tokens
        else:
            logger.warning(f"Failed to sync tokens from server: HTTP {resp.status_code} - {resp.text}")
            return {}
    except Exception as e:
        logger.warning(f"Server token sync connection failed: {e}")
        return {}

def fetch_data_token_from_server(server_url: Optional[str] = None, auth_key: Optional[str] = None) -> Optional[str]:
    """Calls the dedicated /api/tokens/data endpoint on GCP."""
    load_dotenv()
    base_url = (server_url or os.getenv("GCP_GATEWAY_URL", DEFAULT_SERVER_URL)).rstrip("/")
    key = auth_key or os.getenv("SOVEREIGN_AUTH_KEY", os.getenv("UI_PASSWORD", DEFAULT_AUTH_KEY))

    url = f"{base_url}/api/tokens/data"
    headers = {"X-Sovereign-Auth": key}

    try:
        resp = requests.get(url, headers=headers, timeout=8)
        if resp.status_code == 200:
            data = resp.json()
            token = data.get("access_token", "").strip()
            if token:
                update_env_variable("KITE_ACCESS_TOKEN", token)
                return token
    except Exception as e:
        logger.warning(f"Failed to fetch data token from server: {e}")
    return None

def get_data_token(auto_sync: bool = True) -> str:
    """
    Returns active KITE_ACCESS_TOKEN.
    If missing and auto_sync is True, syncs from GCP server.
    """
    load_dotenv()
    token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")
    if not token and auto_sync:
        tokens = sync_tokens_from_server()
        token = tokens.get("KITE_ACCESS_TOKEN", "").strip()
    return token

def get_exec_token(auto_sync: bool = True) -> str:
    """
    Returns active EXEC_KITE_ACCESS_TOKEN.
    If missing and auto_sync is True, syncs from GCP server.
    """
    load_dotenv()
    token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")
    if not token and auto_sync:
        tokens = sync_tokens_from_server()
        token = tokens.get("EXEC_KITE_ACCESS_TOKEN", "").strip()
    return token

def get_kite_data_client(auto_refresh: bool = True, timeout: int = 25) -> KiteConnect:
    """
    Returns initialized KiteConnect client for Data Account (YF4472).
    Automatically syncs from GCP server if credentials are not in environment.
    """
    load_dotenv()
    api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")

    if (not api_key or not access_token) and auto_refresh:
        sync_tokens_from_server()
        load_dotenv()
        api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
        access_token = os.getenv("KITE_ACCESS_TOKEN", "").strip("'\"")

    if not api_key or not access_token:
        raise ValueError("Missing regular KITE credentials in .env (even after server sync)")

    kite = KiteConnect(api_key=api_key, timeout=timeout)
    kite.set_access_token(access_token)
    return kite

def get_kite_exec_client(auto_refresh: bool = True, timeout: int = 25) -> KiteConnect:
    """
    Returns initialized KiteConnect client for Exec Account (HU0092).
    Automatically syncs from GCP server if credentials are not in environment.
    """
    load_dotenv()
    api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")

    if (not api_key or not access_token) and auto_refresh:
        sync_tokens_from_server()
        load_dotenv()
        api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
        access_token = os.getenv("EXEC_KITE_ACCESS_TOKEN", "").strip("'\"")

    if not api_key or not access_token:
        raise ValueError("Missing EXEC_KITE credentials in .env (even after server sync)")

    kite = KiteConnect(api_key=api_key, timeout=timeout)
    kite.set_access_token(access_token)
    return kite
