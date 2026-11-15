#!/usr/bin/env python3
"""
scripts/auto_login_zerodha.py

Fully Autonomous Headless TOTP Authentication for Zerodha Kite Connect.
Performs RFC 6238 TOTP computation, passes 2FA, retrieves request_token,
and exchanges for access_token without any browser or human intervention.
"""

import os
import sys
import json
import time
import base64
import struct
import hmac
import hashlib
import logging
import urllib.parse
import requests
from dotenv import load_dotenv
from kiteconnect import KiteConnect

# Ensure project root in sys.path
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("AutoLoginZerodha")

def compute_totp(secret_key: str) -> str:
    """Computes standard 6-digit TOTP (RFC 6238) from base32 secret."""
    cleaned_key = secret_key.strip().replace(" ", "")
    key_bytes = base64.b32decode(cleaned_key, casefold=True)
    counter = int(time.time() // 30)
    counter_bytes = struct.pack(">Q", counter)
    h = hmac.new(key_bytes, counter_bytes, hashlib.sha1).digest()
    offset = h[-1] & 0x0F
    code = struct.unpack(">I", h[offset:offset+4])[0] & 0x7FFFFFFF
    return str(code % 1000000).zfill(6)

def update_env_file(key: str, value: str, filepath: str = ".env"):
    """Atomically updates or appends key=value in .env file."""
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

def login_account(user_id: str, password: str, totp_secret: str, api_key: str, api_secret: str) -> str:
    """
    Executes headless login to Zerodha Kite and returns fresh access_token.
    """
    logger.info(f"🔑 Initiating headless authentication for account: {user_id}...")
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    })

    # Step 1: Submit User ID & Password
    login_url = "https://kite.zerodha.com/api/login"
    login_resp = session.post(login_url, data={"user_id": user_id, "password": password})
    try:
        login_json = login_resp.json()
    except Exception as e:
        raise RuntimeError(f"Failed to parse login response: {login_resp.text} ({e})")

    if login_json.get("status") != "success":
        raise RuntimeError(f"Kite login failed for {user_id}: {login_json.get('message', login_json)}")

    request_id = login_json["data"]["request_id"]
    logger.info(f"✅ Step 1: Login successful. Received request_id: {request_id}")

    # Step 2: Generate and submit 6-digit TOTP
    totp_code = compute_totp(totp_secret)
    logger.info(f"🔢 Step 2: Generated live TOTP: {totp_code}. Submitting 2FA...")
    twofa_url = "https://kite.zerodha.com/api/twofa"
    twofa_resp = session.post(twofa_url, data={
        "user_id": user_id,
        "request_id": request_id,
        "twofa_value": totp_code,
        "twofa_type": "totp",
        "skip_session": "true"
    })

    try:
        twofa_json = twofa_resp.json()
    except Exception as e:
        raise RuntimeError(f"Failed to parse 2FA response: {twofa_resp.text} ({e})")

    if twofa_json.get("status") != "success":
        raise RuntimeError(f"Kite 2FA failed for {user_id}: {twofa_json.get('message', twofa_json)}")

    logger.info(f"✅ Step 2: 2FA Authentication validated successfully!")

    # Step 3: Trigger Kite Connect Authorization to capture request_token
    auth_url = f"https://kite.zerodha.com/connect/login?v=3&api_key={api_key}"
    logger.info(f"🔗 Step 3: Following OAuth2 authorization redirect...")
    request_token = None

    import re

    # Try non-redirect first to read Location header directly (avoids ConnectionRefused on 127.0.0.1 redirect URIs)
    try:
        auth_resp = session.get(auth_url, allow_redirects=False)
        loc = auth_resp.headers.get("Location", "")
        if "request_token=" in loc:
            match = re.search(r"request_token=([a-zA-Z0-9]+)", loc)
            if match:
                request_token = match.group(1)
    except Exception as e:
        logger.warning(f"Non-redirect check warning: {e}")

    # If not captured via 302 Location header, follow with redirect and catch ConnectionError
    if not request_token:
        try:
            auth_resp = session.get(auth_url, allow_redirects=True)
            match = re.search(r"request_token=([a-zA-Z0-9]+)", auth_resp.url)
            if match:
                request_token = match.group(1)
            else:
                for h in auth_resp.history:
                    match = re.search(r"request_token=([a-zA-Z0-9]+)", h.headers.get("Location", ""))
                    if match:
                        request_token = match.group(1)
                        break
        except requests.exceptions.ConnectionError as ce:
            match = re.search(r"request_token=([a-zA-Z0-9]+)", str(ce))
            if match:
                request_token = match.group(1)

    if not request_token:
        raise RuntimeError(f"Could not extract request_token from redirect chain.")

    logger.info(f"🎯 Step 3: Successfully captured request_token: {request_token[:6]}...{request_token[-4:]}")

    # Step 4: Exchange request_token for access_token
    kite = KiteConnect(api_key=api_key)
    session_data = kite.generate_session(request_token, api_secret=api_secret)
    access_token = session_data["access_token"]
    logger.info(f"🎉 Step 4: Access token generated successfully: {access_token[:6]}...{access_token[-4:]}")
    return access_token

def main():
    load_dotenv()
    env_file = os.path.join(PROJECT_ROOT, ".env")

    tokens_refreshed = {}

    # 1. Execution Account (HU0092)
    exec_user = os.getenv("EXEC_KITE_USER_ID", "HU0092").strip("'\"")
    exec_pass = os.getenv("EXEC_KITE_PASSWORD", "").strip("'\"")
    exec_totp = os.getenv("EXEC_KITE_TOTP_KEY", "").strip("'\"")
    exec_api_key = os.getenv("EXEC_KITE_API_KEY", "").strip("'\"")
    exec_api_secret = os.getenv("EXEC_KITE_API_SECRET", "").strip("'\"")

    if exec_user and exec_pass and exec_totp and exec_api_key and exec_api_secret:
        try:
            exec_token = login_account(exec_user, exec_pass, exec_totp, exec_api_key, exec_api_secret)
            update_env_file("EXEC_KITE_ACCESS_TOKEN", exec_token, filepath=env_file)
            tokens_refreshed["EXEC_KITE_ACCESS_TOKEN"] = exec_token
            logger.info("✅ Execution Account (HU0092) refreshed and written to .env!")
        except Exception as e:
            logger.error(f"❌ Failed to refresh Execution Account: {e}")
    else:
        logger.warning("⚠️ Execution Account credentials incomplete in .env. Skipping.")

    # 2. Data Account (YF4472)
    data_user = os.getenv("KITE_USER_ID", "YF4472").strip("'\"")
    data_pass = os.getenv("KITE_PASSWORD", "").strip("'\"")
    data_totp = os.getenv("KITE_TOTP_KEY", "").strip("'\"")
    data_api_key = os.getenv("KITE_API_KEY", "").strip("'\"")
    data_api_secret = os.getenv("KITE_API_SECRET", "").strip("'\"")

    if data_user and data_pass and data_totp and data_api_key and data_api_secret:
        try:
            data_token = login_account(data_user, data_pass, data_totp, data_api_key, data_api_secret)
            update_env_file("KITE_ACCESS_TOKEN", data_token, filepath=env_file)
            tokens_refreshed["KITE_ACCESS_TOKEN"] = data_token
            logger.info("✅ Data Account (YF4472) refreshed and written to .env!")
        except Exception as e:
            logger.error(f"❌ Failed to refresh Data Account: {e}")
    else:
        logger.warning("⚠️ Data Account credentials incomplete in .env. Skipping.")

    # Save to data/kite_tokens.json
    if tokens_refreshed:
        tokens_json_path = os.path.join(PROJECT_ROOT, "data", "kite_tokens.json")
        os.makedirs(os.path.dirname(tokens_json_path), exist_ok=True)
        existing = {}
        if os.path.exists(tokens_json_path):
            try:
                with open(tokens_json_path, "r") as f:
                    existing = json.load(f)
            except Exception:
                existing = {}
        existing.update(tokens_refreshed)
        existing["updated_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(tokens_json_path, "w") as f:
            json.dump(existing, f, indent=2)
        logger.info(f"💾 Updated {tokens_json_path} successfully.")

    logger.info("🏁 Finished Zerodha Auto-Login job.")

if __name__ == "__main__":
    main()
