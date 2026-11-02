#!/usr/bin/env python3
"""
Sovereign Token Sync CLI Tool
Synchronizes active Kite access tokens from the GCP Gateway (35.231.246.98:5000)
into the local laptop's .env and data/kite_tokens.json.
"""
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import logging
from core.token_manager import sync_tokens_from_server, fetch_data_token_from_server

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    print("=" * 60)
    print("🔄 SOVEREIGN TOKEN SYNCHRONIZER: PULLING FROM GCP GATEWAY")
    print("=" * 60)
    
    tokens = sync_tokens_from_server()
    if tokens:
        data_token = tokens.get("KITE_ACCESS_TOKEN", "")
        exec_token = tokens.get("EXEC_KITE_ACCESS_TOKEN", "")
        updated_at = tokens.get("updated_at", "N/A")
        
        print("\n✅ Tokens successfully updated in local .env:")
        if data_token:
            print(f"   📊 Data Account (YF4472): {data_token[:8]}...{data_token[-6:]}")
        if exec_token:
            print(f"   ⚡ Exec Account (HU0092): {exec_token[:8]}...{exec_token[-6:]}")
        print(f"   🕒 Last Refreshed: {updated_at}")
        print("=" * 60)
    else:
        print("\n❌ Failed to sync tokens from GCP Gateway. Check network connection or server status.")
        sys.exit(1)

if __name__ == "__main__":
    main()
