import os
import subprocess
import time

GCP_INSTANCE = "midnight-sovereign-engine"
ZONE = "us-east1-b"
PROJECT = "myinterviewwithai"
REMOTE_ENV_PATH = "~/StockSuggetion/.env"
LOCAL_ENV_PATH = "/Users/poonamsalke/.gemini/antigravity/scratch/midnight_sovereign/.env"

def sync_env():
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting local sync from GCP...")
    
    # Run gcloud compute ssh to cat the remote .env
    cmd = [
        "gcloud", "compute", "ssh", GCP_INSTANCE,
        "--zone", ZONE,
        "--project", PROJECT,
        "--quiet",
        "--command", f"cat {REMOTE_ENV_PATH}"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
        remote_env_content = result.stdout
        
        if not remote_env_content or "KITE_ACCESS_TOKEN" not in remote_env_content:
            print("Error: Remote .env seems empty or missing KITE_ACCESS_TOKEN.")
            return

        # Parse tokens from remote
        remote_tokens = {}
        for line in remote_env_content.split("\n"):
            line = line.strip()
            if line.startswith("KITE_ACCESS_TOKEN=") or line.startswith("EXEC_KITE_ACCESS_TOKEN="):
                key, val = line.split("=", 1)
                remote_tokens[key] = val
                
        if not remote_tokens:
            print("Error: No tokens found in remote .env")
            return
            
        print(f"Found tokens on remote: {list(remote_tokens.keys())}")

        # Update local .env
        with open(LOCAL_ENV_PATH, "r") as f:
            local_lines = f.readlines()
            
        updated = False
        with open(LOCAL_ENV_PATH, "w") as f:
            for line in local_lines:
                # We specifically do NOT overwrite LIVE_BUY to ensure local stays False
                updated_line = False
                for key, val in remote_tokens.items():
                    if line.startswith(f"{key}="):
                        f.write(f"{key}={val}\n")
                        updated_line = True
                        updated = True
                        break
                if not updated_line:
                    f.write(line)

        if updated:
            print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Successfully synced access tokens to local .env!")
        else:
            print("Tokens were not found in local .env to overwrite (Check format).")

    except subprocess.CalledProcessError as e:
        print(f"Failed to SSH to GCP: {e.stderr}")

if __name__ == "__main__":
    sync_env()
