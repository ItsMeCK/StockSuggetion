import time
import schedule
import subprocess
import os
import requests
import logging
from datetime import datetime
from dotenv import load_dotenv

# Setup Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sovereign_scheduler.log"),
        logging.StreamHandler()
    ]
)

load_dotenv('.env')

def send_telegram(message):
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logging.warning("Telegram Token or Chat ID missing in .env")
        return
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"})
    except Exception as e:
        logging.error(f"Failed to send Telegram: {e}")

def run_engine(mode):
    logging.info(f"--- RUNNING ENGINE: {mode.upper()} ---")
    try:
        # Run the engine
        cmd = [os.path.join(os.getcwd(), "venv/bin/python3"), "run_universal_engine.py", "--mode", mode]
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            logging.info(f"Engine {mode} complete.")
            # If it's a live run, check for ignitions (simple heuristic for now)
            if mode == "live":
                send_telegram(f"🛡️ *MID-DAY SNIPER COMPLETE*\nDashboard updated. Check for 🚀 IGNITION signals now!")
            else:
                send_telegram(f"🏛️ *EOD AUDIT COMPLETE*\nTuesday Watchlist is ready on your dashboard.")
        else:
            logging.error(f"Engine failed: {result.stderr}")
            send_telegram(f"❌ *ENGINE ERROR*: {result.stderr[:100]}")
            
    except Exception as e:
        logging.error(f"Scheduler error: {e}")

# SCHEDULES (IST TIME)
# 12:00 PM - Mid-Day Sniper
schedule.every().day.at("12:00").do(run_engine, mode="live")

# 15:30 PM - Final EOD Audit & Tomorrow's List
schedule.every().day.at("15:30").do(run_engine, mode="eod")

if __name__ == "__main__":
    logging.info("Sovereign Scheduler Active. Waiting for Market Hours...")
    send_telegram("🛡️ *SOVEREIGN ONLINE*\nI am now monitoring the market from your laptop. Will alert you at 12:00 PM and 3:30 PM.")
    
    while True:
        schedule.run_pending()
        time.sleep(60)
