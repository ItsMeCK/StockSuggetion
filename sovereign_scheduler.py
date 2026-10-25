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
        return
    
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        requests.post(url, json={"chat_id": chat_id, "text": message, "parse_mode": "Markdown"})
    except Exception as e:
        logging.error(f"Failed to send Telegram: {e}")

def run_script(script_name, description):
    logging.info(f"--- RUNNING: {description} ({script_name}) ---")
    try:
        py_bin = os.path.join(os.getcwd(), "venv/bin/python")
        cmd = [py_bin, script_name]
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logging.info(f"✅ Success: {description}")
        else:
            logging.error(f"❌ Error running {script_name}: {result.stderr[:200]}")
            send_telegram(f"❌ *JOB ERROR [{description}]*: {result.stderr[:100]}")
    except Exception as e:
        logging.error(f"Scheduler exception on {script_name}: {e}")

# ==============================================================================
# 🕒 MIDNIGHT SOVEREIGN MASTER TRADING SCHEDULE (IST)
# ==============================================================================

# 0. Sunday 18:00 PM - Weekly Eligible Universe Build & Strategy Briefing
schedule.every().sunday.at("18:00").do(
    run_script,
    script_name="scripts/weekly_watchlist_builder.py",
    description="Weekly Eligible Universe Build & Email"
)

# 1. 09:08 AM - Pre-Open Futures Discovery & Daily Focus Watchlist Build
schedule.every().day.at("09:08").do(
    run_script,
    script_name="core/watchlist_evaluator.py",
    description="09:08 Pre-Open Watchlist Refresh & Email"
)

# 2. 09:16 AM - T+1 Morning BTST Liquidator (Exits overnight holds at open gap)
schedule.every().day.at("09:16").do(
    run_script,
    script_name="scripts/btst_liquidator.py",
    description="09:16 Morning BTST Liquidator"
)

# 3. 09:30 AM to 12:10 PM - Morning Paper Scalp Engine (Every 5 Minutes, ₹0 Live Risk)
def run_5m_morning_paper():
    now = datetime.now()
    is_morning = (
        (now.hour == 9 and now.minute >= 30) or
        (now.hour in (10, 11)) or
        (now.hour == 12 and now.minute <= 10)
    )
    if is_morning:
        run_script("scripts/live_5m_trigger_job.py", "5-Minute Precision Paper Scalp Engine")

schedule.every(5).minutes.do(run_5m_morning_paper)

# 4. 12:15 PM, 13:15 PM, 14:15 PM - Market-Wide Afternoon BTST Scanner (+₹2.13L Overnight Edge)
# Scans full 216 F&O universe at hourly candle close to capture institutional momentum
schedule.every().day.at("12:15").do(
    run_script,
    script_name="scripts/live_hourly_job.py",
    description="12:15 PM Market-Wide BTST Scanner"
)
schedule.every().day.at("13:15").do(
    run_script,
    script_name="scripts/live_hourly_job.py",
    description="13:15 PM Market-Wide BTST Scanner"
)
schedule.every().day.at("14:15").do(
    run_script,
    script_name="scripts/live_hourly_job.py",
    description="14:15 PM Market-Wide BTST Scanner"
)

# 5. 15:15 PM - Engine 2 3:15 PM Insider / BTST Sniper (100% Live Capital Edge)
schedule.every().day.at("15:15").do(
    run_script,
    script_name="scripts/anticipatory_execution_job.py",
    description="15:15 PM Insider / BTST Sniper"
)

# 6. 15:26 PM - BTST Order Fill Verifier & Overnight SL Safety Net
schedule.every().day.at("15:26").do(
    run_script,
    script_name="scripts/btst_fill_verifier.py",
    description="15:26 PM BTST Fill Verifier"
)

# 7. Minutely Active Position Monitor & Dynamic Exit Engine (09:15 to 15:30 IST)
def run_minutely_monitor():
    now = datetime.now()
    is_market_hours = (
        (now.hour == 9 and now.minute >= 15) or
        (10 <= now.hour <= 14) or
        (now.hour == 15 and now.minute <= 30)
    )
    if is_market_hours:
        run_script("scripts/monitor_positions.py", "Minutely Position Monitor & Exit Engine")

schedule.every(1).minutes.do(run_minutely_monitor)

# 8. 15:45 PM - EOD Portfolio & Section 43(5) Tax Ledger Reconciliation
schedule.every().day.at("15:45").do(
    run_script,
    script_name="generate_portfolio_dashboard.py",
    description="EOD Dashboard & Ledger Reconciliation"
)

if __name__ == "__main__":
    logging.info("🛡️ Sovereign Master Scheduler Active (Weekly + Pre-Open + 5m Paper + Afternoon BTST)...")
    send_telegram("🛡️ *SOVEREIGN MASTER ONLINE*\nSchedule Active:\n• Sun 18:00: Weekly Universe Build\n• 09:08 AM: Pre-Open Watchlist Refresh\n• 09:16 AM: BTST Liquidator\n• 09:30-12:10: 5m Paper Scalp Engine\n• 12:15, 13:15, 14:15: Market-Wide BTST Scanner\n• 15:15 PM: 3:15 PM BTST Sniper\n• 15:45 PM: EOD Audit")
    
    while True:
        schedule.run_pending()
        time.sleep(10)
