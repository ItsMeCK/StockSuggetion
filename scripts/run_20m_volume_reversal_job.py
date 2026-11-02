#!/usr/bin/env python3
"""
20-Minute Volume Reversal Guardian Job
Schedule: Runs every 20 minutes between 09:25 and 15:15 IST (e.g. */20 9-15 * * 1-5).
Evaluates active F&O positions on HU0092, consults the Gemini LLM Arbiter, and liquidates on volume reversal.
"""
import os
import sys
from datetime import datetime

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import logging
from dotenv import load_dotenv
from core.smart_volume_reversal_engine import SmartVolumeReversalEngine

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    now = datetime.now()
    logger.info(f"⚡ [20M GUARDIAN] Starting cycle at {now.strftime('%Y-%m-%d %H:%M:%S IST')}...")
    
    # Check market hours (09:15 to 15:30)
    current_time = now.time()
    from datetime import time as dt_time
    if not (dt_time(9, 15) <= current_time <= dt_time(15, 30)):
        logger.info("Outside NSE market trading hours (09:15 - 15:30). Exiting.")
        return

    engine = SmartVolumeReversalEngine()
    results = engine.run_20m_cycle(now=now)
    logger.info(f"✅ [20M GUARDIAN] Completed cycle. Evaluated {len(results)} positions.")

if __name__ == "__main__":
    main()
