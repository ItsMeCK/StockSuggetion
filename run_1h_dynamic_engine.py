import time
import logging
import schedule
from pipeline.intraday_1h_engine import Intraday1HEngine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def job():
    logging.info("Starting 1-Hour Intraday Execution Cycle...")
    engine = Intraday1HEngine()
    engine.run_hourly_cycle()
    logging.info("Cycle complete. Waiting for next tick...")

def run_scheduler():
    logging.info("Initializing 1-Hour Dynamic Execution Scheduler...")
    
    # Schedule runs at standard market hours
    market_times = ["09:30", "10:30", "11:30", "12:30", "13:30", "14:30", "15:15"]
    
    for t in market_times:
        schedule.every().day.at(t).do(job)
        logging.info(f"Scheduled cycle for {t} IST")
        
    logging.info("Scheduler active. Press Ctrl+C to exit.")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == "__main__":
    import sys
    # Allow --dry-run to execute immediately once for testing
    if len(sys.argv) > 1 and sys.argv[1] == "--dry-run":
        logging.info("Executing Single Dry Run...")
        job()
    else:
        run_scheduler()
