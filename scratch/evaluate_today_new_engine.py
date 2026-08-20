import os
import sys
import pytz
from datetime import datetime, timezone, timedelta
from unittest.mock import patch
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

def run_backtest_for_today():
    from scripts.live_hourly_job import run_hourly_evaluation
    
    # Today's date is 2026-08-18
    # Trading hours in IST: 10:15, 11:15, 12:15, 13:15, 14:15
    ist_tz = pytz.timezone("Asia/Kolkata")
    today = datetime(2026, 8, 18, tzinfo=ist_tz)
    
    hours_to_test = [10, 11, 12, 13, 14]
    
    for h in hours_to_test:
        print(f"\n{'='*50}")
        print(f"🕒 RUNNING BACKTEST FOR HOUR: {h}:15 IST")
        print(f"{'='*50}")
        
        target_ist = today.replace(hour=h, minute=15, second=0)
        target_utc = target_ist.astimezone(timezone.utc)
        
        # Patch datetime to simulate we are at this exact hour
        class MockDatetime(datetime):
            @classmethod
            def now(cls, tz=None):
                if tz is None:
                    # Return local time equivalent to target_ist
                    return target_ist.replace(tzinfo=None)
                return target_utc.astimezone(tz)
                
            @classmethod
            def utcnow(cls):
                return target_utc.replace(tzinfo=None)
                
        # Also patch execute_trade so we don't send emails or place trades
        with patch('scripts.live_hourly_job.datetime', MockDatetime), \
             patch('scripts.live_hourly_job.execute_trade') as mock_exec, \
             patch('alerts.email_notifier.SovereignEmailer.send_scorecard') as mock_email:
             
             # Prevent re-fetching massive parquet if already downloaded?
             # Actually, let it fetch from parquet, it's fast. But intraday_ingestion might fetch 500 API calls!
             # Let's mock intraday_ingestion.fetch_data to be a no-op so it just uses existing parquet.
             with patch('pipeline.intraday_ingestion.IntradayIngestionEngine.fetch_data') as mock_ingest:
                 run_hourly_evaluation()

if __name__ == "__main__":
    run_backtest_for_today()
