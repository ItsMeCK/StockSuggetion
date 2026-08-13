import datetime
from unittest.mock import patch
from scripts.live_hourly_job import run_hourly_evaluation

# Fake the time to be 10:16 AM
fake_time = datetime.datetime.now().replace(hour=10, minute=16)

class MockDatetime(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return fake_time
        return fake_time.astimezone(tz)

with patch('scripts.live_hourly_job.datetime', MockDatetime):
    try:
        run_hourly_evaluation()
    except Exception as e:
        print(f"\nCRASH DETECTED: {e}")
