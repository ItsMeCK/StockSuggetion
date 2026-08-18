import sys
from unittest.mock import patch
import datetime
from scripts.live_hourly_job import run_hourly_evaluation

class MockDatetime(datetime.datetime):
    @classmethod
    def now(cls, tz=None):
        if tz:
            return tz.localize(datetime.datetime(2026, 8, 17, 12, 15, 0)) if hasattr(tz, 'localize') else datetime.datetime(2026, 8, 17, 12, 15, 0, tzinfo=tz)
        return datetime.datetime(2026, 8, 17, 12, 15, 0)

with patch('scripts.live_hourly_job.datetime', MockDatetime):
    run_hourly_evaluation()
