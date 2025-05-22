# utils/time_utils.py

from datetime import datetime, timedelta, timezone
from config.settings import SNAPSHOT_TIME_WINDOW_MINUTES

def get_snapshot_time_window():
    """Returns (start_time, end_time) in UTC timezone."""
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(minutes=SNAPSHOT_TIME_WINDOW_MINUTES)
    return start_time, end_time
