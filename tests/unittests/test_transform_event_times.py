from tap_mixpanel.transform import transform_event_times
import pytz
import unittest
from datetime import datetime

UTC = pytz.utc


class TestTransformEventTimes(unittest.TestCase):

    def test_utc_now(self):
        
        input_time = datetime.utcnow()
        
        record = {"time": input_time.timestamp()}
        project_timezone = "UTC"
        
        actual = transform_event_times(record, project_timezone)
        expected = {"time": input_time.astimezone(UTC).strftime("%04Y-%m-%dT%H:%M:%S.000000Z")}
        
        self.assertEqual(expected, actual)
        
        
    def test_eastern_time(self):
        project_timezone = "US/Eastern"
        EASTERN = pytz.timezone(project_timezone)
        # This gives us 2021-08-12T11:00:00-4:00
        input_time = EASTERN.localize(datetime(2021, 8, 12, 11, 0, 0))
        
        record = {"time": input_time.timestamp()}
        actual = transform_event_times(record, project_timezone)
        
        expected = {"time": input_time.astimezone(UTC).strftime("%04Y-%m-%dT%H:%M:%S.000000Z")}

        self.assertEqual(expected, actual)