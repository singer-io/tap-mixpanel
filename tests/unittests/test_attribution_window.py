from datetime import datetime
import unittest
import pytz
from parameterized import parameterized
from singer.utils import strptime_to_utc
from tap_mixpanel.streams import Export

NOW_TIME = datetime(year=2022, month=10, day=10).replace(tzinfo=pytz.UTC)


class TestAttributionWindow(unittest.TestCase):
    """
    Test that for attribution window > current_date - (start_date/bookmark),
    Sync start from start_date.
    """

    @parameterized.expand([
        ["start_date_before_attribution_window", "2022-09-24T00:00:00Z", "2022-10-05T00:00:00Z"],
        ["from_the_start_date", "2022-10-05T00:00:00Z", "2022-10-05T00:00:00Z"],
        ["attribution_window_before_start_date", "2022-10-07T00:00:00Z", "2022-10-05T00:00:00Z"],
    ])
    def test_start_window_value(self, test_name, last_datetime, expected_start_window):
        """
        Test given scenarios:
            - If start_date or bookmark is < attribution window, start_window will be as it is.
            - Other wise, start window will be start_date
        """

        stream = Export(None)
        date_window_size = 30
        attribution_window = 10

        start_window, _, _ = stream.define_bookmark_filters(days_interval=date_window_size,
                                                            last_datetime=last_datetime,
                                                            now_datetime=NOW_TIME,
                                                            attribution_window=attribution_window,
                                                            start_date="2022-10-05T00:00:00Z")

        # Verify that start window is expected
        self.assertEqual(start_window, strptime_to_utc(expected_start_window))
