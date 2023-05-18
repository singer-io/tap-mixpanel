import unittest
from unittest import mock
from datetime import datetime
from singer.catalog import Catalog
import pytz
from tap_mixpanel import main

TEST_CONFIG = {
    "start_date": "2022-09-01",
    "project_timezone": "US/Pacific",
    "api_secret": "API_SECRET",
    "attribution_window": 5,
    "user_agent": "USER_AGENT"
}


class MockArgs:
    """Mock args object class"""

    def __init__(self, config=None, catalog=None, state={}, discover=False) -> None:
        self.config = config
        self.catalog = catalog
        self.state = state
        self.discover = discover


# Setting now time to fix value
@mock.patch("singer.utils.now", return_value=datetime(year=2022, month=10, day=1).replace(tzinfo=pytz.UTC))
@mock.patch("tap_mixpanel.MixpanelClient.check_access")
@mock.patch("singer.utils.parse_args")
@mock.patch("tap_mixpanel._sync")
class TestSyncMode(unittest.TestCase):
    """
    Test the main function for sync mode.
    """

    mock_catalog = {"streams": [
        {"stream": "invoices", "schema": {}, "metadata": {}}]}

    @mock.patch("tap_mixpanel._discover")
    @mock.patch("tap_mixpanel.LOGGER.warning")
    def test_sync_with_catalog(self, mock_logger, mock_discover, mock_sync,
                               mock_args, mock_check_access, mock_now):
        """
        Test sync mode with catalog given in args.
        """
        test_config = {**TEST_CONFIG, "start_date": "2020-09-01"}

        mock_args.return_value = MockArgs(config=test_config,
                                          catalog=Catalog.from_dict(self.mock_catalog))
        main()

        # Verify `_sync` is called with expected arguments
        # Start_date will be set as 1 year from now date-time(2022-10-01)
        mock_sync.assert_called_with(client=mock.ANY,
                                     config=test_config,
                                     catalog=Catalog.from_dict(self.mock_catalog),
                                     state={},
                                     start_date="2021-10-01T00:00:00.000000Z")

        # verify `_discover` function is not called
        self.assertFalse(mock_discover.called)

        # Verify that for start_date before 1 year given warning logger is printed
        mock_logger.assert_any_call("start_date greater than 1 year maximum for API.")

    @mock.patch("tap_mixpanel._discover")
    def test_without_catalog(self, mock_discover, mock_sync, mock_args, mock_check_access, mock_now):
        """
        Test sync mode without catalog given in args.
        """

        discover_catalog = Catalog.from_dict(self.mock_catalog)
        mock_discover.return_value = discover_catalog
        mock_args.return_value = MockArgs(config=TEST_CONFIG)
        main()

        # verify `_discover` and `_sync` function is called
        self.assertTrue(mock_discover.called)
        self.assertTrue(mock_sync.called)

    def test_sync_with_state(self, mock_sync, mock_args, mock_check_access, mock_now):
        """
        Test sync mode with the state given in args.
        """
        mock_state = {"bookmarks": {"projec ts": ""}}
        mock_args.return_value = MockArgs(config=TEST_CONFIG,
                                          catalog=Catalog.from_dict(self.mock_catalog),
                                          state=mock_state)
        main()

        # Verify `_sync` is called with expected arguments
        mock_sync.assert_called_with(client=mock.ANY,
                                     config=TEST_CONFIG,
                                     catalog=Catalog.from_dict(self.mock_catalog),
                                     state=mock_state,
                                     start_date=TEST_CONFIG["start_date"])

    @mock.patch("tap_mixpanel._discover")
    def test_discover_mode(self, mock_discover, mock_sync, mock_args, mock_check_access, mock_now):
        """
        Test discover mode calls discover function to generate catalog.
        """

        discover_catalog = Catalog.from_dict(self.mock_catalog)
        mock_discover.return_value = discover_catalog
        mock_args.return_value = MockArgs(config=TEST_CONFIG,
                                          discover=True)
        main()

        # Verify `_discover` is called
        self.assertTrue(mock_discover.called)
