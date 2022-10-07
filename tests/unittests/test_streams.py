from unittest import mock
from datetime import datetime
import unittest
from parameterized import parameterized
import pytz
from singer.catalog import Catalog
from tap_mixpanel.streams import Cohorts, Funnels
from tap_mixpanel.client import MixpanelClient

START_DATE = '2022-07-30T00:00:00.000000Z'
CONFIG = {'start_date': START_DATE,
          'api_domain': 'API_DOMAIN',
          'api_secret': 'API_SECRET',
          'request_timeout': 300,
          'user_agent': 'user_agent'}
ID_OBJECT = {'id': 1}
NOW_TIME = datetime(year=2022, month=10, day=1).replace(tzinfo=pytz.UTC)
DATETIME_OBJ = datetime


def get_stream_catalog(stream_name, key_properties=[], is_selected=False):
    """Return catalog for stream"""
    return {
        "schema": {},
        "tap_stream_id": stream_name,
        "stream": stream_name,
        "metadata": [
            {
                "breadcrumb": [],
                "metadata":{"selected": is_selected}
            }
        ],
        "key_properties": key_properties
    }


CATALOG = Catalog.from_dict({
    "streams": [
        get_stream_catalog("export", []),
        get_stream_catalog("funnels", ["funnel_id", "date"]),
        get_stream_catalog("cohorts", ["id"]),
        get_stream_catalog("cohort_members", ["cohort_id", "distinct_id"]),
    ]
})


class TestStreamsUtils(unittest.TestCase):
    """
    Test all utility functions of streams module
    """

    @parameterized.expand([
        # ["test_name", "stream_name", "state", "expected_output"]
        ['test_empty_state', 'a', {}, "default"],
        ['test_empty_bookmark', 'a', {'b': 'bookmark_a'}, "default"],
        ['test_non_empty_bookmark', 'a', {'a': 'bookmark_a'}, "bookmark_a"]
    ])
    def test_get_bookmark(self, test_name, stream_name, state, expected_output):
        """
        Test get_bookmark function for the following scenarios,
        Case 1: Return default value if `bookmarks` key is not found in the state
        Case 2: Return default value if stream_name is not found in the bookmarks
        Case 3: Return actual bookmark value if it is found in the state
        """
        obj = Funnels(None)

        actual_output = obj.get_bookmark({"bookmarks": state}, stream_name, "default")

        self.assertEqual(expected_output, actual_output)

    @parameterized.expand([
        # ["test_name", "stream_name", "state", "bookmark_value"]
        ['test_empty_state', 'a', {}, "BOOKMARK"],
        ['test_empty_bookmark', 'a', {'b': 'state_b'}, "BOOKMARK"],
        ['test_non_empty_bookmark', 'a', {'a': 'state_a'}, "BOOKMARK"]
    ])
    @mock.patch("singer.write_state")
    @mock.patch("tap_mixpanel.streams.LOGGER.info")
    def test_write_bookmark(self, test_name, stream_name, state, bookmark_value,
                            mock_logger, mock_write_state):
        """
        Test get_bookmark function for the following scenarios,
        Case 1: Return default value if `bookmarks` key is not found in the state
        Case 2: Return default value if stream_name is not found in the bookmarks
        Case 3: Return actual bookmark value if it is found in the state
        """
        obj = Funnels(None)
        expected_output = {"bookmarks": {**state, stream_name: bookmark_value}}
        obj.write_bookmark({"bookmarks": state}, stream_name, bookmark_value)

        mock_logger.assert_called_with("Write state for stream: %s, value: %s",
                                       stream_name, bookmark_value)
        mock_write_state.assert_called_with(expected_output)


@mock.patch("datetime.datetime")
@mock.patch('singer.write_record')
@mock.patch('tap_mixpanel.client.MixpanelClient.request')
@mock.patch('singer.write_state')
class TestSyncEndpoint(unittest.TestCase):
    """
    Test sync endpoint method of different streams.
    """
    cohorts_responses = [
        [{"id": 111}],
        {
            "results": [
                {"$distinct_id": "48342", "$properties": {}}
            ]
        }
    ]
    funnel_parent_response = [
        {
            "funnel_id": 111111,
            "name": "Standard Funnel 1"
        }
    ]
    funnels_from_bookmark = [
        funnel_parent_response,
        {
            "data": {
                "2022-08-25": {},
                "2022-08-27": {},
                "2022-08-30": {},
            }
        },
        {
            "data": {
                "2022-10-01": {},
            }
        }
    ]
    funnels_from_start_date = [
        funnel_parent_response,
        {
            "data": {
                "2022-08-02": {},
                "2022-08-25": {},
            }
        },
        {
            "data": {
                "2022-08-29": {},
                "2022-08-30": {},
            }
        },
        {
            "data": {
                "2022-09-29": {},
                "2022-10-01": {},
            }
        },
    ]

    @parameterized.expand([
        ["only_parent_selected_with_0_record", ["cohorts"], [[]], 1, 0],
        ["only_parent_selected", ["cohorts"], [[{"id": 111}, {"id": 222}]], 1, 2],
        ["only_child_selected", ["cohort_members"], cohorts_responses, 2, 1],
        ["both_selected", ["cohorts", "cohort_members"], cohorts_responses, 2, 2],
    ])
    def test_full_table_streams(self, mock_singer, mock_request, mock_write_record, mock_datetime, test_name,
                                selected_streams, response, write_state_call_count, write_record_call_count):
        """
        Test sync_endpoint function for parent and child streams.
        """
        mock_datetime.now.return_value = NOW_TIME
        client = MixpanelClient(CONFIG['api_secret'],
                                CONFIG['api_domain'],
                                CONFIG['request_timeout'],
                                CONFIG['user_agent'])
        obj = Cohorts(client)

        mock_request.side_effect = response
        obj.sync(state={}, catalog=CATALOG, config=CONFIG,
                 start_date=START_DATE, selected_streams=selected_streams)

        # Verify that bookmark is not written for full table stream
        self.assertFalse(mock_singer.called)
        # Verify request method is called for the expected no of time.
        self.assertEqual(mock_request.call_count, write_state_call_count)
        # Verify that write_record is called for the expected no of time.
        self.assertEqual(mock_write_record.call_count, write_record_call_count)

    @parameterized.expand([
        ["without_state", {}, funnels_from_start_date, 4, 6],
        ["with_state", {"bookmarks": {"funnels": "2022-08-26T00:00:00Z"}}, funnels_from_bookmark, 3, 3],
    ])
    def test_incremental_streams(self, mock_write_state, mock_request, mock_write_record, mock_datetime,
                                 test_name, state, response, request_call_count, write_record_call_count):
        """
        Test sync_endpoint function for parent and child streams.
        """
        mock_datetime.now.return_value = NOW_TIME
        client = MixpanelClient(CONFIG['api_secret'],
                                CONFIG['api_domain'],
                                CONFIG['request_timeout'],
                                CONFIG['user_agent'])
        obj = Funnels(client)

        # As We have kept last record replication value fix, expected bookmark will be fixed
        expected_bookmark = {"bookmarks": {'funnels': '2022-10-01T00:00:00Z'}}

        mock_request.side_effect = response
        obj.sync(state=state, catalog=CATALOG, config=CONFIG,
                 start_date=START_DATE, selected_streams=["funnels"])

        # Verify that bookmark is written for INCREMENTAL stream
        self.assertTrue(mock_write_state.called)
        mock_write_state.assert_called_with(expected_bookmark)
        # Verify request method is called for the expected no of time.
        self.assertEqual(mock_request.call_count, request_call_count)
        # Verify that write_record is called for the expected no of time.
        self.assertEqual(mock_write_record.call_count, write_record_call_count)
