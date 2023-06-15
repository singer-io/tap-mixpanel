import unittest
from unittest import mock
from parameterized import parameterized
from singer import Catalog
from tap_mixpanel.sync import write_schemas_recursive, get_streams_to_sync, sync


def get_stream_catalog(stream_name, is_selected=False):
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
        "key_properties": []
    }


def get_catalog(parent=False, child=False):
    """Return complete catalog"""

    return Catalog.from_dict({
        "streams": [
            get_stream_catalog("engage"),
            get_stream_catalog("cohorts", parent),
            get_stream_catalog("cohort_members", child),
            get_stream_catalog("revenue", parent),
        ]
    })


class TestSyncFunctions(unittest.TestCase):
    """
    Test `sync` function.
    """

    @parameterized.expand([
        # ["test_name", "mock_catalog", "selected_streams", "synced_streams"]
        ["only_parent_selected", get_catalog(parent=True), ["cohorts", "revenue"], 2],
        ["only_child_selected", get_catalog(child=True), ["cohort_members"], 1],
        ["both_selected", get_catalog(parent=True, child=True), ["cohorts", "cohort_members", "revenue"], 2],
        ["No_streams_selected", get_catalog(), [], 0],
    ])
    @mock.patch("singer.write_state")
    @mock.patch("singer.write_schema")
    @mock.patch("tap_mixpanel.streams.MixPanel.sync")
    def test_sync(self, test_name, mock_catalog, selected_streams, synced_streams,
                  mock_sync_endpoint, mock_write_schemas, mock_write_state):
        """
        Test sync function.
        """
        client = mock.Mock()
        sync(client=client, config={}, state={}, catalog=mock_catalog, start_date="START_DATE")

        # Verify write schema is called for selected streams
        self.assertEqual(mock_write_schemas.call_count, len(selected_streams))
        for stream in selected_streams:
            mock_write_schemas.assert_any_call(stream, mock.ANY, mock.ANY)

        # Verify sync object was called for syncing parent streams
        self.assertEqual(mock_sync_endpoint.call_count, synced_streams)


class TestGetStreamsToSync(unittest.TestCase):
    """
    Testcase for `get_stream_to_sync` in sync.
    """

    @parameterized.expand([
        # ["test_name", "selected_streams", "expected_streams"]
        ['test_parent_selected', ["funnels", "cohorts"], ["funnels", "cohorts"]],
        ['test_child_selected', ["cohort_members"], ["cohorts"]],
        ['test_both_selected', ["cohorts", "cohort_members", "revenue"], ["cohorts", "revenue"]]
    ])
    def test_sync_streams(self, test_name, selected_streams, expected_streams):
        """
        Test that if an only child is selected in the catalog,
        then `get_stream_to_sync` returns the parent streams if selected stream is child.
        """
        sync_streams = get_streams_to_sync(selected_streams)

        # Verify that the expected list of streams is returned
        self.assertEqual(sync_streams, expected_streams)


@mock.patch("singer.write_schema")
class TestWriteSchemas(unittest.TestCase):
    """
    Test `write_schemas` function.
    """

    test_catalog = Catalog.from_dict({"streams": [
        get_stream_catalog("cohorts"),
        get_stream_catalog("cohort_members"),
        get_stream_catalog("revenue")
    ]})

    @parameterized.expand([
        # ["test_name", "selected_streams", "mock_write_schema"]
        ["parents_selected", ["cohorts"]],
        ["child_selected", ["cohort_members"]],
        ["parent_and_child_selected", ["cohorts", "cohort_members"]],
    ])
    def test_write_schema(self, mock_write_schema, test_name, selected_streams):
        """
        Test that only schema is written for only selected streams.
        """
        write_schemas_recursive("cohorts", self.test_catalog, selected_streams)
        # Verify write_schema function is called for only selected streams.
        self.assertEqual(mock_write_schema.call_count, len(selected_streams))
        for stream in selected_streams:
            mock_write_schema.assert_any_call(stream, mock.ANY, mock.ANY)

    @mock.patch("tap_mixpanel.streams.LOGGER.error")
    def test_OS_exception(self, mock_logger, mock_schema):
        """
        Test on occurrence of OS error, Expected error logger is printed.
        """
        mock_schema.side_effect = OSError()
        with self.assertRaises(OSError):
            write_schemas_recursive("cohorts", self.test_catalog, ["cohorts"])

        # Verify expected logger is printed
        mock_logger.assert_called_with("OS Error writing schema for: %s", "cohorts")
