"""
Test tap combined
"""

import unittest
import os
import re
from datetime import datetime as dt
from datetime import timedelta
from tap_tester import menagerie
import tap_tester.runner as runner
import tap_tester.connections as connections
from tap_tester.scenario import SCENARIOS
from base import TestMixPanelBase

class MixPanelAutomaticFieldsTest(TestMixPanelBase):
    """
    Ensure running the tap with all streams selected and all fields deselected results in the replication of just the 
    primary keys and replication keys (automatic fields).
    """
    def name(self):
        return "mix_panel_automatic_fields_test"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {
            'start_date': self.get_start_date(),
            'date_window_size': '7',
            'attribution_window': '14',
            'project_timezone': 'US/Pacific',
            'select_properties_by_default': 'false'
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        when no fields are selected and only the automatic fields are replicated.
        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        streams_to_test = self.expected_streams()
        
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_automatic_fields = [catalog for catalog in found_catalogs
                                      if catalog.get('tap_stream_id') in streams_to_test]
        
        self.perform_and_verify_table_and_field_selection(conn_id, test_catalogs_automatic_fields, select_all_fields=False,)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                # collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row['data'].keys()) for row in data.get('messages', [])]


                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, -1), 0,
                    msg="The number of records is not over the stream max limit")

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)
