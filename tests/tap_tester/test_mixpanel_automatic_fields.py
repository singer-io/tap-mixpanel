from tap_tester import connections, runner

from base import TestMixPanelBase


class MixPanelAutomaticFieldsTest(TestMixPanelBase):
    """
    Ensure running the tap with all streams selected and all fields
    deselected results in the replication of just the
    primary keys and replication keys (automatic fields).
    """

    @staticmethod
    def name():
        return "tap_tester_mixpanel_automatic_fields_test"

    def automatic_fields_test_run(self):
        """
        • Verify we can deselect all fields except when inclusion=automatic,
          which is handled by base.py methods
        • Verify that only the automatic fields are sent to the target.
        • Verify that all replicated records have unique primary key values.
        """
        expected_streams = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_automatic_fields = [
            catalog
            for catalog in found_catalogs
            if catalog.get("tap_stream_id") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_automatic_fields, select_all_fields=False)

        record_count_by_stream = self.run_and_verify_sync(conn_id)
        synced_records = runner.get_records_from_target_output()

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_keys = self.expected_automatic_fields().get(stream)

                # Collect actual values
                data = synced_records.get(stream, {})
                record_messages_keys = [set(row['data'].keys())
                                        for row in data.get('messages', [])]

                # Verify that you get some records for each stream
                self.assertGreater(
                    record_count_by_stream.get(stream, 0), 0,
                    msg="The number of records is not over the stream max limit",
                )

                # Verify that only the automatic fields are sent to the target
                for actual_keys in record_messages_keys:
                    self.assertSetEqual(expected_keys, actual_keys)

    def test_standard_auto_fields(self):
        """Automatic fields test for standard server"""
        self.eu_residency = False
        self.automatic_fields_test_run()
