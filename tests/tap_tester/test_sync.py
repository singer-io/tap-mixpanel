import tap_tester.connections as connections
from base import TestMixPanelBase


class MixPanelSyncTest(TestMixPanelBase):
    def name(self):
        return "mix_panel_sync_test"


    def sync_test_run(self):
        """
        Testing that sync creates the appropriate catalog with valid metadata.
        • Verify that all fields and all streams have selected set to True in the metadata
        """
        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)


        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                      if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(conn_id,test_catalogs_all_fields)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        # check if all streams have collected records
        for stream in streams_to_test:
            self.assertGreater(
                record_count_by_stream.get(stream, 0), 0,
                msg="failed to replicate any data for stream : {}".format(stream)
            )


    def test_run(self):
        #Sync test for standard server
        self.eu_residency = False
        self.sync_test_run()

        #Sync test for EU recidency server
        self.eu_residency = True
        self.sync_test_run()
