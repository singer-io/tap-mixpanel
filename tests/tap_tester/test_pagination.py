import tap_tester.connections as connections
import tap_tester.runner as runner
from base import TestMixPanelBase


class MixPanelPaginationTest(TestMixPanelBase):
    def name(self):
        return "mix_panel_pagination_test"


    def test_run(self):
        """
        Verify that for each stream you can get multiple pages of data
        and that when all fields are selected more than the automatic fields are replicated.
        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """
        page_size = 1
        streams_to_test = self.expected_streams()
        
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                      if catalog.get('tap_stream_id') in streams_to_test]
        
        self.perform_and_verify_table_and_field_selection(conn_id,test_catalogs_all_fields)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        synced_records = runner.get_records_from_target_output()

        for stream in streams_to_test:
            with self.subTest(stream=stream):
                # expected values
                expected_primary_keys = self.expected_pks()

                # verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, 0)
                self.assertGreater(record_count_sync,self.API_LIMIT,
                msg="The number of records is not over the stream max limit")

                primary_keys_list = [(message.get('data').get(expected_pk) for expected_pk in expected_primary_keys)
                                       for message in synced_records.get(stream).get('messages')
                                       if message.get('action') == 'upsert']

                # verify that the automatic fields are sent to the target
                
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).issuperset(self.expected_pks().get(stream, set())),
                    msg="The fields sent to the target don't include all automatic fields")
                
                if record_count_sync > page_size:
                    
                    primary_keys_list_1 = primary_keys_list[:page_size]
                    primary_keys_list_2 = primary_keys_list[page_size:2*page_size]

                    primary_keys_page_1 = set(primary_keys_list_1)
                    primary_keys_page_2 = set(primary_keys_list_2)

                    # Verify by private keys that data is unique for page
                    self.assertTrue(primary_keys_page_1.isdisjoint(primary_keys_page_2))
