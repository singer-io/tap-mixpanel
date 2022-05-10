from math import ceil

import tap_tester.connections as connections
import tap_tester.runner as runner
import tap_tester.menagerie as menagerie
from tap_tester.logger import LOGGER

from base import TestMixPanelBase


class MixPanelPaginationTest(TestMixPanelBase):
    def name(self):
        return "mix_panel_pagination_test"

    def pagination_test_run(self):
        """
        • Verify that for each stream you can get multiple pages of data
        • Verify no duplicate pages are replicated
        • Verify no unexpected streams were replicated

        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """

        # Only following below 2 streams support pagination
        streams_to_test = {'engage', 'cohort_members'}

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(
                fields_from_field_level_md)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test, synced_stream_names)

        for stream in streams_to_test:

            with self.subTest(stream=stream):

                # expected values
                expected_primary_keys = self.expected_pks()[stream]

                # collect actual values
                messages = synced_records.get(stream)
                actual_all_keys = [set(message['data'].keys()) for message in messages['messages']
                                   if message['action'] == 'upsert'][0]
                primary_keys_list = [tuple([message['data'][expected_pk] for expected_pk in expected_primary_keys])
                                     for message in messages['messages'] if message['action'] == 'upsert']

                # verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, 0)
                self.assertGreater(record_count_sync, self.API_LIMIT,
                                   msg="The number of records is not over the stream max limit")


                # Chunk the replicated records (just primary keys) into expected pages
                pages = []
                page_count = ceil(len(primary_keys_list) / self.API_LIMIT)
                page_size = self.API_LIMIT
                for page_index in range(page_count):
                    page_start = page_index * page_size
                    page_end = (page_index + 1) * page_size
                    pages.append(set(primary_keys_list[page_start:page_end]))

                # Verify by primary keys that data is unique for each page
                for current_index, current_page in enumerate(pages):
                    with self.subTest(current_page_primary_keys=current_page):

                        for other_index, other_page in enumerate(pages):
                            if current_index == other_index:
                                continue  # don't compare the page to itself

                            self.assertTrue(
                                current_page.isdisjoint(other_page), msg=f'other_page_primary_keys={other_page}'
                            )

    def test_run(self):
        # Pagination test for standard server
        self.eu_residency = False
        self.pagination_test_run()


        # Pagination test for EU recidency server
        self.eu_residency = True
        self.pagination_test_run()
