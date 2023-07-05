from math import ceil

from tap_tester import connections, menagerie, runner

from base import TestMixPanelBase


class MixPanelPaginationAllFieldsTest(TestMixPanelBase):

    @staticmethod
    def name():
        return "tap_tester_mixpanel_pagination_all_fields_test"

    def pagination_test_run(self):
        """
        All Fields Test
        • Verify that when all fields are selected more than the automatic fields are replicated.
        • Verify no unexpected streams were replicated
        • Verify that more than just the automatic fields are replicated for each stream.
        • Verify all fields for each stream are replicated
        • Verify that the automatic fields are sent to the target
        Pagination Test
        • Verify that for each stream you can get multiple pages of data
        • Verify no duplicate pages are replicated
        • Verify no unexpected streams were replicated
        PREREQUISITE
        For EACH stream add enough data that you surpass the limit of a single
        fetch of data.  For instance if you have a limit of 250 records ensure
        that 251 (or more) records have been posted for that stream.
        """

        # Only following below 2 streams support pagination
        streams_to_test_all_fields = self.expected_streams()
        streams_to_test_pagination = {'engage', 'cohort_members'}

        expected_automatic_fields = self.expected_automatic_fields()
        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [catalog for catalog in found_catalogs
                                    if catalog.get('tap_stream_id') in streams_to_test_all_fields]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields)

        # Grab metadata after performing table-and-field selection to set expectations
        # used for asserting all fields are replicated
        stream_to_all_catalog_fields = dict()
        for catalog in test_catalogs_all_fields:
            stream_id, stream_name = catalog['stream_id'], catalog['stream_name']
            catalog_entry = menagerie.get_annotated_schema(conn_id, stream_id)
            fields_from_field_level_md = [md_entry['breadcrumb'][1]
                                          for md_entry in catalog_entry['metadata']
                                          if md_entry['breadcrumb'] != []]
            stream_to_all_catalog_fields[stream_name] = set(fields_from_field_level_md)

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        actual_fields_by_stream = runner.examine_target_output_for_fields()

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(streams_to_test_all_fields, synced_stream_names)

        # All Fields Test
        for stream in streams_to_test_all_fields:
            with self.subTest(logging="Primary Functional Test", stream=stream):

                # Expected values
                expected_all_keys = stream_to_all_catalog_fields[stream]
                expected_automatic_keys = expected_automatic_fields.get(stream, set())

                # Collect actual values
                messages = synced_records.get(stream)
                actual_all_keys = set()
                for message in messages['messages']:
                    if message['action'] == 'upsert':
                        actual_all_keys.update(set(message['data'].keys()))

                # Verify that the automatic fields are sent to the target
                self.assertTrue(
                    actual_fields_by_stream.get(stream, set()).issuperset(
                        expected_automatic_keys),
                    msg="The fields sent to the target don't include all automatic fields")

                # Verify that more than just the automatic fields are replicated for each stream.
                if stream != "cohort_members":  # cohort_member has just 2 key and both are automatic
                    self.assertGreater(len(expected_all_keys),
                                       len(expected_automatic_keys))

                self.assertTrue(expected_automatic_keys.issubset(
                    expected_all_keys), msg=f'{expected_automatic_keys-expected_all_keys} is not in "expected_all_keys"')

                # As we can't find the below fields in the docs and also
                # it won't be generated by mixpanel APIs now so expected.
                if stream == "export":
                    expected_all_keys = expected_all_keys - {'labels', 'sampling_factor', 'dataset', 'mp_reserved_duration_s', 'mp_reserved_origin_end',
                                                             'mp_reserved_origin_start', 'mp_reserved_event_count', 'mp_reserved_event_name'}

                # Verify all fields for each stream are replicated
                # Skip engage as it return records in random manner with dynamic fields.
                if not stream == "engage":
                    self.assertSetEqual(expected_all_keys, actual_all_keys)

        # Pagination Test
        for stream in streams_to_test_pagination:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_pks()[stream]

                # Collect actual values
                messages = synced_records.get(stream)
                primary_keys_list = [tuple([message['data'][expected_pk] for expected_pk in expected_primary_keys])
                                     for message in messages['messages'] if message['action'] == 'upsert']

                # Verify that we can paginate with all fields selected
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

        # Pagination test for EU residency server
        self.eu_residency = True
        self.pagination_test_run()

    def test_run_ssa(self):
        # perform checks with service account auth
        self.service_account_authentication = True
        self.pagination_test_run()