from math import ceil

from tap_tester import connections, runner

from base import TestMixPanelBase


class MixPanelPaginationTest(TestMixPanelBase):

    @staticmethod
    def name():
        return "tap_tester_mixpanel_pagination_test"

    def pagination_test_run(self):
        """
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
        expected_streams = {"engage", "cohort_members"}

        conn_id = connections.ensure_connection(self)

        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Table and field selection
        test_catalogs_all_fields = [
            catalog
            for catalog in found_catalogs
            if catalog.get("tap_stream_id") in expected_streams
        ]

        self.perform_and_verify_table_and_field_selection(
            conn_id, test_catalogs_all_fields
        )

        record_count_by_stream = self.run_and_verify_sync(conn_id)

        synced_records = runner.get_records_from_target_output()

        # Verify no unexpected streams were replicated
        synced_stream_names = set(synced_records.keys())
        self.assertSetEqual(expected_streams, synced_stream_names)

        for stream in expected_streams:
            with self.subTest(stream=stream):

                # Expected values
                expected_primary_keys = self.expected_pks()[stream]

                # Collect actual values
                messages = synced_records.get(stream)
                primary_keys_list = [
                    tuple(
                        message["data"][expected_pk]
                        for expected_pk in expected_primary_keys
                    )
                    for message in messages["messages"]
                    if message["action"] == "upsert"
                ]

                # Verify that we can paginate with all fields selected
                record_count_sync = record_count_by_stream.get(stream, 0)
                self.assertGreater(
                    record_count_sync,
                    self.API_LIMIT,
                    msg="The number of records is not over the stream max limit",
                )

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
                                continue  # Don't compare the page to itself

                            self.assertTrue(
                                current_page.isdisjoint(other_page),
                                msg=f"other_page_primary_keys={other_page}",
                            )

    def test_run(self):
        # Pagination test for standard server
        self.eu_residency = False
        self.pagination_test_run()

        # Pagination test for EU residency server
        self.eu_residency = True
        self.pagination_test_run()
