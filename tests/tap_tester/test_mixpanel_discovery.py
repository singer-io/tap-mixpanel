import re
from tap_tester import menagerie, connections, LOGGER

from base import TestMixPanelBase

class MixPanelDiscoverTest(TestMixPanelBase):
    """
        Testing that discovery creates the appropriate catalog with valid metadata.
        • Verify number of actual streams discovered match expected
        • Verify the stream names discovered were what we expect
        • Verify stream names follow naming convention
          streams should only have lowercase alphas and underscores
        • Verify there is only 1 top level breadcrumb
        • Verify replication key(s)
        • Verify primary key(s)
        • Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
        • Verify the actual replication matches our expected replication method
        • Verify that primary, replication and foreign keys
          are given the inclusion of automatic.
        • Verify that all other fields have inclusion of available metadata.
    """

    @staticmethod
    def name():
        return "tap_tester_mixpanel_discover_test"

    def discovery_test_run(self):

        region = "EU" if self.eu_residency else "Standard"
        LOGGER.info(f"Testing against {region} account.")

        self.assertion_logging_enabled = True

        streams_to_test = self.expected_streams()

        conn_id = connections.ensure_connection(self, payload_hook=None)

        # Verify that there are catalogs found
        found_catalogs = self.run_and_verify_check_mode(conn_id)

        # Verify stream names follow naming convention
        # streams should only have lowercase alphas and underscores
        found_catalog_names = {c["tap_stream_id"] for c in found_catalogs}
        self.assertTrue(
            all([re.fullmatch(r"[a-z_]+", name) for name in found_catalog_names]),
            logging="asserting all streams defined in catalog follow the naming convention '[a-z_]+'",
        )

        for stream in streams_to_test:
            with self.subTest(stream=stream):

                # Verify the catalog is found for a given stream
                catalog = next(
                    iter(
                        [
                            catalog
                            for catalog in found_catalogs
                            if catalog["stream_name"] == stream
                        ]
                    )
                )
                self.assertIsNotNone(
                    catalog, logging="Asserting entry is present in catalog"
                )

                # Collecting expected values
                expected_primary_keys = self.expected_pks()[stream]
                expected_replication_keys = self.expected_replication_keys()[stream]
                expected_automatic_fields = self.expected_automatic_fields().get(stream)
                expected_replication_method = self.expected_replication_method()[stream]

                # Collecting actual values...
                schema_and_metadata = menagerie.get_annotated_schema(
                    conn_id, catalog["stream_id"]
                )
                metadata = schema_and_metadata["metadata"]
                stream_properties = [
                    item for item in metadata if item.get("breadcrumb") == []
                ]
                actual_primary_keys = set(
                    stream_properties[0]
                    .get("metadata", {self.PRIMARY_KEYS: []})
                    .get(self.PRIMARY_KEYS, [])
                )
                actual_replication_keys = set(
                    stream_properties[0]
                    .get("metadata", {self.REPLICATION_KEYS: []})
                    .get(self.REPLICATION_KEYS, [])
                )
                actual_replication_method = (
                    stream_properties[0]
                    .get("metadata", {self.REPLICATION_METHOD: None})
                    .get(self.REPLICATION_METHOD)
                )
                actual_automatic_fields = {
                    item.get("breadcrumb", ["properties", None])[1]
                    for item in metadata
                    if item.get("metadata").get("inclusion") == "automatic"
                }

                actual_fields = []
                for md_entry in metadata:
                    if md_entry["breadcrumb"] != []:
                        actual_fields.append(md_entry["breadcrumb"][1])

                ##########################################################################
                # Metadata assertions
                ##########################################################################

                # Verify there is only 1 top level breadcrumb in metadata
                self.assertEqual(
                    len(stream_properties),
                    1,
                    logging="Asserting there is only 1 top level breadcrumb in metadata",
                )

                # Verify there is no duplicate metadata entries
                self.assertEqual(
                    len(actual_fields),
                    len(set(actual_fields)),
                    msg="Duplicates in the fields retrieved",
                )

                # Verify that if there is a replication key we are doing INCREMENTAL otherwise FULL
                if actual_replication_keys:
                    self.assertEqual(
                        actual_replication_method,
                        self.INCREMENTAL,
                        logging=f"Asserting replication method is {self.INCREMENTAL} when replication keys are defined",
                    )
                else:
                    self.assertEqual(
                        actual_replication_method,
                        self.FULL_TABLE,
                        logging=f"Asserting replication method is {self.FULL_TABLE} when replication keys are not defined",
                    )

                # Verify the actual replication matches our expected replication method
                self.assertEqual(
                    expected_replication_method,
                    actual_replication_method,
                    logging=f"Asserting replication method is {expected_replication_method}",
                )

                # Verify replication key(s)
                self.assertEqual(
                    expected_replication_keys,
                    actual_replication_keys,
                    logging=f"asserting replication keys are {expected_replication_keys}",
                )

                # Verify primary key(s) match expectations
                self.assertSetEqual(
                    expected_primary_keys,
                    actual_primary_keys,
                    logging=f"asserting primary keys are {expected_primary_keys}",
                )

                # Verify that primary keys and replication keys
                # are given the inclusion of automatic in metadata.
                self.assertSetEqual(
                    expected_automatic_fields,
                    actual_automatic_fields,
                    logging=f"asserting primary and replication keys {expected_automatic_fields} are automatic",
                )

                # Verify that all other fields have inclusion of available.
                # This assumes there are no unsupported fields for SaaS sources
                self.assertTrue(
                    all(
                        {
                            item.get("metadata").get("inclusion") == "available"
                         for item in metadata
                         if item.get("breadcrumb", []) != []
                         and item.get("breadcrumb", ["properties", None])[1]
                            not in actual_automatic_fields
                        }
                    ),
                    logging="Asserting non-key-property fields are available for field selection",
                )

    def test_standard_discovery(self):
        """Discovery test for standard server."""
        self.eu_residency = False
        self.discovery_test_run()

    def test_eu_discovery(self):
        """Discovery test for EU residency server."""
        self.eu_residency = True
        self.discovery_test_run()
