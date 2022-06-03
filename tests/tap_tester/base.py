"""
Test tap combined
"""

import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz

from tap_tester import connections
from tap_tester import runner
from tap_tester import menagerie
from tap_tester.logger import LOGGER
from tap_tester.base_case import BaseCase


class TestMixPanelBase(BaseCase):
    """ Test the tap combined """
    START_DATE_FORMAT = "%Y-%m-%dT00:00:00Z"
    REPLICATION_KEYS = "valid-replication-keys"
    PRIMARY_KEYS = "table-key-properties"
    FOREIGN_KEYS = "table-foreign-key-properties"
    REPLICATION_METHOD = "forced-replication-method"
    INCREMENTAL = "INCREMENTAL"
    FULL_TABLE = "FULL_TABLE"
    API_LIMIT = 250
    TYPE = "platform.mixpanel"
    start_date = ""
    end_date = ""
    eu_residency = True

    def tap_name(self):
        """The name of the tap"""
        return "tap-mixpanel"

    def expected_metadata(self):
        """The expected streams and metadata about the streams"""
        return {
            'export': {
                self.PRIMARY_KEYS: set(),
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'time'},
            },
            'engage': {
                self.PRIMARY_KEYS: {"distinct_id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            'funnels': {
                self.PRIMARY_KEYS: {'funnel_id', 'date'},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'datetime'},
            },
            'cohorts': {
                self.PRIMARY_KEYS: {"id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            'cohort_members': {
                self.PRIMARY_KEYS: {"cohort_id", "distinct_id"},
                self.REPLICATION_METHOD: self.FULL_TABLE,
            },
            'revenue': {
                self.PRIMARY_KEYS: {"date"},
                self.REPLICATION_METHOD: self.INCREMENTAL,
                self.REPLICATION_KEYS: {'datetime'},
            },

            'annotations': {
                self.PRIMARY_KEYS: {"date"},
                self.REPLICATION_METHOD: self.FULL_TABLE
            }
        }

    def setUp(self):
        missing_envs = []
        if self.eu_residency:
            creds = {"api_secret": "TAP_MIXPANEL_EU_RESIDENCY_API_SECRET"}
        else:
            creds = {"api_secret": "TAP_MIXPANEL_API_SECRET"}

        for cred in creds:
            if os.getenv(creds[cred]) == None:
                missing_envs.append(creds[cred])

        if len(missing_envs) != 0:
            raise Exception("set " + ", ".join(missing_envs))

        BaseCase.setUp(self)

    def get_type(self):
        """the expected url route ending"""
        return "platform.mixpanel"

    def get_properties(self, original: bool = True):
        """Configuration properties required for the tap."""

        return_value = {
            'start_date': '2020-02-01T00:00:00Z',
            'end_date': '2020-03-01T00:00:00Z',
            'date_window_size': '30',
            'attribution_window': '5',
            'project_timezone': 'US/Pacific',
            "eu_residency": 'false',
            'select_properties_by_default': 'false'
        }
        if self.eu_residency:
            return_value.update({"project_timezone": "Europe/Amsterdam", "eu_residency": 'true'})

        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

    def get_start_date(self):
        return dt.strftime(dt.utcnow() - timedelta(days=30), self.START_DATE_FORMAT)

    def get_credentials(self):
        """Authentication information for the test account. Api secret is expected as a property."""

        credentials_dict = {}
        if self.eu_residency:
            creds = {"api_secret": "TAP_MIXPANEL_EU_RESIDENCY_API_SECRET"}
        else:
            creds = {"api_secret": "TAP_MIXPANEL_API_SECRET"}

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def expected_streams(self):
        """A set of expected stream names"""

        # Skip `export` and `revenue` stream for EU recidency server as
        # revenue stream endpoint returns 400 bad reuqest and
        # export stream endpoint returns 200 terminated early response.
        # So, as per discussion decided that let the customer come with the issues
        # that these streams are not working. Skip the streams in the circleci.
        if self.eu_residency:
            return set(self.expected_metadata().keys()) - {"export", "revenue"}

        return set(self.expected_metadata().keys())

    def expected_pks(self):
        """return a dictionary with key of table name and value as a set of primary key fields"""
        return {table: properties.get(self.PRIMARY_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_keys(self):
        """return a dictionary with key of table name and value as a set of replication key fields"""
        return {table: properties.get(self.REPLICATION_KEYS, set())
                for table, properties
                in self.expected_metadata().items()}

    def expected_replication_method(self):
        """return a dictionary with key of table name nd value of replication method"""
        return {table: properties.get(self.REPLICATION_METHOD, None)
                for table, properties
                in self.expected_metadata().items()}

    def expected_automatic_fields(self):
        """return a dictionary with key of table name and value as a set of automatic key fields"""
        auto_fields = {}
        for k, v in self.expected_metadata().items():
            auto_fields[k] = v.get(self.PRIMARY_KEYS, set()) | v.get(self.REPLICATION_KEYS, set()) \
                | v.get(self.FOREIGN_KEYS, set())
        return auto_fields

    #########################
    #   Helper Methods      #
    #########################

    def run_and_verify_check_mode(self, conn_id):
        """
        Run the tap in check mode and verify it succeeds.
        This should be ran prior to field selection and initial sync.
        Return the connection id and found catalogs from menagerie.
        """
        # run in check mode
        check_job_name = runner.run_check_mode(self, conn_id)

        # verify check exit codes
        exit_status = menagerie.get_exit_status(conn_id, check_job_name)
        menagerie.verify_check_exit_status(self, exit_status, check_job_name)

        found_catalogs = menagerie.get_catalogs(conn_id)
        self.assertGreater(len(
            found_catalogs), 0, msg="unable to locate schemas for connection {}".format(conn_id))

        found_catalog_names = set(
            map(lambda c: c['stream_name'], found_catalogs))

        subset = self.expected_streams().issubset(found_catalog_names)
        self.assertTrue(
            subset, msg="Expected check streams are not subset of discovered catalog")
        LOGGER.info("discovered schemas are OK")

        return found_catalogs

    def run_and_verify_sync(self, conn_id):
        """
        Run a sync job and make sure it exited properly.
        Return a dictionary with keys of streams synced
        and values of records synced for each stream
        """

        # Run a sync job using orchestrator
        sync_job_name = runner.run_sync_mode(self, conn_id)

        # Verify tap and target exit codes
        exit_status = menagerie.get_exit_status(conn_id, sync_job_name)
        menagerie.verify_sync_exit_status(self, exit_status, sync_job_name)

        # Verify actual rows were synced
        sync_record_count = runner.examine_target_output_file(
            self, conn_id, self.expected_streams(), self.expected_pks())
        self.assertGreater(
            sum(sync_record_count.values()), 0,
            msg="failed to replicate any data: {}".format(sync_record_count)
        )
        LOGGER.info(f"total replicated row count: {sum(sync_record_count.values())}")

        return sync_record_count

    def perform_and_verify_table_and_field_selection(self, conn_id, test_catalogs, select_all_fields=True):
        """
        Perform table and field selection based off of the streams to select
        set and field selection parameters.
        Verify this results in the expected streams selected and all or no
        fields selected for those streams.
        """

        # Select all available fields or select no fields from all testable streams
        self.select_all_streams_and_fields(
            conn_id, test_catalogs, select_all_fields)

        catalogs = menagerie.get_catalogs(conn_id)

        # Ensure our selection affects the catalog
        expected_selected = [tc.get('stream_name') for tc in test_catalogs]

        for cat in catalogs:
            catalog_entry = menagerie.get_annotated_schema(
                conn_id, cat['stream_id'])

            # Verify all testable streams are selected
            selected = catalog_entry.get('annotated-schema').get('selected')
            LOGGER.info(f"Validating selection on {cat['stream_name']}: {selected}")

            if cat['stream_name'] not in expected_selected:
                self.assertFalse(
                    selected, msg="Stream selected, but not testable.")
                continue  # Skip remaining assertions if we aren't selecting this stream
            self.assertTrue(selected, msg="Stream not selected.")

            if select_all_fields:
                # Verify all fields within each selected stream are selected
                for field, field_props in catalog_entry.get('annotated-schema').get('properties').items():
                    field_selected = field_props.get('selected')
                    LOGGER.info(f"\tValidating selection on {cat['stream_name']}.{field}: {field_selected}")

                    self.assertTrue(field_selected, msg="Field not selected.")
            else:
                # Verify only automatic fields are selected
                expected_automatic_fields = self.expected_automatic_fields().get(
                    cat['stream_name'])
                selected_fields = self.get_selected_fields_from_metadata(
                    catalog_entry['metadata'])
                self.assertEqual(expected_automatic_fields, selected_fields)

    def get_selected_fields_from_metadata(self, metadata):
        selected_fields = set()
        for field in metadata:
            is_field_metadata = len(field['breadcrumb']) > 1
            inclusion_automatic_or_selected = (
                field['metadata']['selected'] is True or
                field['metadata']['inclusion'] == 'automatic'
            )
            if is_field_metadata and inclusion_automatic_or_selected:
                selected_fields.add(field['breadcrumb'][1])
        return selected_fields

    def select_all_streams_and_fields(self, conn_id, catalogs, select_all_fields: bool = True):
        """Select all streams and all fields within streams"""
        for catalog in catalogs:
            schema = menagerie.get_annotated_schema(
                conn_id, catalog['stream_id'])

            non_selected_properties = []
            if not select_all_fields:
                # get a list of all properties so that none are selected
                non_selected_properties = schema.get('annotated-schema', {}).get(
                    'properties', {}).keys()

            connections.select_catalog_and_fields_via_metadata(
                conn_id, catalog, schema, [], non_selected_properties)

    def parse_date(self, date_value):
        """
        Pass in string-formatted-datetime, parse the value, and return it as an unformatted datetime object.
        """
        date_formats = {
            "%Y-%m-%dT%H:%M:%S.%fZ",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S.%f+00:00",
            "%Y-%m-%dT%H:%M:%S+00:00",
            "%Y-%m-%d"
        }
        for date_format in date_formats:
            try:
                date_stripped = dt.strptime(date_value, date_format)
                return date_stripped
            except ValueError:
                continue

        raise NotImplementedError(
            "Tests do not account for dates of this format: {}".format(date_value))

    def calculated_states_by_stream(self, current_state):
        timedelta_by_stream = {stream: [1,0,0]  # {stream_name: [days, hours, minutes], ...}
                               for stream in self.expected_streams()}

        stream_to_calculated_state = {stream: "" for stream in current_state['bookmarks'].keys()}
        for stream, state in current_state['bookmarks'].items():

            state_as_datetime = dateutil.parser.parse(state)

            days, hours, minutes = timedelta_by_stream[stream]
            calculated_state_as_datetime = state_as_datetime - timedelta(days=days, hours=hours, minutes=minutes)

            state_format = '%Y-%m-%dT%H:%M:%S-00:00'
            calculated_state_formatted = dt.strftime(calculated_state_as_datetime, state_format)

            stream_to_calculated_state[stream] = calculated_state_formatted

        return stream_to_calculated_state

    ##########################################################################
    # Tap Specific Methods
    ##########################################################################

    def convert_state_to_utc(self, date_str):
        """
        Convert a saved bookmark value of the form '2020-08-25T13:17:36-07:00' to
        a string formatted utc datetime,
        in order to compare aginast json formatted datetime values
        """
        date_object = dateutil.parser.parse(date_str)
        date_object_utc = date_object.astimezone(tz=pytz.UTC)
        return dt.strftime(date_object_utc, "%Y-%m-%dT%H:%M:%SZ")

    def timedelta_formatted(self, dtime, days=0):
        try:
            date_stripped = dt.strptime(dtime, self.START_DATE_FORMAT)
            return_date = date_stripped + timedelta(days=days)

            return dt.strftime(return_date, self.START_DATE_FORMAT)

        except ValueError:
            try:
                date_stripped = dt.strptime(
                    dtime, self.BOOKMARK_COMPARISON_FORMAT)
                return_date = date_stripped + timedelta(days=days)

                return dt.strftime(return_date, self.BOOKMARK_COMPARISON_FORMAT)

            except ValueError:
                return Exception("Datetime object is not of the format: {}".format(self.START_DATE_FORMAT))

    def is_incremental(self, stream):
        return self.expected_metadata().get(stream).get(self.REPLICATION_METHOD) == self.INCREMENTAL
