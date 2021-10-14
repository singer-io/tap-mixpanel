"""
This module defines the stream classes and their individual sync logic.
"""

import json
import math
from datetime import datetime, timedelta

import pytz
import singer
from singer import Transformer, metadata, metrics, utils
from singer.utils import strptime_to_utc

from tap_mixpanel.client import MixpanelClient
from tap_mixpanel.transform import transform_record, transform_datetime

LOGGER = singer.get_logger()


class MixPanel:
    """
    A base class representing singer streams.
    :param client: The API client used to extract records from external source
    """
    tap_stream_id = None
    replication_method = None
    replication_keys = []
    key_properties = []
    to_replicate = True
    date_dictionary = False
    path = None
    params = {}
    parent = None
    data_key = None
    bookmark_query_field_from = None
    bookmark_query_field_to = None
    pagination = False
    parent_path = None
    parent_id_field = None
    url = "https://mixpanel.com/api/2.0"

    def __init__(self, client: MixpanelClient):
        self.client = client

    def write_schema(self, catalog, stream_name):
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        try:
            singer.write_schema(stream_name, schema, stream.key_properties)
        except OSError as err:
            LOGGER.error("OS Error writing schema for: %s",stream_name)
            raise err

    def get_bookmark(self, state, stream, default):
        if (state is None) or ("bookmarks" not in state):
            return default
        return state.get("bookmarks", {}).get(stream, default)

    def write_bookmark(self, state, stream, value):
        if "bookmarks" not in state:
            state["bookmarks"] = {}
        state["bookmarks"][stream] = value
        LOGGER.info("Write state for stream: %s, value: %s", stream, value)
        singer.write_state(state)

    def process_records(
        self,  # pylint: disable=too-many-branches
        catalog,
        stream_name,
        records,
        time_extracted,
        bookmark_field=None,
        max_bookmark_value=None,
        last_datetime=None,
    ):
        stream = catalog.get_stream(stream_name)
        schema = stream.schema.to_dict()
        stream_metadata = metadata.to_map(stream.metadata)

        with metrics.record_counter(stream_name) as counter:
            for record in records:
                # Transform record for Singer.io
                with Transformer() as transformer:
                    try:
                        transformed_record = transformer.transform(
                            record, schema, stream_metadata
                        )
                    except Exception as err:
                        LOGGER.error("Error: %s",str(err))
                        LOGGER.error("For schema: %s",
                            json.dumps(schema, sort_keys=True, indent=2)
                        )
                        raise err

                    # Reset max_bookmark_value to new value if higher
                    if transformed_record.get(bookmark_field):
                        if max_bookmark_value is None or transformed_record[
                            bookmark_field
                        ] > transform_datetime(max_bookmark_value):
                            max_bookmark_value = transformed_record[bookmark_field]

                    if bookmark_field and (bookmark_field in transformed_record):
                        last_dttm = transform_datetime(last_datetime)
                        bookmark_dttm = transform_datetime(
                            transformed_record[bookmark_field]
                        )
                        # Keep only records whose bookmark is after the last_datetime
                        if bookmark_dttm >= last_dttm:
                            singer.write_record(
                                stream_name,
                                transformed_record,
                                time_extracted=time_extracted,
                            )
                            counter.increment()
                    else:
                        singer.write_record(
                            stream_name,
                            transformed_record,
                            time_extracted=time_extracted,
                        )
                        counter.increment()

            return max_bookmark_value, counter.value

    def get_and_transform_records(
            self, querystring, project_timezone, max_bookmark_value, catalog, last_datetime, endpoint_total,
            limit, total_records, parent_total, record_count, page, offset, parent_record, date_total):
        """
        Get the records using the client get request and transform it using transform_records
        and return the max_bookmark_value
        """

        session_id = None
        data = self.client.request(
            method='GET',
            url=self.url,
            path=self.path,
            params=querystring,
            endpoint=self.tap_stream_id)

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        full_url = '{}/{}{}'.format(
            self.url,
            self.path,
            '?{}'.format(querystring) if querystring else '')
        if not data:
            LOGGER.info('No data for URL: %s',full_url)
            # No data results
        else:  # has data
            # Transform data with transform_json from transform.py
            # The data_key identifies the array/list of records below the <root> element
            # LOGGER.info('data = {}'.format(data)) # TESTING, comment out
            transformed_data = []  # initialize the record list

            # Endpoints: funnels, revenue return results as dictionary for each date
            # Standardize results to a list/array
            if self.date_dictionary and self.data_key in data:
                results = {}
                results_list = []
                for key, val in data[self.data_key].items():
                    # skip $overall summary
                    if key != '$overall':
                        val['date'] = key
                        val['datetime'] = '{}T00:00:00Z'.format(key)
                        results_list.append(val)
                results[self.data_key] = results_list
                data = results

            # Cohorts endpoint returns results as a list/array (no data_key)
            # All other endpoints have a data_key
            if not self.data_key:
                self.data_key = 'results'
                new_data = {
                    'results': data
                }
                data = new_data

            transformed_data = []
            # Loop through result records
            for record in data[self.data_key]:
                # transform record and append to transformed_data array
                transformed_record = transform_record(record,
                                                      self.tap_stream_id,
                                                      project_timezone,
                                                      parent_record)
                transformed_data.append(transformed_record)

                # Check for missing keys
                for key in self.key_properties:
                    val = transformed_record.get(key)
                    if not val:
                        LOGGER.error('Error: Missing Key')
                        raise 'Missing Key'

                # End data record loop

            if not transformed_data:
                 LOGGER.info('No transformed data for data = %s', data)
                # No transformed data results
            else:  # has transformed data
                # Process records and get the max_bookmark_value and record_count
                max_bookmark_value, record_count = self.process_records(
                    catalog=catalog,
                    stream_name=self.tap_stream_id,
                    records=transformed_data,
                    time_extracted=time_extracted,
                    bookmark_field=next(iter(self.replication_keys), None),
                    max_bookmark_value=max_bookmark_value,
                    last_datetime=last_datetime)
                LOGGER.info('Stream %s, batch processed %s records', self.tap_stream_id, record_count)

                # set total_records and pagination fields
                if page == 0:
                    if isinstance(data, dict):
                        total_records = data.get('total', record_count)
                    else:
                        total_records = record_count
                parent_total = parent_total + record_count
                date_total = date_total + record_count
                endpoint_total = endpoint_total + record_count
                if isinstance(data, dict):
                    session_id = data.get('session_id', None)

                # to_rec: to record; ending record for the batch page
                if self.pagination:
                    to_rec = offset + limit
                    if to_rec > total_records:
                        to_rec = total_records
                else:
                    to_rec = record_count

                LOGGER.info('Synced Stream: %s, page: %s, %s to %s of total: %s',
                            self.tap_stream_id,
                            page,
                            offset,
                            to_rec,
                            total_records)
                # End has transformed data
            # End has data results

        # Pagination: increment the offset by the limit (batch-size) and page
        offset = offset + limit
        page = page + 1
        return parent_total, date_total, offset, page, session_id, endpoint_total, max_bookmark_value, total_records

    def define_bookmark_filters(self, days_interval, last_datetime, now_datetime, attribution_window):
        """
        define the params from and to according to the filters provided in
         the bookmark_query_field_from and bookmark_query_field_to
        """
        if self.bookmark_query_field_from and self.bookmark_query_field_to:
            # days_interval from config date_window_size, default = 60; passed to function from sync
            if not days_interval:
                days_interval = 30

            last_dttm = strptime_to_utc(last_datetime)
            delta_days = (now_datetime - last_dttm).days
            if delta_days <= attribution_window:
                delta_days = attribution_window
                LOGGER.info("Start bookmark less than %s day attribution window.", attribution_window)
            elif delta_days >= 365:
                delta_days = 365
                LOGGER.warning("Start date or bookmark greater than 1 year maxiumum.")
                LOGGER.warning("Setting bookmark start to 1 year ago.")

            start_window = now_datetime - timedelta(days=delta_days)
            end_window = start_window + timedelta(days=days_interval)
            if end_window > now_datetime:
                end_window = now_datetime
        else:
            start_window = strptime_to_utc(last_datetime)
            end_window = now_datetime
            diff_sec = (end_window - start_window).seconds
            # round-up difference to days
            days_interval = math.ceil(diff_sec / (3600 * 24))

        return start_window, end_window, days_interval

    def sync(self, state, catalog, config, start_date):
        # the sync method common to all the streams which internally call methods depending on different endpoints

        bookmark_field = next(iter(self.replication_keys), None)
        project_timezone = config.get("project_timezone", "UTC")
        days_interval = int(config.get("date_window_size", "30"))
        attribution_window = int(config.get("attribution_window", "5"))

        #Update url if eu_residency is selected
        if str(config.get('eu_residency')).lower() == "true":
            if self.tap_stream_id == 'export':
                self.url = 'https://data-eu.mixpanel.com/api/2.0'
            else:
                self.url = 'https://eu.mixpanel.com/api/2.0'

        # Get the latest bookmark for the stream and set the last_integer/datetime
        last_datetime = self.get_bookmark(
            state, self.tap_stream_id, start_date)
        max_bookmark_value = last_datetime

        self.write_schema(catalog, self.tap_stream_id)

        # windowing: loop through date days_interval date windows from last_datetime to now_datetime
        tzone = pytz.timezone(project_timezone)
        now_datetime = datetime.now(tzone)
        end_date = config.get('end_date')

        if end_date:
            now_datetime = strptime_to_utc(end_date)

        start_window, end_window, days_interval = self.define_bookmark_filters(
            days_interval, last_datetime, now_datetime, attribution_window)
        # LOOP order: Date Windows, Parent IDs, Page
        # Initialize counter
        endpoint_total = 0  # Total for ALL: parents, date windows, and pages

        # Begin date windowing loop
        while start_window < now_datetime:
            # Initialize counters
            date_total = 0  # Total records for a date window
            parent_total = 0  # Total records for parent ID
            total_records = 0  # Total records for all pages
            record_count = 0  # Total processed for page

            params = self.params  # adds in endpoint specific, sort, filter params

            if self.bookmark_query_field_from and self.bookmark_query_field_to:
                # Request dates need to be normalized to project timezone or else errors may occur
                # Errors occur when from_date is > 365 days ago
                #   and when to_date > today (in project timezone)
                from_date = str(start_window.astimezone(tzone).date())
                to_date = str(end_window.astimezone(tzone).date())
                LOGGER.info("START Sync for Stream: %s", self.tap_stream_id)
                if self.bookmark_query_field_from:
                    LOGGER.info("Date window from: %s to %s", from_date, to_date)
                params[self.bookmark_query_field_from] = from_date
                params[self.bookmark_query_field_to] = to_date

            # funnels and cohorts have a parent endpoint with parent_data and parent_id_field
            if self.parent_path and self.parent_id_field:
                # API request data
                LOGGER.info("URL for Parent Stream %s: %s/%s",
                            self.tap_stream_id,
                            self.url,
                            self.parent_path)
                parent_data = self.client.request(
                    method="GET",
                    url=self.url,
                    path=self.parent_path,
                    endpoint="parent_data",
                )
            # Other endpoints (not funnels, cohorts): Simulate parent_data with single record
            else:
                parent_data = [{"id": "none"}]
                self.parent_id_field = "id"

            for parent_record in parent_data:
                parent_id = parent_record.get(self.parent_id_field)
                LOGGER.info('START: Stream: %s, parent_id: %s', self.tap_stream_id, parent_id)

                # pagination: loop thru all pages of data using next (if not None)
                page = 0  # First page is page=0, second page is page=1, ...
                offset = 0
                limit = 250  # Default page_size
                # Initialize counters
                parent_total = 0  # Total records for parent ID
                total_records = 0  # Total records for all pages
                record_count = 0  # Total processed for page

                session_id = 'initial'
                if self.pagination:
                    params['page_size'] = limit

                # Popped session_id and page number of last parents stream call.
                params.pop('session_id', None)
                params.pop('page', None)

                while offset <= total_records and session_id is not None:
                    if self.pagination and page != 0:
                        params['session_id'] = session_id
                        params['page'] = page

                    # querystring: Squash query params into string and replace [parent_id]
                    querystring = '&'.join(['%s=%s' % (key, value) for (key, value) in params.items()])
                    querystring = querystring.replace('[parent_id]', str(parent_id))

                    full_url = '{}/{}{}'.format(
                        self.url,
                        self.path,
                        '?{}'.format(querystring) if querystring else '')

                    LOGGER.info('URL for Stream %s: %s', self.tap_stream_id, full_url)

                    # API request data
                    # data = {}

                    parent_total, date_total, offset, page, session_id, endpoint_total, max_bookmark_value, total_records = self.get_and_transform_records(
                        querystring, project_timezone, max_bookmark_value, catalog, last_datetime, endpoint_total, limit, total_records, parent_total, record_count, page, offset, parent_record, date_total)
                   # End stream != 'export'
                LOGGER.info('FINISHED: Stream: %s, parent_id: %s', self.tap_stream_id, parent_id)
                LOGGER.info('Total records for parent: %s', parent_total)
                # End parent record loop
            LOGGER.info("FINISHED Sync for Stream: %s",self.tap_stream_id)
            if self.bookmark_query_field_from:
                LOGGER.info("Date window from: %s to %s",from_date, to_date)
            LOGGER.info("Total records for date window: %s", date_total)
            # Increment date window
            start_window = end_window
            next_end_window = end_window + timedelta(days=days_interval)
            if next_end_window > now_datetime:
                end_window = now_datetime
            else:
                end_window = next_end_window

            # Update the state with the max_bookmark_value for the stream
            if bookmark_field:
                self.write_bookmark(state, self.tap_stream_id, max_bookmark_value)
            # End date window loop
        # Return endpoint_total across all batches
        return endpoint_total


class Annotations(MixPanel):
    """
    List the annotations for a given date range.
    Docs: https://developer.mixpanel.com/reference/annotations
    """
    tap_stream_id = "annotations"
    key_properties = ["date"]
    path = "annotations"
    data_key = "annotations"
    bookmark_query_field_from = "from_date"
    bookmark_query_field_to = "to_date"
    replication_method = "FULL_TABLE"
    params = {}
    replication_keys = []


class CohortMembers(MixPanel):
    """
    The list endpoint returns all of the cohorts in a given project.
    The JSON formatted return contains the cohort name, id, count, description, creation date, and visibility for every cohort in the project.
    Docs: https://developer.mixpanel.com/reference/engage
    """
    tap_stream_id = "cohort_members"
    path = "engage"
    key_properties = ["cohort_id", "distinct_id"]
    params = {"filter_by_cohort": '{"id": [parent_id]}'}
    data_key = "results"
    pagination = True
    parent_path = "cohorts/list"
    parent_id_field = "id"
    replication_keys = []
    replication_method = "FULL_TABLE"
    bookmark_query_field_from = None
    bookmark_query_field_to = None


class Cohorts(MixPanel):
    """
    Takes a JSON object with a single key called id whose value is the cohort ID. behaviors and filter_by_cohort are mutually exclusive.
    Docs: https://developer.mixpanel.com/reference/cohorts
    """
    tap_stream_id = "cohorts"
    path = "cohorts/list"
    key_properties = ["id"]
    data_key = None
    replication_method = "FULL_TABLE"
    params = {}
    replication_keys = []
    bookmark_query_field_from = None
    bookmark_query_field_to = None


class Engage(MixPanel):
    """
    Query user profile data and return list of users that fit specified parameters.
    Docs: https://developer.mixpanel.com/reference/engage
    """
    tap_stream_id = "engage"
    path = "engage"
    data_key = "results"
    pagination = True
    key_properties = ["distinct_id"]
    replication_method = "FULL_TABLE"
    bookmark_query_field_from = None
    bookmark_query_field_to = None
    params = {}
    replication_keys = []


class Export(MixPanel):
    """
    Every data point sent to Mixpanel is stored as JSON in our data store.
    The raw export API allows you to download your event data as it is received and stored within Mixpanel,
    complete with all event properties (including distinct_id) and the exact timestamp the event was fired.
    Docs: https://developer.mixpanel.com/reference/export
    """
    tap_stream_id = "export"
    path = "export"
    data_key = "results"
    replication_keys = ["time"]
    url = "https://data.mixpanel.com/api/2.0"
    bookmark_query_field_from = "from_date"
    bookmark_query_field_to = "to_date"
    replication_method = "INCREMENTAL"
    params = {}

    def get_and_transform_records(
            self, querystring, project_timezone, max_bookmark_value, catalog, last_datetime, endpoint_total,
            limit, total_records, parent_total, record_count, page, offset, parent_record, date_total):
        """
        Get the records using the client get request and transform it using transform_records
        and return the max_bookmark_value
        """
        data = self.client.request_export(
            method='GET',
            url=self.url,
            path=self.path,
            params=querystring,
            endpoint=self.tap_stream_id)

        # time_extracted: datetime when the data was extracted from the API
        time_extracted = utils.now()
        transformed_data = []
        for record in data:
            if record and str(record) != '':
                # transform record and append to transformed_data array
                transformed_record = transform_record(record,
                                                      self.tap_stream_id,
                                                      project_timezone)
                transformed_data.append(transformed_record)

                # Check for missing keys
                for key in self.key_properties:
                    val = transformed_record.get(key)
                    if not val:
                        LOGGER.error('Error: Missing Key')
                        raise 'Missing Key'

                if len(transformed_data) == limit:
                    # Process full batch (limit = 250) records
                    #   and get the max_bookmark_value and record_count
                    max_bookmark_value, record_count = self.process_records(
                        catalog=catalog,
                        stream_name=self.tap_stream_id,
                        records=transformed_data,
                        time_extracted=time_extracted,
                        bookmark_field=next(iter(self.replication_keys), None),
                        max_bookmark_value=max_bookmark_value,
                        last_datetime=last_datetime)
                    LOGGER.info('Stream %s, batch processed %s records', self.tap_stream_id, record_count)

                    total_records = total_records + record_count
                    parent_total = parent_total + record_count
                    date_total = date_total + record_count
                    endpoint_total = endpoint_total + record_count
                    transformed_data = []
                    # End if (batch = limit 250)
                # End if record
            # End has export_data records loop

        # Process remaining, partial batch
        if len(transformed_data) > 0:
            max_bookmark_value, record_count = self.process_records(
                catalog=catalog,
                stream_name=self.tap_stream_id,
                records=transformed_data,
                time_extracted=time_extracted,
                bookmark_field=next(iter(self.replication_keys), None),
                max_bookmark_value=max_bookmark_value,
                last_datetime=last_datetime)
            LOGGER.info('Stream %s, batch processed %s records', self.tap_stream_id, record_count)

            total_records = total_records + record_count
            parent_total = parent_total + record_count
            date_total = date_total + record_count
            endpoint_total = endpoint_total + record_count
            # End if transformed_data

        # Export does not provide pagination; session_id = None breaks out of loop.
        session_id = None
        return parent_total, date_total, offset, page, session_id, endpoint_total, max_bookmark_value, total_records


class Funnels(MixPanel):
    """
    Get data for a funnel. funnel_id as a parameter to the API to get the funnel that you wish to get data for.
    Docs: https://developer.mixpanel.com/reference/funnels
    """
    tap_stream_id = "funnels"
    path = "funnels"
    key_properties = ["funnel_id", "date"]
    data_key = "data"
    date_dictionary = True
    bookmark_query_field_from = "from_date"
    bookmark_query_field_to = "to_date"
    replication_keys = ["datetime"]
    params = {"funnel_id": "[parent_id]", "unit": "day"}
    parent_path = "funnels/list"
    parent_id_field = "funnel_id"
    replication_method = "INCREMENTAL"


class Revenue(MixPanel):
    """
    Get the revenue data.
    """
    tap_stream_id = "revenue"
    path = "engage/revenue"
    key_properties = ["date"]
    data_key = "results"
    date_dictionary = True
    bookmark_query_field_from = "from_date"
    bookmark_query_field_to = "to_date"
    replication_keys = ["datetime"]
    params = {"unit": "day"}
    replication_method = "INCREMENTAL"


STREAMS = {
    "export": Export,
    "engage": Engage,
    "funnels": Funnels,
    "cohorts": Cohorts,
    "cohort_members": CohortMembers,
    "revenue": Revenue,
    "annotations": Annotations,
}
