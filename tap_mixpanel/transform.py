import datetime
import pytz
import singer
from singer.utils import strftime


LOGGER = singer.get_logger()

# De-nest properties for engage and export endpoints
def denest_properties(this_json, properties_node):
    new_json = []
    for record in this_json:
        if properties_node in record:
            for key, val in record[properties_node].items():
                if key[0:1] == '$':
                    new_key = key.replace('$', 'mp_reserved_')
                else:
                    new_key = key
                record[new_key] = val
            record.pop(properties_node, None)
            new_json.append(record)
    return new_json


# Time conversion from $time integer using project_timezone
# Reference: https://help.mixpanel.com/hc/en-us/articles/115004547203-Manage-Timezones-for-Projects-in-Mixpanel#exporting-data-from-mixpanel
def transform_event_times(this_json, project_timezone):
    new_json = this_json
    timezone = pytz.timezone(project_timezone)
    i = 0
    for record in this_json:
        # Get integer time
        time_int = int(record.get('time'))

        # Create beginning_datetime: beginning of epoch time in project timezone
        naive_time = datetime.time(0, 0)
        date = datetime.date(1970, 1, 1)
        naive_datetime = datetime.datetime.combine(date, naive_time)
        beginning_datetime = timezone.localize(naive_datetime)

        # Create new_time_utc by adding seconds to beginning_datetime, normalizing,
        #   and converting to string
        add_seconds = datetime.timedelta(seconds=time_int)
        new_time = beginning_datetime + add_seconds
        # 'normalize' accounts for daylight savings time
        new_time_utc_str = strftime(timezone.normalize(new_time).astimezone(pytz.utc))

        new_json[i]['time'] = new_time_utc_str

        i = i + 1

    return new_json


# Remove leading $ from engage $distinct_id
def transform_engage(this_json):
    new_json = this_json
    i = 0
    for record in this_json:
        distinct_id = record.get('$distinct_id')
        new_json[i]['distinct_id'] = distinct_id
        new_json[i].pop('$distinct_id', None)
        i = i + 1
    return new_json


# Funnels: combine parent record with each date record
def transform_funnels(this_json, parent_record):
    new_json = this_json
    i = 0
    for record in this_json:
        # Include parent record nodes in each record
        new_json[i].update(parent_record)
        i = i + 1
    return new_json


# Cohort Members: provide all distinct_id's for each cohort_id
def transform_cohort_members(this_json, parent_record):
    results = []
    cohort_id = parent_record.get('id')
    for record in this_json:
        result = {}
        # Get distinct_id from each record
        distinct_id = record.get('$distinct_id')
        result['distinct_id'] = distinct_id
        result['cohort_id'] = cohort_id
        results.append(result)
    return results


# Run other transforms, as needed: denest_list_nodes, transform_conversation_parts
def transform_json(this_json, stream_name, project_timezone, parent_record=None):
    new_json = this_json
    if stream_name == 'engage':
        trans_json = transform_engage(this_json)
        new_json = denest_properties(trans_json, '$properties')
    elif stream_name == 'export':
        denested_json = denest_properties(this_json, 'properties')
        new_json = transform_event_times(denested_json, project_timezone)
    elif stream_name == 'funnels':
        new_json = transform_funnels(this_json, parent_record)
    elif stream_name == 'cohort_members':
        new_json = transform_cohort_members(this_json, parent_record)
    else:
        new_json = this_json

    return new_json
