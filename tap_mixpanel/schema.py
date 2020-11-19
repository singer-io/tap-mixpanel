import os
import json
from singer import metadata
from tap_mixpanel.streams import STREAMS
import singer

LOGGER = singer.get_logger()

# Reference:
# https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#Metadata

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schema(client, properties_flag, stream_name):
    schema_path = get_abs_path('schemas/{}.json'.format(stream_name))

    with open(schema_path) as file:
        schema = json.load(file)

    # Set whether to allow additional properties for engage and export endpoints
    # Event and Engage properties are dynamic and depend on the properties provided on upload,
    #   when the Event or Engage (user/person) was created.
    # Depending on the tap config parameter select_properties_by_default,
    #   the json schema should allow additional properties (additionalProperties = true).
    if stream_name in ('engage', 'export') and str(properties_flag).lower() == 'true':
        schema['additionalProperties'] = True
    else:
        schema['additionalProperties'] = False

    if stream_name == 'engage':
        properties = client.request(
            method='GET',
            url='https://mixpanel.com/api/2.0',
            path='engage/properties',
            params={'limit': 2000},
            endpoint='engage_properties')
        if properties.get('status') == 'ok':
            results = properties.get('results', {})
            for key, val in results.items():
                if key[0:1] == '$':
                    new_key = 'mp_reserved_{}'.format(key[1:])
                else:
                    new_key = key

                # property_type: string, number, boolean, datetime, object, list
                # Reference:
                # https://help.mixpanel.com/hc/en-us/articles/115004547063-Properties-Supported-Data-Types
                property_type = val.get('type')

                types = {
                    'boolean': {
                        'type': ['null', 'boolean']
                    },
                    'number': {
                        'type': ['null', 'number'],
                        'multipleOf': 1e-20
                    },
                    'datetime': {
                        'type': ['null', 'string'],
                        'format': 'date-time'
                    },
                    'object': {
                        'type': ['null', 'object'],
                        'additionalProperties': True
                    },
                    'list': {
                        'type': ['null', 'array'],
                        'required': False,
                        'items': {}
                    },
                    'string': {
                        'type': ['null', 'string']
                    }
                }

                if property_type in types.keys():
                    # Make the types a list containing all types starting with the one returned to us by the API
                    this_type = [types.pop(property_type)]
                    this_type += list(types.values())

                else:
                    this_type = list(types.values())


                schema['properties'][new_key] = {'anyOf': this_type}

    if stream_name == 'export':
        # Event properties endpoint:
        #  https://developer.mixpanel.com/docs/data-export-api#section-hr-span-style-font-family-courier-top-span
        results = client.request(
            method='GET',
            url='https://mixpanel.com/api/2.0',
            path='events/properties/top',
            params={'limit': 2000},
            endpoint='event_properties')
        for key, val in results.items():
            if key[0:1] == '$':
                new_key = 'mp_reserved_{}'.format(key[1:])
            else:
                new_key = key

            # string ONLY for event properties (no other datatypes)
            # Reference: https://help.mixpanel.com/hc/en-us/articles/360001355266-Event-Properties#field-size-character-limits-for-event-properties
            schema['properties'][new_key] = {
                'type': ['null', 'string']
            }

    return schema

def get_schemas(client, properties_flag):
    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():
        # When the client detects disable_engage_endpoint, skip discovering the stream
        if stream_name == 'engage' and client.disable_engage_endpoint:
            LOGGER.warning('Mixpanel returned a 402 indicating the Engage endpoint and stream is unavailable. Skipping.')
            continue

        schema = get_schema(client, properties_flag, stream_name)

        schemas[stream_name] = schema
        mdata = metadata.new()

        # Documentation:
        # https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#singer-python-helper-functions
        # Reference:
        # https://github.com/singer-io/singer-python/blob/master/singer/metadata.py#L25-L44
        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.get('key_properties', None),
            valid_replication_keys=stream_metadata.get('replication_keys', None),
            replication_method=stream_metadata.get('replication_method', None)
        )
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
