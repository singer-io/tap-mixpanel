from singer.catalog import Catalog, CatalogEntry, Schema
from tap_mixpanel.schema import get_schemas
from tap_mixpanel.streams import STREAMS

def discover(client, properties_flag):
    schemas, field_metadata = get_schemas(client, properties_flag)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(CatalogEntry(
            stream=stream_name,
            tap_stream_id=stream_name,
            key_properties=STREAMS[stream_name].key_properties,
            schema=schema,
            metadata=mdata
        ))

    return catalog
