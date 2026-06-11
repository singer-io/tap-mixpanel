import singer
from singer.catalog import Catalog, CatalogEntry, Schema

from tap_mixpanel.client import MixpanelForbiddenError
from tap_mixpanel.schema import get_schemas
from tap_mixpanel.streams import STREAMS

LOGGER = singer.get_logger()


def _prune_inaccessible_children(schemas, field_metadata):
    """Remove child streams from the catalog whose parent stream was excluded.

    Mutates schemas and field_metadata in place.
    """
    for name, stream_cls in list(STREAMS.items()):
        if name in schemas and stream_cls.parent and stream_cls.parent not in schemas:
            LOGGER.warning(
                "Stream '%s' excluded from catalog because its parent stream '%s' is not accessible.",
                name,
                stream_cls.parent,
            )
            schemas.pop(name, None)
            field_metadata.pop(name, None)


def _apply_access_checks(client, schemas, field_metadata):
    """Probe each parent stream for read access and remove inaccessible streams
    (and their children) from schemas and field_metadata in place.

    Raises MixpanelForbiddenError if no parent streams are accessible.
    """
    inaccessible_streams = [
        stream_name
        for stream_name, stream_cls in STREAMS.items()
        if stream_name in schemas
        and not stream_cls.parent
        and not stream_cls(client=client).check_access()
    ]

    for stream_name in inaccessible_streams:
        schemas.pop(stream_name, None)
        field_metadata.pop(stream_name, None)

    _prune_inaccessible_children(schemas, field_metadata)

    if inaccessible_streams:
        if not schemas:
            raise MixpanelForbiddenError(
                "HTTP-error-code: 403, Error: The account credentials supplied do not have 'read' access to any "
                "of the streams supported by the tap. Data collection cannot be initiated due to lack of permissions."
            )
        LOGGER.warning(
            "The account credentials supplied do not have 'read' access to the following stream(s): %s. "
            "These streams have been excluded from the catalog.",
            ", ".join(inaccessible_streams),
        )


def discover(client, properties_flag):
    """Run the discovery mode, prepare the catalog file and return catalog.

    Args:
        client (MixpanelClient): Client object to make http calls.
        properties_flag (str): Setting this argument to `true` ensures that new properties on
                               events and engage records are captured.

    Returns:
        singer.Catalog: Catalog object having schema and metadata of all the streams.
    """
    schemas, field_metadata = get_schemas(client, properties_flag)
    _apply_access_checks(client, schemas, field_metadata)
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=STREAMS[stream_name].key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
