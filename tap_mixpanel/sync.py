import singer

from tap_mixpanel.streams import STREAMS

LOGGER = singer.get_logger()


def update_currently_syncing(state, stream_name):
    """
    Currently syncing sets the stream currently being delivered in the state.
    If the integration is interrupted, this state property is used to identify
     the starting point to continue from.
    Reference: https://github.com/singer-io/singer-python/blob/master/singer/bookmarks.py#L41-L46
    """
    if (stream_name is None) and ('currently_syncing' in state):
        del state['currently_syncing']
    else:
        singer.set_currently_syncing(state, stream_name)
    singer.write_state(state)


def sync(client, config, catalog, state, start_date):
    """
    Get selected_streams from catalog, based on state last_stream
    last_stream = Previous currently synced stream, if the load was interrupted
    """
    last_stream = singer.get_currently_syncing(state)
    LOGGER.info('last/currently syncing stream: %s', last_stream)
    selected_streams = []
    for stream in catalog.get_selected_streams(state):
        selected_streams.append(stream.stream)
    LOGGER.info('selected_streams: %s', selected_streams)

    if not selected_streams:
        return

    # Loop through selected_streams
    for stream_name in selected_streams:
        LOGGER.info('START Syncing: %s', stream_name)
        update_currently_syncing(state, stream_name)
        stream_obj = STREAMS[stream_name](client)
        endpoint_total = stream_obj.sync(
            catalog=catalog,
            state=state,
            config=config,
            start_date=start_date
        )

        update_currently_syncing(state, None)
        LOGGER.info('FINISHED Syncing: %s, Total endpoint records: %s', stream_name, endpoint_total)
