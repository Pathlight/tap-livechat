import singer
from .client import Client
import datetime

LOGGER = singer.get_logger()


def sync_chats(client, stream, state):
    singer.write_schema(
        stream_name=stream.tap_stream_id,
        schema=stream.schema.to_dict(),
        key_properties=stream.key_properties,
    )

    bookmark = state['bookmarks']['chats']  # this is going to be a string
    curr_synced_thru = bookmark

    for row in client.paging_get('chats', date_from=bookmark, include_pending=0):
        # it's possible for a single chat to have multiple agents assigned
        # so we create one row per agent per ticket
        for agent in row.get('agents', []):
            row['agent_email'] = agent['email']
            singer.write_record(stream.tap_stream_id, row)
        if row['type'] == 'missed_chat':
            # missed chats don't have agents
            row['agent_email'] = 'unassigned'
            singer.write_record(stream.tap_stream_id, row)
            record_date = row.get('time')
            record_date = record_date[:10]
        else:
            record_date = row.get('ended_timestamp')
            record_date = datetime.datetime.utcfromtimestamp(record_date)
            record_date = datetime.datetime.strftime(record_date, '%Y-%m-%d')
        curr_synced_thru = max(curr_synced_thru, record_date)

    if curr_synced_thru > bookmark:
        state['bookmarks']['chats'] = curr_synced_thru
        singer.write_state(state)


def sync(config, state, catalog):
    """ Sync data from tap source """

    client = Client(config)
    if not state:
        state['bookmarks'] = {
            'chats': config['start_date']
        }

    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        if stream.tap_stream_id == 'chats':
            sync_chats(client, stream, state)
        else:
            LOGGER.info(stream.tap_stream_id + "not implemented")
