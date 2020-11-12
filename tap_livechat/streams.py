import datetime
import pytz
import singer
import dateutil.parser as parser

from singer.utils import strftime as singer_strftime

LOGGER = singer.get_logger()

SUB_STREAMS = {
    'chats': ['messages', 'events']
}
TIMESTAMP_FIELDS = set(['ended_timestamp', 'started_timestamp', 'timestamp'])


def transform_value(key, value):
    if key in TIMESTAMP_FIELDS:
        value = datetime.datetime.utcfromtimestamp(value).replace(tzinfo=pytz.utc)
        # reformat to use RFC3339 format
        value = singer_strftime(value)

    return value


class Stream():
    name = None
    replication_method = None
    key_properties = None
    stream = None

    def __init__(self, client=None, start_date=None):
        self.client = client
        if start_date:
            self.start_date = start_date
        else:
            self.start_date = datetime.datetime.min.strftime('%Y-%m-%d')

    def is_selected(self):
        return self.stream is not None

    def update_bookmark(self, state, value):
        current_bookmark = singer.get_bookmark(state, self.name, self.replication_key)
        if value and value > current_bookmark:
            singer.write_bookmark(state, self.name, self.replication_key, value)


class Chats(Stream):
    name = 'chats'
    replication_method = 'INCREMENTAL'
    key_properties = ['id', 'agent_email']
    replication_key = 'ended_timestamp'

    def sync(self, state):
        try:
            sync_thru = singer.get_bookmark(state, self.name, self.replication_key)
        except TypeError:
            sync_thru = self.start_date

        sync_thru = max(sync_thru, self.start_date)
        date_from = parser.parse(sync_thru)

        events_stream = Events(self.client)
        messages_stream = Messages(self.client)

        for row in self.client.paging_get('chats', date_from=date_from.strftime('%Y-%m-%d'), include_pending=0):
            chat = {}
            for key, value in row.items():
                chat[key] = transform_value(key, value)

            # it's possible for a single chat to have multiple agents assigned
            # so we create one row per agent per ticket
            for agent in row.get('agents', []):
                chat['agent_email'] = agent['email']
                yield(self.stream, chat)

            # missed chats don't have agents, and have different fields for time
            if row['type'] == 'missed_chat':
                chat['agent_email'] = 'unassigned'
                yield(self.stream, chat)
                chat_date = row.get('time')
            else:
                chat_date = chat.get('ended_timestamp')

            if events_stream.is_selected() and row.get('events'):
                yield from events_stream.sync(chat)
            if messages_stream.is_selected() and row.get('messages'):
                yield from messages_stream.sync(chat)

            curr_synced_thru = max(sync_thru, chat_date)

        # messages are returned in descending order
        self.update_bookmark(state, curr_synced_thru)


class Messages(Stream):
    name = 'messages'
    replication_method = 'INCREMENTAL'
    key_properties = ['event_id']

    def sync(self, chat):
        for m in chat.get('messages'):
            message = {}
            for key, value in m.items():
                message[key] = transform_value(key, value)
            message = {
                **message,
                'chat_id': chat['id'],
                'chat_started': chat.get('started_timestamp') or chat.get('time'),
                'chat_ended': chat.get('ended_timestamp') or chat.get('time')
            }
            yield(self.stream, message)


class Events(Stream):
    name = 'events'
    replication_method = 'INCREMENTAL'
    key_properties = ['event_id']

    def sync(self, chat):
        for e in chat.get('events'):
            event = {}
            for key, value in e.items():
                event[key] = transform_value(key, value)

            event = {
                **event,
                'chat_id': chat['id'],
                'chat_started': chat.get('started_timestamp') or chat.get('time'),
                'chat_ended': chat.get('ended_timestamp') or chat.get('time')
            }
            yield(self.stream, event)


STREAMS = {
    "chats": Chats,
    "messages": Messages,
    "events": Events
}
