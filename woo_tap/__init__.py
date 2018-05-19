#!/usr/bin/env python3
import itertools
import os
import sys
import time
import re
import json
import attr
import urllib
import dateutil.parser
import singer
import singer.metrics as metrics
from singer import utils
from singer import (UNIX_MILLISECONDS_INTEGER_DATETIME_PARSING,
                    _transform_datetime)
from woocommerce import API

REQUIRED_CONFIG_KEYS = ["url", "consumer_key", "consumer_secret", "start_date"]
LOGGER = singer.get_logger()

CONFIG = {
    "url": None,
    "consumer_key": None,
    "consumer_secret": None,
    "after":None
}

# ENDPOINTS = {
#     "orders":"/wp-json/wc/v2/orders%s" % (urllib.urlencode(CONFIG.after))
# }

def get_abs_path(path):
    '''Returns the absolute path'''
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def load_schema(entity):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

    return schema

def sync_orders(STATE, catalog):
    #this is where you make a call to woocommerce
    return STATE

@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    sync = attr.ib()

STREAMS = [
    Stream("orders", sync_orders)
]

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

def get_streams_to_sync(streams, state):
    '''Get the streams to sync'''
    current_stream = singer.get_currently_syncing(state)
    result = streams
    if current_stream:
        result = list(itertools.dropwhile(
            lambda x: x.tap_stream_id != current_stream, streams))
    if not result:
        raise Exception("Unknown stream {} in state".format(current_stream))
    return result


def get_selected_streams(remaining_streams, annotated_schema):
    selected_streams = []

    for stream in remaining_streams:
        tap_stream_id = stream.tap_stream_id
        for stream_idx, annotated_stream in enumerate(annotated_schema.streams):
            if tap_stream_id == annotated_stream.tap_stream_id:
                schema = annotated_stream.schema
                if "selected" in schema and schema.selected is True:

                    selected_streams.append(stream)

    return selected_streams

def do_sync(STATE, catalogs):
    '''Sync the streams that were selected'''
    remaining_streams = get_streams_to_sync(STREAMS, STATE)
    selected_streams = get_selected_streams(remaining_streams, catalogs)
    if len(selected_streams) < 1:
        LOGGER.info("No Streams selected, please check that you have a schema selected in your catalog")
        return

    LOGGER.info("Starting sync. Will sync these streams: %s",
                [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        LOGGER.info("Syncing %s", stream.tap_stream_id)
        singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            catalog = [c for c in catalogs.get('streams')
                       if c.get('stream') == stream.tap_stream_id][0]
            STATE = stream.sync(STATE, catalog)
        except SourceUnavailableException:
            pass

    singer.set_currently_syncing(STATE, None)
    singer.write_state(STATE)
    LOGGER.info("Sync completed")

def load_discovered_schema(stream):
    '''Attach inclusion automatic to each schema'''
    schema = load_schema(stream.tap_stream_id)
    for k in schema['properties']:
        schema['properties'][k]['inclusion'] = 'automatic'
    return schema

def discover_schemas():
    '''Iterate through streams, push to an array and return'''
    result = {'streams': []}
    for stream in STREAMS:
        LOGGER.info('Loading schema for %s', stream.tap_stream_id)
        result['streams'].append({'stream': stream.tap_stream_id,
                                  'tap_stream_id': stream.tap_stream_id,
                                  'schema': load_discovered_schema(stream)})
    return result

def do_discover():
    '''JSON dump the schemas to stdout'''
    LOGGER.info("Loading Schemas")
    json.dump(discover_schemas(), sys.stdout, indent=4)

@utils.handle_top_exception(LOGGER)
def main():
    '''Entry point'''
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    CONFIG.update(args.config)
    STATE = {}

    if args.state:
        STATE.update(args.state)
    if args.discover:
        do_discover()
    elif args.properties:
        do_sync(STATE, args.properties)
    elif args.catalog:
        do_sync(STATE, args.catalog)
    else:
        LOGGER.info("No Streams were selected")

if __name__ == "__main__":
    main()