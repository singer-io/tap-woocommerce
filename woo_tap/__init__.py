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
    "start_date":None
}

ENDPOINTS = {
    "orders":"orders?after={0}&page={1}"
}

def get_endpoint(endpoint, kwargs):
    '''Get the full url for the endpoint'''
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))
    
    after = urllib.parse.quote(kwargs[0])
    page = kwargs[1]
    return ENDPOINTS[endpoint].format(after,page)

def get_start(STATE, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        return CONFIG["start_date"]
    return current_bookmark

def load_schema(entity):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

    return schema

def filter_order(order):
    filtered = {
        "order_id":order["id"],
        "order_key":order["order_key"],
        "status":order["status"],
        "date_created":order["date_created"],
        "date_modified":order["date_modified"],
        "discount_total":order["discount_total"],
        "shipping_total":order["shipping_total"],
        "total":order["total"],
        "line_items":order["line_items"]
    }
    return filtered


def sync_orders(STATE, catalog):
    wcapi = API(
        url=CONFIG["url"],
        consumer_key=CONFIG["consumer_key"],
        consumer_secret=CONFIG["consumer_secret"],
        wp_api=True,
        version="wc/v2"
    )
    schema = load_schema("orders")
    singer.write_schema("orders", schema, ["order_id"], catalog.stream_alias)

    start = get_start(STATE, "contacts", "start_date")
    last_update = start
    page_number = 1
    while True:
        endpoint = get_endpoint("orders", [start, page_number])
        print(endpoint)
        orders = wcapi.get(endpoint).json()
        for order in orders:
            #prob need to convert dates to insure they are comparable
            if("date_created" in order) and (order["date_created"] > start):
                last_update = order["date_created"]
            order = filter_order(order)
            singer.write_record("orders", order)
        if len(orders) < 10:
            break
        else:
            page_number +=1

@attr.s
class Stream(object):
    tap_stream_id = attr.ib()
    sync = attr.ib()

STREAMS = [
    Stream("orders", sync_orders)
]

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
                if (hasattr(schema, "selected")) and (schema.selected is True):
                    selected_streams.append(stream)

    return selected_streams

def do_sync(STATE, catalogs):
    '''Sync the streams that were selected'''
    remaining_streams = get_streams_to_sync(STREAMS, STATE)
    selected_streams = get_selected_streams(remaining_streams, catalogs)
    if len(selected_streams) < 1:
        LOGGER.info("No Streams selected, please check that you have a schema selected in your catalog")
        return

    LOGGER.info("Starting sync. Will sync these streams: %s", [stream.tap_stream_id for stream in selected_streams])

    for stream in selected_streams:
        LOGGER.info("Syncing %s", stream.tap_stream_id)
        singer.set_currently_syncing(STATE, stream.tap_stream_id)
        singer.write_state(STATE)

        try:
            catalog = [cat for cat in catalogs.streams if cat.stream == stream.tap_stream_id][0]
            STATE = stream.sync(STATE, catalog)
        except SourceUnavailableException:
            pass

    # singer.set_currently_syncing(STATE, None)
    # singer.write_state(STATE)
    # LOGGER.info("Sync completed")

def get_abs_path(path):
    '''Returns the absolute path'''
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

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