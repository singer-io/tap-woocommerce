#!/usr/bin/env python3
import itertools
import os
import sys
import time
import re
import json
import attr
import urllib
import requests
import backoff
from requests.auth import HTTPBasicAuth
import singer
import singer.metrics as metrics
from singer import utils
import datetime
import dateutil
from dateutil import parser


REQUIRED_CONFIG_KEYS = ["url", "consumer_key", "consumer_secret", "start_date"]
LOGGER = singer.get_logger()

CONFIG = {
    "url": None,
    "consumer_key": None,
    "consumer_secret": None,
    "start_date":None
}

ENDPOINTS = {
    "orders":"wp-json/wc/v2/orders?after={0}&orderby=date&order=asc&per_page=100&page={1}"
}

def get_endpoint(endpoint, kwargs):
    '''Get the full url for the endpoint'''
    if endpoint not in ENDPOINTS:
        raise ValueError("Invalid endpoint {}".format(endpoint))
    
    after = urllib.parse.quote(kwargs[0])
    page = kwargs[1]
    return CONFIG["url"]+ENDPOINTS[endpoint].format(after,page)

def get_start(STATE, tap_stream_id, bookmark_key):
    current_bookmark = singer.get_bookmark(STATE, tap_stream_id, bookmark_key)
    if current_bookmark is None:
        return CONFIG["start_date"]
    return current_bookmark

def load_schema(entity):
    '''Returns the schema for the specified source'''
    schema = utils.load_json(get_abs_path("schemas/{}.json".format(entity)))

    return schema

def filter_items(item):
    filtered = {
        "id":int(item["id"]),
        "name":str(item["name"]),
        "product_id":int(item["product_id"]),
        "variation_id":int(item["variation_id"]),
        "quantity":int(item["quantity"]),
        "subtotal":float(item["subtotal"]),
        "subtotal_tax":float(item["subtotal_tax"]),
        "total":float(item["total"]),
        "sku":str(item["sku"]),
        "price":float(item["price"])
    }
    return filtered

def filter_coupons(coupon):
    filtered = {
        "id":int(coupon["id"]),
        "code":str(coupon["code"]),
        "discount":float(coupon["discount"])
    }
    return filtered

def filter_shipping(ship):
    filtered = {
        "id":int(ship["id"]),
        "method_title":str(ship["method_title"]),
        "method_id":str(ship["method_id"]),
        "total":float(ship["total"])
    }
    return filtered

def filter_order(order):
    tzinfo = parser.parse(CONFIG["start_date"]).tzinfo
    if "line_items" in order and len(order["line_items"])>0:
        line_items = [filter_items(item) for item in order["line_items"]]
    else:
        line_items = None
    if "coupon_lines" in order and len(order["coupon_lines"])>0:
        coupon_lines = [filter_coupons(coupon) for coupon in order["coupon_lines"]]
    else:
        coupon_lines = None
    if "shippng_lines" in order and len(order["shipping_lines"])>0:
        shipping_lines = [filter_shipping(ship) for ship in order["shipping_lines"]]
    else:
        shipping_lines = None

    filtered = {
        "order_id":int(order["id"]),
        "order_key":str(order["order_key"]),
        "status":str(order["status"]),
        "date_created":parser.parse(order["date_created"]).replace(tzinfo=tzinfo).isoformat(),
        "date_modified":parser.parse(order["date_modified"]).replace(tzinfo=tzinfo).isoformat(),
        "discount_total":float(order["discount_total"]),
        "shipping_total":float(order["shipping_total"]),
        "total":float(order["total"]),
        "line_items":line_items
    }
    return filtered

def giveup(exc):
    return exc.response is not None \
        and 400 <= exc.response.status_code < 500 \
        and exc.response.status_code != 429

@utils.backoff((backoff.expo,requests.exceptions.RequestException), giveup)
@utils.ratelimit(20, 1)
def gen_request(stream_id, url):
    with metrics.http_request_timer(stream_id) as timer:
        resp = requests.get(url, auth=HTTPBasicAuth(CONFIG["consumer_key"], CONFIG["consumer_secret"]))
        timer.tags[metrics.Tag.http_status_code] = resp.status_code
        resp.raise_for_status()
        return resp.json()


def sync_orders(STATE, catalog):
    schema = load_schema("orders")
    singer.write_schema("orders", schema, ["order_id"])

    start = get_start(STATE, "orders", "last_update")
    LOGGER.info("Only syncing orders updated since " + start)
    last_update = start
    page_number = 1
    with metrics.record_counter("orders") as counter:
        while True:
            endpoint = get_endpoint("orders", [start, page_number])
            LOGGER.info("GET %s", endpoint)
            orders = gen_request("orders",endpoint)
            for order in orders:
                counter.increment()
                order = filter_order(order)
                if("date_created" in order) and (parser.parse(order["date_created"]) > parser.parse(last_update)):
                    last_update = order["date_created"]
                singer.write_record("orders", order)
            if len(orders) < 100:
                break
            else:
                page_number +=1
    STATE = singer.write_bookmark(STATE, 'orders', 'last_update', last_update) 
    singer.write_state(STATE)
    LOGGER.info("Completed Orders Sync")
    return STATE

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
        except Exception as e:
            LOGGER.critical(e)
            raise e

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