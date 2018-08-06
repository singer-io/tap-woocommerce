# tap-woocommerce

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [WooCommerce](http://woocommerce.github.io/woocommerce-rest-api-docs/)
- Extracts the following resources:
  - [List-Orders](http://woocommerce.github.io/woocommerce-rest-api-docs/#list-all-orders)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

Usage:

1. Create Config file from sample-config.json

```
{
  "url":"https://example.com/",
  "consumer_key":"ck_woocommerce",
  "consumer_secret":"cs_woocommerce",
  "start_date":"ISO8601-Date-String"
}
```
- the consumer key and consumer secret will be needed to be generated from within woocommerce settings > api > api keys
- start date will determine how far back in your order history the tap will go
	- this is only relevant for the initial run, progress afterwards will be bookmarked

2. Discover

```
$tap-woocommerce --config config.json --discover >> catalog.json
```
- Run the above to discover the data points the tap supports for each of Woocommerce's endpoints (currently only List-Orders)

3. Select Streams

```
    {
       "schema": {
            "properties": {...},
            "type": "object",
            "selected": true
        },
        "stream": "orders",
        "tap_stream_id": "orders"
    }
```
- Add ```"selected":true``` within the schema object to select the stream

4.Run the tap

```
$tap-woocommerce --config config.json --catalog catalog.json
```

5.Run with Stitch Target

- Install target

```
pip install target-stitch
```
- Create Config

```
{
  "client_id" : your_stitch_id,
  "token" : "your_stitch_token"
}
```
- Run tap with target

```
tap-woocommerce --config config.json --catalog catalog.json | target-stitch --config target-config.json
```
---

Copyright &copy; 2018 Stitch
