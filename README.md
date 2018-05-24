# woo-tap

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

This tap:

- Pulls raw data from [WooCommerce](http://woocommerce.github.io/woocommerce-rest-api-docs/)
- Extracts the following resources:
  - [List-Orders](http://woocommerce.github.io/woocommerce-rest-api-docs/#list-all-orders)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state

---

Copyright &copy; 2018 Stitch
