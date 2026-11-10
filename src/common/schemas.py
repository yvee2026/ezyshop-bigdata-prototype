"""Explicit event schemas (no schema inference on streams).

Two views of each event:
* *_JSON: the wire format as produced to Kafka. event_time stays a string so a malformed
  timestamp can be detected and quarantined instead of silently becoming NULL.
* REQUIRED_FIELDS: fields that must be non-null after parsing, otherwise the record is
  routed to bronze/_quarantine.
"""
from __future__ import annotations

from pyspark.sql.types import (ArrayType, DoubleType, IntegerType, StringType, StructField,
                               StructType)

CLICK_ACTIONS = ("view", "add_to_cart", "remove_from_cart")
PAYMENT_METHODS = ("card", "paypal", "afterpay", "apple_pay", "gift_card")

_COMMON = [
    StructField("event_id", StringType()),
    StructField("event_time", StringType()),
    StructField("customer_pid", StringType()),
]

CLICKS_JSON = StructType(_COMMON + [
    StructField("session_id", StringType()),
    StructField("page", StringType()),
    StructField("product_id", StringType()),
    StructField("action", StringType()),
    StructField("device", StringType()),
    StructField("referrer", StringType()),
])

ORDER_ITEM = StructType([
    StructField("product_id", StringType()),
    StructField("qty", IntegerType()),
    StructField("unit_price", DoubleType()),
])

ORDERS_JSON = StructType(_COMMON + [
    StructField("order_id", StringType()),
    StructField("items", ArrayType(ORDER_ITEM)),
    StructField("total", DoubleType()),
    StructField("payment_method", StringType()),
    StructField("campaign_id", StringType()),
])

REVIEWS_JSON = StructType(_COMMON + [
    StructField("product_id", StringType()),
    StructField("rating", IntegerType()),
    StructField("title", StringType()),
    StructField("text", StringType()),
])

SCHEMAS: dict[str, StructType] = {"clicks": CLICKS_JSON, "orders": ORDERS_JSON, "reviews": REVIEWS_JSON}

REQUIRED_FIELDS: dict[str, list[str]] = {
    "clicks": ["event_id", "event_time", "customer_pid", "product_id", "action"],
    "orders": ["event_id", "event_time", "customer_pid", "order_id", "items", "total"],
    "reviews": ["event_id", "event_time", "customer_pid", "product_id", "rating", "text"],
}
