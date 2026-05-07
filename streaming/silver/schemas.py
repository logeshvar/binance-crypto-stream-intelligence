from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
)


TRADE_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("event_type", StringType()),
        StructField("symbol", StringType()),
        StructField("event_time", StringType()),
        StructField("trade_id", LongType()),
        StructField("price", StringType()),
        StructField("quantity", StringType()),
        StructField("trade_value", StringType()),
        StructField("is_buyer_market_maker", BooleanType()),
        StructField("source", StringType()),
        StructField("ingest_time", StringType()),
    ]
)


KLINE_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("event_type", StringType()),
        StructField("symbol", StringType()),
        StructField("event_time", StringType()),
        StructField("kline_start_time", StringType()),
        StructField("kline_close_time", StringType()),
        StructField("interval", StringType()),
        StructField("open_price", StringType()),
        StructField("high_price", StringType()),
        StructField("low_price", StringType()),
        StructField("close_price", StringType()),
        StructField("volume", StringType()),
        StructField("quote_asset_volume", StringType()),
        StructField("number_of_trades", LongType()),
        StructField("taker_buy_base_asset_volume", StringType()),
        StructField("taker_buy_quote_asset_volume", StringType()),
        StructField("is_closed", BooleanType()),
        StructField("source", StringType()),
        StructField("ingest_time", StringType()),
    ]
)


TICKER_EVENT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("event_type", StringType()),
        StructField("symbol", StringType()),
        StructField("event_time", StringType()),
        StructField("price_change", StringType()),
        StructField("price_change_percent", StringType()),
        StructField("weighted_avg_price", StringType()),
        StructField("last_price", StringType()),
        StructField("last_quantity", StringType()),
        StructField("open_price", StringType()),
        StructField("high_price", StringType()),
        StructField("low_price", StringType()),
        StructField("volume", StringType()),
        StructField("quote_volume", StringType()),
        StructField("open_time", StringType()),
        StructField("close_time", StringType()),
        StructField("first_trade_id", LongType()),
        StructField("last_trade_id", LongType()),
        StructField("trade_count", LongType()),
        StructField("source", StringType()),
        StructField("ingest_time", StringType()),
    ]
)
