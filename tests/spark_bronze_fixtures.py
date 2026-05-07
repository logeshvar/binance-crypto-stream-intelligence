from __future__ import annotations

import json
from datetime import date, datetime, timezone
from typing import Any

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DateType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


BRONZE_SCHEMA = StructType(
    [
        StructField("topic", StringType(), False),
        StructField("partition", IntegerType(), False),
        StructField("offset", LongType(), False),
        StructField("key", StringType(), True),
        StructField("value", StringType(), True),
        StructField("kafka_timestamp", TimestampType(), False),
        StructField("ingest_time", TimestampType(), False),
        StructField("process_date", DateType(), False),
    ]
)


def bronze_df(spark: SparkSession, topic: str, events: list[dict[str, Any]]):
    now = datetime(2026, 5, 7, 10, 0, tzinfo=timezone.utc)
    rows = [
        {
            "topic": topic,
            "partition": index % 3,
            "offset": index,
            "key": event.get("symbol"),
            "value": json.dumps(event),
            "kafka_timestamp": now,
            "ingest_time": now,
            "process_date": date(2026, 5, 7),
        }
        for index, event in enumerate(events)
    ]
    return spark.createDataFrame(rows, BRONZE_SCHEMA)


def trade_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "schema_version": "1.0",
        "event_type": "trade",
        "symbol": "BTCUSDT",
        "event_time": "2026-05-07T10:15:30.123Z",
        "trade_id": 123456,
        "price": "62500.12",
        "quantity": "0.005",
        "trade_value": "312.50060",
        "is_buyer_market_maker": False,
        "source": "binance",
        "ingest_time": "2026-05-07T10:15:30.500Z",
    }
    event.update(overrides)
    return event


def kline_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "schema_version": "1.0",
        "event_type": "kline",
        "symbol": "ETHUSDT",
        "event_time": "2026-05-07T10:15:59.999Z",
        "kline_start_time": "2026-05-07T10:15:00.000Z",
        "kline_close_time": "2026-05-07T10:15:59.999Z",
        "interval": "1m",
        "open_price": "3120.10",
        "high_price": "3124.00",
        "low_price": "3118.45",
        "close_price": "3122.30",
        "volume": "18.420",
        "quote_asset_volume": "57508.766",
        "number_of_trades": 394,
        "taker_buy_base_asset_volume": "10.200",
        "taker_buy_quote_asset_volume": "31847.460",
        "is_closed": True,
        "source": "binance",
        "ingest_time": "2026-05-07T10:16:00.250Z",
    }
    event.update(overrides)
    return event


def ticker_event(**overrides: Any) -> dict[str, Any]:
    event = {
        "schema_version": "1.0",
        "event_type": "ticker",
        "symbol": "SOLUSDT",
        "event_time": "2026-05-07T10:15:30.123Z",
        "price_change": "1.25000000",
        "price_change_percent": "0.820",
        "weighted_avg_price": "152.43000000",
        "last_price": "153.70000000",
        "last_quantity": "4.12000000",
        "open_price": "152.45000000",
        "high_price": "155.10000000",
        "low_price": "149.80000000",
        "volume": "512430.12000000",
        "quote_volume": "78100234.99000000",
        "open_time": "2026-05-06T10:15:30.123Z",
        "close_time": "2026-05-07T10:15:30.123Z",
        "first_trade_id": 1000000,
        "last_trade_id": 1012350,
        "trade_count": 12351,
        "source": "binance",
        "ingest_time": "2026-05-07T10:15:30.500Z",
    }
    event.update(overrides)
    return event
