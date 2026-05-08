from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


SILVER_TRADE_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("event_time", TimestampType(), nullable=False),
        StructField("trade_id", LongType(), nullable=False),
        StructField("price", DecimalType(38, 18), nullable=False),
        StructField("quantity", DecimalType(38, 18), nullable=False),
        StructField("trade_value", DecimalType(38, 18), nullable=False),
        StructField("is_buyer_market_maker", BooleanType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("ingest_time", TimestampType(), nullable=False),
        StructField("bronze_topic", StringType(), nullable=False),
        StructField("bronze_partition", LongType(), nullable=False),
        StructField("bronze_offset", LongType(), nullable=False),
        StructField("process_time", TimestampType(), nullable=False),
    ]
)


def utc_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc).replace(tzinfo=None)


def trade_row(
    *,
    symbol: str = "BTCUSDT",
    event_time: str = "2026-04-30T10:00:00Z",
    trade_id: int = 1,
    price: str = "100.00",
    quantity: str = "1.00",
):
    price_decimal = Decimal(price)
    quantity_decimal = Decimal(quantity)
    return (
        symbol,
        utc_ts(event_time),
        trade_id,
        price_decimal,
        quantity_decimal,
        price_decimal * quantity_decimal,
        False,
        "binance",
        utc_ts(event_time),
        "market.trades.raw",
        0,
        trade_id,
        utc_ts(event_time),
    )


def silver_trades_df(spark_session: SparkSession, rows: list[tuple]):
    return spark_session.createDataFrame(rows, schema=SILVER_TRADE_SCHEMA)
