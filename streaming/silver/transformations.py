from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, StructType

from streaming.silver.schemas import (
    KLINE_EVENT_SCHEMA,
    TICKER_EVENT_SCHEMA,
    TRADE_EVENT_SCHEMA,
)


DECIMAL_TYPE = DecimalType(38, 18)


def parse_bronze_payload(bronze_df: DataFrame, payload_schema: StructType) -> DataFrame:
    return bronze_df.withColumn("payload", F.from_json(F.col("value"), payload_schema))


def utc_timestamp(column_name: str):
    return F.to_timestamp(F.regexp_replace(F.col(column_name), "Z$", "+00:00"))


def transform_trades(
    bronze_df: DataFrame,
    *,
    watermark_delay: str | None = None,
    deduplicate: bool = True,
) -> DataFrame:
    parsed = parse_bronze_payload(bronze_df, TRADE_EVENT_SCHEMA)
    selected = parsed.select(
        F.upper(F.col("payload.symbol")).alias("symbol"),
        utc_timestamp("payload.event_time").alias("event_time"),
        F.col("payload.trade_id").cast("long").alias("trade_id"),
        F.col("payload.price").cast(DECIMAL_TYPE).alias("price"),
        F.col("payload.quantity").cast(DECIMAL_TYPE).alias("quantity"),
        F.col("payload.is_buyer_market_maker").alias("is_buyer_market_maker"),
        F.col("payload.source").alias("source"),
        utc_timestamp("payload.ingest_time").alias("ingest_time"),
        F.col("topic").alias("bronze_topic"),
        F.col("partition").alias("bronze_partition"),
        F.col("offset").alias("bronze_offset"),
        F.current_timestamp().alias("process_time"),
    ).withColumn("trade_value", (F.col("price") * F.col("quantity")).cast(DECIMAL_TYPE))

    filtered = selected.filter(
        (F.col("symbol").isNotNull())
        & (F.length(F.trim(F.col("symbol"))) > 0)
        & (F.col("event_time").isNotNull())
        & (F.col("trade_id").isNotNull())
        & (F.col("price") > F.lit(0))
        & (F.col("quantity") > F.lit(0))
    )

    if watermark_delay and filtered.isStreaming:
        filtered = filtered.withWatermark("event_time", watermark_delay)
    if deduplicate:
        filtered = filtered.dropDuplicates(["symbol", "trade_id"])

    return filtered.select(
        "symbol",
        "event_time",
        "trade_id",
        "price",
        "quantity",
        "trade_value",
        "is_buyer_market_maker",
        "source",
        "ingest_time",
        "bronze_topic",
        "bronze_partition",
        "bronze_offset",
        "process_time",
    )


def transform_klines(
    bronze_df: DataFrame,
    *,
    watermark_delay: str | None = None,
    deduplicate: bool = True,
) -> DataFrame:
    parsed = parse_bronze_payload(bronze_df, KLINE_EVENT_SCHEMA)
    selected = parsed.select(
        F.upper(F.col("payload.symbol")).alias("symbol"),
        utc_timestamp("payload.kline_start_time").alias("kline_start_time"),
        utc_timestamp("payload.kline_close_time").alias("kline_close_time"),
        F.col("payload.open_price").cast(DECIMAL_TYPE).alias("open_price"),
        F.col("payload.high_price").cast(DECIMAL_TYPE).alias("high_price"),
        F.col("payload.low_price").cast(DECIMAL_TYPE).alias("low_price"),
        F.col("payload.close_price").cast(DECIMAL_TYPE).alias("close_price"),
        F.col("payload.volume").cast(DECIMAL_TYPE).alias("volume"),
        F.col("payload.number_of_trades").cast("long").alias("number_of_trades"),
        F.col("payload.is_closed").alias("is_closed"),
        utc_timestamp("payload.event_time").alias("event_time"),
        F.col("payload.source").alias("source"),
        F.current_timestamp().alias("process_time"),
    )

    filtered = selected.filter(
        (F.col("symbol").isNotNull())
        & (F.length(F.trim(F.col("symbol"))) > 0)
        & (F.col("kline_start_time").isNotNull())
        & (F.col("kline_close_time").isNotNull())
        & (F.col("event_time").isNotNull())
        & (F.col("open_price").isNotNull())
        & (F.col("high_price").isNotNull())
        & (F.col("low_price").isNotNull())
        & (F.col("close_price").isNotNull())
        & (F.col("volume") >= F.lit(0))
    )

    if watermark_delay and filtered.isStreaming:
        filtered = filtered.withWatermark("event_time", watermark_delay)
    if deduplicate:
        filtered = filtered.dropDuplicates(["symbol", "kline_start_time", "kline_close_time"])

    return filtered


def transform_tickers(
    bronze_df: DataFrame,
    *,
    watermark_delay: str | None = None,
    deduplicate: bool = True,
) -> DataFrame:
    parsed = parse_bronze_payload(bronze_df, TICKER_EVENT_SCHEMA)
    selected = parsed.select(
        F.upper(F.col("payload.symbol")).alias("symbol"),
        utc_timestamp("payload.event_time").alias("event_time"),
        F.col("payload.last_price").cast(DECIMAL_TYPE).alias("last_price"),
        F.col("payload.price_change").cast(DECIMAL_TYPE).alias("price_change"),
        F.col("payload.price_change_percent").cast(DECIMAL_TYPE).alias("price_change_percent"),
        F.col("payload.weighted_avg_price").cast(DECIMAL_TYPE).alias("weighted_avg_price"),
        F.col("payload.high_price").cast(DECIMAL_TYPE).alias("high_price"),
        F.col("payload.low_price").cast(DECIMAL_TYPE).alias("low_price"),
        F.col("payload.volume").cast(DECIMAL_TYPE).alias("volume"),
        F.col("payload.quote_volume").cast(DECIMAL_TYPE).alias("quote_volume"),
        F.col("payload.source").alias("source"),
        F.current_timestamp().alias("process_time"),
    )

    filtered = selected.filter(
        (F.col("symbol").isNotNull())
        & (F.length(F.trim(F.col("symbol"))) > 0)
        & (F.col("event_time").isNotNull())
        & (F.col("last_price") > F.lit(0))
        & (F.col("volume") >= F.lit(0))
    )

    if watermark_delay and filtered.isStreaming:
        filtered = filtered.withWatermark("event_time", watermark_delay)
    if deduplicate:
        filtered = filtered.dropDuplicates(["symbol", "event_time"])

    return filtered
