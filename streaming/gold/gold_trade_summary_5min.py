from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DecimalType, LongType, StringType, StructField, StructType, TimestampType

from streaming.gold.common import apply_event_time_watermark


TRADE_SUMMARY_SCHEMA = StructType(
    [
        StructField("symbol", StringType(), nullable=False),
        StructField("window_start", TimestampType(), nullable=True),
        StructField("window_end", TimestampType(), nullable=True),
        StructField("total_volume", DecimalType(38, 18), nullable=True),
        StructField("number_of_trades", LongType(), nullable=False),
        StructField("average_trade_value", DecimalType(38, 22), nullable=True),
        StructField("max_trade_value", DecimalType(38, 18), nullable=True),
        StructField("min_trade_value", DecimalType(38, 18), nullable=True),
    ]
)


def empty_trade_summary_df(spark: SparkSession) -> DataFrame:
    return spark.createDataFrame([], schema=TRADE_SUMMARY_SCHEMA)


def build_trade_summary_5min(trades_df: DataFrame, watermark_delay: str) -> DataFrame:
    watermarked = apply_event_time_watermark(trades_df, "event_time", watermark_delay)

    aggregated = watermarked.groupBy(
        F.col("symbol"),
        F.window("event_time", "5 minutes").alias("event_window"),
    ).agg(
        F.sum("quantity").alias("total_volume"),
        F.count("*").alias("number_of_trades"),
        F.avg("trade_value").alias("average_trade_value"),
        F.max("trade_value").alias("max_trade_value"),
        F.min("trade_value").alias("min_trade_value"),
    )

    return aggregated.select(
        "symbol",
        F.col("event_window.start").alias("window_start"),
        F.col("event_window.end").alias("window_end"),
        "total_volume",
        "number_of_trades",
        "average_trade_value",
        "max_trade_value",
        "min_trade_value",
    )
