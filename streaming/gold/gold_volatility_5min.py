from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.gold.common import apply_event_time_watermark


def classify_volatility(price_change_pct_col: F.Column) -> F.Column:
    absolute_change = F.abs(price_change_pct_col)
    return (
        F.when(absolute_change >= F.lit(1.5), F.lit("HIGH"))
        .when(absolute_change >= F.lit(0.5), F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
    )


def build_volatility_5min(trades_df: DataFrame, watermark_delay: str) -> DataFrame:
    watermarked = apply_event_time_watermark(trades_df, "event_time", watermark_delay)

    aggregated = watermarked.groupBy(
        F.col("symbol"),
        F.window("event_time", "5 minutes").alias("event_window"),
    ).agg(
        F.avg("price").alias("avg_price"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.stddev_pop("price").alias("price_stddev"),
        F.min_by(F.col("price"), F.col("event_time")).alias("start_price"),
        F.max_by(F.col("price"), F.col("event_time")).alias("end_price"),
    )

    with_change = aggregated.withColumn(
        "price_change_pct",
        F.when(
            F.col("start_price") > F.lit(0),
            ((F.col("end_price") - F.col("start_price")) / F.col("start_price")) * F.lit(100),
        ),
    )

    return with_change.select(
        "symbol",
        F.col("event_window.start").alias("window_start"),
        F.col("event_window.end").alias("window_end"),
        "avg_price",
        "min_price",
        "max_price",
        "price_stddev",
        "price_change_pct",
        classify_volatility(F.col("price_change_pct")).alias("volatility_level"),
    )
