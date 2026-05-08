from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.gold.common import apply_event_time_watermark


def classify_price_alert(price_change_pct_col: F.Column) -> F.Column:
    return (
        F.when(price_change_pct_col >= F.lit(1.5), F.lit("PRICE_SURGE"))
        .when(price_change_pct_col <= F.lit(-1.5), F.lit("PRICE_DROP"))
    )


def classify_alert_severity(price_change_pct_col: F.Column) -> F.Column:
    absolute_change = F.abs(price_change_pct_col)
    return (
        F.when(absolute_change >= F.lit(3.0), F.lit("CRITICAL"))
        .when(absolute_change >= F.lit(1.5), F.lit("HIGH"))
        .otherwise(F.lit("LOW"))
    )


def build_price_movement_alerts(trades_df: DataFrame, watermark_delay: str) -> DataFrame:
    watermarked = apply_event_time_watermark(trades_df, "event_time", watermark_delay)

    aggregated = watermarked.groupBy(
        F.col("symbol"),
        F.window("event_time", "5 minutes").alias("event_window"),
    ).agg(
        F.min_by(F.col("price"), F.col("event_time")).alias("start_price"),
        F.max_by(F.col("price"), F.col("event_time")).alias("end_price"),
    )

    with_alerts = (
        aggregated.withColumn(
            "price_change_pct",
            F.when(
                F.col("start_price") > F.lit(0),
                ((F.col("end_price") - F.col("start_price")) / F.col("start_price")) * F.lit(100),
            ),
        )
        .withColumn("alert_type", classify_price_alert(F.col("price_change_pct")))
        .withColumn("severity", classify_alert_severity(F.col("price_change_pct")))
        .where(F.col("alert_type").isNotNull())
    )

    return with_alerts.select(
        "symbol",
        F.col("event_window.start").alias("window_start"),
        F.col("event_window.end").alias("window_end"),
        "start_price",
        "end_price",
        "price_change_pct",
        "alert_type",
        "severity",
    )
