from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.gold.common import apply_event_time_watermark
from streaming.gold.gold_price_alerts import classify_price_alert
from streaming.gold.gold_volatility_5min import classify_volatility


def build_watchlist_summary(trades_df: DataFrame, watermark_delay: str) -> DataFrame:
    watermarked = apply_event_time_watermark(trades_df, "event_time", watermark_delay)

    aggregated = watermarked.groupBy(
        F.col("symbol"),
        F.window("event_time", "5 minutes").alias("event_window"),
    ).agg(
        F.max_by(F.col("price"), F.col("event_time")).alias("latest_price"),
        F.min_by(F.col("price"), F.col("event_time")).alias("start_price"),
        F.max_by(F.col("price"), F.col("event_time")).alias("end_price"),
        F.sum("quantity").alias("volume_5m"),
        F.stddev_pop("price").alias("price_stddev"),
    )

    with_metrics = aggregated.withColumn(
        "price_change_5m_pct",
        F.when(
            F.col("start_price") > F.lit(0),
            ((F.col("end_price") - F.col("start_price")) / F.col("start_price")) * F.lit(100),
        ),
    )

    return with_metrics.select(
        "symbol",
        "latest_price",
        "price_change_5m_pct",
        "volume_5m",
        classify_volatility(F.col("price_change_5m_pct")).alias("volatility_level"),
        F.coalesce(classify_price_alert(F.col("price_change_5m_pct")), F.lit("NONE")).alias("latest_signal"),
        F.col("event_window.end").alias("last_updated_time"),
    )
