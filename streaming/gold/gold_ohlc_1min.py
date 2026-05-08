from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from streaming.gold.common import apply_event_time_watermark


def build_ohlc_1min(trades_df: DataFrame, watermark_delay: str) -> DataFrame:
    watermarked = apply_event_time_watermark(trades_df, "event_time", watermark_delay)

    aggregated = watermarked.groupBy(
        F.col("symbol"),
        F.window("event_time", "1 minute").alias("event_window"),
    ).agg(
        F.min_by(F.col("price"), F.col("event_time")).alias("open_price"),
        F.max("price").alias("high_price"),
        F.min("price").alias("low_price"),
        F.max_by(F.col("price"), F.col("event_time")).alias("close_price"),
        F.count("*").alias("trade_count"),
        F.sum("quantity").alias("total_quantity"),
        F.sum("trade_value").alias("total_trade_value"),
    )

    return aggregated.select(
        "symbol",
        F.col("event_window.start").alias("window_start"),
        F.col("event_window.end").alias("window_end"),
        "open_price",
        "high_price",
        "low_price",
        "close_price",
        "trade_count",
        "total_quantity",
        "total_trade_value",
    )
