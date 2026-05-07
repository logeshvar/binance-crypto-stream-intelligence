from __future__ import annotations

import logging
import os
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from streaming.silver.common import (
    SilverJobConfig,
    read_bronze_delta_stream,
    write_silver_stream,
)
from streaming.silver.transformations import transform_klines, transform_tickers, transform_trades
from streaming.spark_session import create_spark_session


LOGGER = logging.getLogger(__name__)
SilverTransform = Callable[[DataFrame, str], DataFrame]


def build_jobs() -> list[tuple[SilverJobConfig, SilverTransform]]:
    return [
        (
            SilverJobConfig.from_env(
                app_name="silver-market-trades",
                source_table_name="bronze_market_trades_raw",
                output_table_name="silver_market_trades",
                checkpoint_subdir="silver/trades",
            ),
            lambda bronze_df, watermark_delay: transform_trades(
                bronze_df,
                watermark_delay=watermark_delay,
            ),
        ),
        (
            SilverJobConfig.from_env(
                app_name="silver-market-klines",
                source_table_name="bronze_market_klines_raw",
                output_table_name="silver_market_klines",
                checkpoint_subdir="silver/klines",
            ),
            lambda bronze_df, watermark_delay: transform_klines(
                bronze_df,
                watermark_delay=watermark_delay,
            ),
        ),
        (
            SilverJobConfig.from_env(
                app_name="silver-market-tickers",
                source_table_name="bronze_market_tickers_raw",
                output_table_name="silver_market_tickers",
                checkpoint_subdir="silver/tickers",
            ),
            lambda bronze_df, watermark_delay: transform_tickers(
                bronze_df,
                watermark_delay=watermark_delay,
            ),
        ),
    ]


def start_query(
    config: SilverJobConfig,
    transform: SilverTransform,
    spark: SparkSession,
) -> StreamingQuery:
    bronze_df = read_bronze_delta_stream(spark, config)
    silver_df = transform(bronze_df, config.watermark_delay)
    query = write_silver_stream(silver_df, config)
    LOGGER.info("Started Silver stream query=%s source=%s", config.output_table_name, config.source_path)
    return query


def main() -> None:
    logging.basicConfig(level=os.getenv("STREAMING_LOG_LEVEL", "INFO"))
    os.environ.setdefault("SPARK_UI_PORT", os.getenv("SILVER_SPARK_UI_PORT", "4050"))

    jobs = build_jobs()
    spark = create_spark_session("silver-market-all")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    queries: list[StreamingQuery] = []
    try:
        for config, transform in jobs:
            queries.append(start_query(config, transform, spark))
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
