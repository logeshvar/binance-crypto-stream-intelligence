from __future__ import annotations

import logging
import os

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from streaming.bronze.common import (
    BronzeJobConfig,
    read_kafka_stream,
    select_bronze_columns,
    write_bronze_stream,
)
from streaming.spark_session import create_spark_session


LOGGER = logging.getLogger(__name__)


def build_configs() -> list[BronzeJobConfig]:
    return [
        BronzeJobConfig.from_env(
            app_name="bronze-market-trades-raw",
            topic_env="TOPIC_TRADES_RAW",
            default_topic="market.trades.raw",
            table_name="bronze_market_trades_raw",
            checkpoint_subdir="bronze/trades",
        ),
        BronzeJobConfig.from_env(
            app_name="bronze-market-klines-raw",
            topic_env="TOPIC_KLINES_RAW",
            default_topic="market.klines.raw",
            table_name="bronze_market_klines_raw",
            checkpoint_subdir="bronze/klines",
        ),
        BronzeJobConfig.from_env(
            app_name="bronze-market-tickers-raw",
            topic_env="TOPIC_TICKERS_RAW",
            default_topic="market.tickers.raw",
            table_name="bronze_market_tickers_raw",
            checkpoint_subdir="bronze/tickers",
        ),
        BronzeJobConfig.from_env(
            app_name="bronze-market-invalid-events",
            topic_env="TOPIC_EVENTS_INVALID",
            default_topic="market.events.invalid",
            table_name="bronze_market_invalid_events",
            checkpoint_subdir="bronze/invalid_events",
        ),
    ]


def start_query(config: BronzeJobConfig, spark: SparkSession) -> StreamingQuery:
    kafka_df = read_kafka_stream(spark, config)
    bronze_df = select_bronze_columns(kafka_df)
    query = write_bronze_stream(bronze_df, config)
    LOGGER.info("Started Bronze stream query=%s topic=%s", config.table_name, config.topic)
    return query


def main() -> None:
    logging.basicConfig(level=os.getenv("STREAMING_LOG_LEVEL", "INFO"))
    os.environ.setdefault("SPARK_UI_PORT", os.getenv("BRONZE_SPARK_UI_PORT", "4040"))

    configs = build_configs()
    spark = create_spark_session("bronze-market-raw-all")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    queries: list[StreamingQuery] = []
    try:
        for config in configs:
            queries.append(start_query(config, spark))
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
