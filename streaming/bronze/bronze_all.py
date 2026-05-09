from __future__ import annotations

import logging
import os
from pathlib import Path

from pyspark.sql import functions as F
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from streaming.bronze.common import (
    BronzeJobConfig,
    select_bronze_columns,
)
from streaming.logging_utils import configure_streaming_logging
from streaming.spark_session import create_spark_session


LOGGER = logging.getLogger(__name__)
COMBINED_QUERY_NAME = "bronze_market_raw_all"


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


def read_combined_kafka_stream(spark: SparkSession, configs: list[BronzeJobConfig]) -> DataFrame:
    first_config = configs[0]
    topics = ",".join(config.topic for config in configs)
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", first_config.kafka_bootstrap_servers)
        .option("subscribe", topics)
        .option("startingOffsets", first_config.starting_offsets)
        .option("failOnDataLoss", first_config.fail_on_data_loss)
    )

    if first_config.max_offsets_per_trigger:
        reader = reader.option("maxOffsetsPerTrigger", first_config.max_offsets_per_trigger)

    return reader.load()


def write_topic_batch(batch_df: DataFrame, batch_id: int, configs: list[BronzeJobConfig]) -> None:
    if batch_df.isEmpty():
        return

    batch_df.persist()
    try:
        for config in configs:
            topic_df = batch_df.filter(F.col("topic") == config.topic)
            if topic_df.isEmpty():
                continue

            (
                topic_df.write.format("delta")
                .mode("append")
                .option("txnAppId", f"{COMBINED_QUERY_NAME}_{config.table_name}")
                .option("txnVersion", batch_id)
                .partitionBy("process_date")
                .save(config.output_path)
            )
            LOGGER.info(
                "Wrote Bronze batch=%s topic=%s table=%s",
                batch_id,
                config.topic,
                config.table_name,
            )
    finally:
        batch_df.unpersist()


def start_combined_query(configs: list[BronzeJobConfig], spark: SparkSession) -> StreamingQuery:
    first_config = configs[0]
    checkpoint_root = Path(os.getenv("CHECKPOINT_ROOT", "./storage/checkpoints"))
    checkpoint_path = str(checkpoint_root / "bronze/all")

    kafka_df = read_combined_kafka_stream(spark, configs)
    bronze_df = select_bronze_columns(kafka_df)
    writer = (
        bronze_df.writeStream.queryName(COMBINED_QUERY_NAME)
        .option("checkpointLocation", checkpoint_path)
        .foreachBatch(lambda batch_df, batch_id: write_topic_batch(batch_df, batch_id, configs))
    )

    if first_config.trigger_processing_time:
        writer = writer.trigger(processingTime=first_config.trigger_processing_time)

    topics = ",".join(config.topic for config in configs)
    LOGGER.info("Starting combined Bronze stream query=%s topics=%s", COMBINED_QUERY_NAME, topics)
    return writer.start()


def main() -> None:
    configure_streaming_logging()
    os.environ.setdefault("SPARK_UI_PORT", os.getenv("BRONZE_SPARK_UI_PORT", "4040"))

    configs = build_configs()
    spark = create_spark_session("bronze-market-raw-all")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    query: StreamingQuery | None = None
    try:
        query = start_combined_query(configs, spark)
        spark.streams.awaitAnyTermination()
    finally:
        if query and query.isActive:
            query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
