from __future__ import annotations

import logging
import os
from collections.abc import Callable
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import StreamingQuery

from streaming.gold.common import GoldJobConfig, read_silver_delta_stream, write_gold_stream
from streaming.gold.gold_ohlc_1min import build_ohlc_1min
from streaming.gold.gold_price_alerts import build_price_movement_alerts
from streaming.gold.gold_trade_summary_5min import build_trade_summary_5min
from streaming.gold.gold_trade_summary_5min import empty_trade_summary_df
from streaming.gold.gold_volatility_5min import build_volatility_5min
from streaming.gold.gold_volume_spikes import start_volume_spike_query
from streaming.gold.gold_watchlist_summary import build_watchlist_summary
from streaming.logging_utils import configure_streaming_logging
from streaming.spark_session import create_spark_session


LOGGER = logging.getLogger(__name__)
GoldTransform = Callable[[DataFrame, str], DataFrame]


def build_jobs() -> list[tuple[GoldJobConfig, GoldTransform]]:
    trades_table = "silver_market_trades"
    return [
        (
            GoldJobConfig.from_env(
                app_name="gold-symbol-1min-ohlc",
                source_table_name=trades_table,
                output_table_name="gold_symbol_1min_ohlc",
                checkpoint_subdir="gold/ohlc_1min",
            ),
            build_ohlc_1min,
        ),
        (
            GoldJobConfig.from_env(
                app_name="gold-symbol-5min-trade-summary",
                source_table_name=trades_table,
                output_table_name="gold_symbol_5min_trade_summary",
                checkpoint_subdir="gold/trade_summary_5min",
            ),
            build_trade_summary_5min,
        ),
        (
            GoldJobConfig.from_env(
                app_name="gold-symbol-5min-volatility",
                source_table_name=trades_table,
                output_table_name="gold_symbol_5min_volatility",
                checkpoint_subdir="gold/volatility_5min",
            ),
            build_volatility_5min,
        ),
        (
            GoldJobConfig.from_env(
                app_name="gold-price-movement-alerts",
                source_table_name=trades_table,
                output_table_name="gold_price_movement_alerts",
                checkpoint_subdir="gold/price_alerts",
            ),
            build_price_movement_alerts,
        ),
        (
            GoldJobConfig.from_env(
                app_name="gold-market-watchlist-summary",
                source_table_name=trades_table,
                output_table_name="gold_market_watchlist_summary",
                checkpoint_subdir="gold/watchlist_summary",
            ),
            build_watchlist_summary,
        ),
    ]


def build_volume_spike_config() -> GoldJobConfig:
    return GoldJobConfig.from_env(
        app_name="gold-volume-spike-signals",
        source_table_name="gold_symbol_5min_trade_summary",
        output_table_name="gold_volume_spike_signals",
        checkpoint_subdir="gold/volume_spikes",
        source_root_env="GOLD_PATH",
    )


def ensure_trade_summary_table(spark: SparkSession, output_path: str) -> None:
    delta_log_path = Path(output_path) / "_delta_log"
    if delta_log_path.exists():
        return
    empty_trade_summary_df(spark).write.format("delta").mode("overwrite").save(output_path)


def start_query(
    config: GoldJobConfig,
    transform: GoldTransform,
    spark: SparkSession,
) -> StreamingQuery:
    silver_df = read_silver_delta_stream(spark, config)
    gold_df = transform(silver_df, config.watermark_delay)
    query = write_gold_stream(gold_df, config)
    LOGGER.info("Started Gold stream query=%s source=%s", config.output_table_name, config.source_path)
    return query


def main() -> None:
    configure_streaming_logging()
    os.environ.setdefault("SPARK_UI_PORT", os.getenv("GOLD_SPARK_UI_PORT", "4060"))

    jobs = build_jobs()
    spark = create_spark_session("gold-market-all")
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))

    queries: list[StreamingQuery] = []
    try:
        trade_summary_config = next(
            config for config, _ in jobs if config.output_table_name == "gold_symbol_5min_trade_summary"
        )
        ensure_trade_summary_table(spark, trade_summary_config.output_path)

        for config, transform in jobs:
            queries.append(start_query(config, transform, spark))
        volume_spike_config = build_volume_spike_config()
        queries.append(start_volume_spike_query(spark, volume_spike_config))
        LOGGER.info("Started Gold stream query=%s source=%s", volume_spike_config.output_table_name, volume_spike_config.source_path)
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            if query.isActive:
                query.stop()
        spark.stop()


if __name__ == "__main__":
    main()
