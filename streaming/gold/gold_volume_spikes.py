from __future__ import annotations

import os
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from streaming.gold.common import GoldJobConfig


def classify_volume_spike(volume_spike_ratio_col: F.Column) -> F.Column:
    return (
        F.when(volume_spike_ratio_col >= F.lit(2.0), F.lit("HIGH"))
        .when(volume_spike_ratio_col >= F.lit(1.5), F.lit("MEDIUM"))
        .otherwise(F.lit("LOW"))
    )


def build_volume_spike_records(
    current_summary_df: DataFrame,
    historical_summary_df: DataFrame,
    baseline_lookback_minutes: int,
) -> DataFrame:
    current = current_summary_df.select(
        "symbol",
        "window_start",
        "window_end",
        F.col("total_volume").alias("current_volume"),
        "number_of_trades",
    ).alias("current")
    history = historical_summary_df.select(
        "symbol",
        "window_start",
        "window_end",
        "total_volume",
    ).alias("history")

    lookback_interval = F.expr(f"INTERVAL {baseline_lookback_minutes} MINUTES")
    joined = current.join(
        history,
        on=(
            (F.col("current.symbol") == F.col("history.symbol"))
            & (F.col("history.window_end") < F.col("current.window_start"))
            & (F.col("history.window_end") >= (F.col("current.window_start") - lookback_interval))
        ),
        how="left",
    )

    with_baseline = joined.groupBy(
        F.col("current.symbol").alias("symbol"),
        F.col("current.window_start").alias("window_start"),
        F.col("current.window_end").alias("window_end"),
        F.col("current.current_volume").alias("current_volume"),
        F.col("current.number_of_trades").alias("number_of_trades"),
    ).agg(
        F.avg("history.total_volume").alias("baseline_volume"),
    )

    with_ratio = (
        with_baseline.where(F.col("baseline_volume").isNotNull() & (F.col("baseline_volume") > F.lit(0)))
        .withColumn(
            "volume_spike_ratio",
            F.col("current_volume") / F.col("baseline_volume"),
        )
        .withColumn("signal_strength", classify_volume_spike(F.col("volume_spike_ratio")))
    )

    return with_ratio.select(
        "symbol",
        "window_start",
        "window_end",
        "current_volume",
        "baseline_volume",
        "volume_spike_ratio",
        "signal_strength",
        "number_of_trades",
    )


def write_volume_spike_batch(
    batch_df: DataFrame,
    batch_id: int,
    *,
    spark: SparkSession,
    summary_path: str,
    output_path: str,
    baseline_lookback_minutes: int,
) -> None:
    if batch_df.rdd.isEmpty():
        return

    historical_summary_df = spark.read.format("delta").load(summary_path)
    volume_spikes_df = build_volume_spike_records(
        batch_df,
        historical_summary_df,
        baseline_lookback_minutes=baseline_lookback_minutes,
    )

    (
        volume_spikes_df.write.format("delta")
        .mode("append")
        .option("txnAppId", "gold-volume-spike-signals")
        .option("txnVersion", batch_id)
        .save(output_path)
    )


def start_volume_spike_query(spark: SparkSession, config: GoldJobConfig) -> StreamingQuery:
    baseline_lookback_minutes = int(os.getenv("GOLD_VOLUME_BASELINE_LOOKBACK_MINUTES", "30"))
    source_path = Path(config.source_path)
    if not (source_path / "_delta_log").exists():
        raise FileNotFoundError(
            f"Trade summary Delta table must exist before starting volume spikes: {config.source_path}"
        )

    reader = spark.readStream.format("delta")
    if config.max_files_per_trigger:
        reader = reader.option("maxFilesPerTrigger", config.max_files_per_trigger)

    writer = (
        reader.load(config.source_path)
        .writeStream.queryName(config.output_table_name)
        .foreachBatch(
            lambda batch_df, batch_id: write_volume_spike_batch(
                batch_df,
                batch_id,
                spark=spark,
                summary_path=config.source_path,
                output_path=config.output_path,
                baseline_lookback_minutes=baseline_lookback_minutes,
            )
        )
        .option("checkpointLocation", config.checkpoint_path)
    )

    if config.trigger_processing_time:
        writer = writer.trigger(processingTime=config.trigger_processing_time)

    return writer.start()
