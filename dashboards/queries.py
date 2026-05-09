from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD_ROOT = REPO_ROOT / "storage" / "gold"

GOLD_TABLE_NAMES = [
    "gold_symbol_1min_ohlc",
    "gold_symbol_5min_trade_summary",
    "gold_symbol_5min_volatility",
    "gold_volume_spike_signals",
    "gold_price_movement_alerts",
    "gold_market_watchlist_summary",
]

ALERT_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("symbol", StringType()),
        StructField("alert_type", StringType()),
        StructField("severity", StringType()),
        StructField("window_start", StringType()),
        StructField("window_end", StringType()),
        StructField("metric_value", DoubleType()),
        StructField("description", StringType()),
        StructField("created_at", StringType()),
    ]
)


def get_gold_root() -> Path:
    return Path(os.getenv("GOLD_PATH", str(DEFAULT_GOLD_ROOT))).expanduser().resolve()


def get_gold_table_paths(gold_root: Path | None = None) -> dict[str, Path]:
    root = gold_root or get_gold_root()
    return {table_name: root / table_name for table_name in GOLD_TABLE_NAMES}


def delta_table_exists(path: Path) -> bool:
    return (path / "_delta_log").exists()


def read_delta_table(spark: SparkSession, path: Path) -> DataFrame | None:
    if not delta_table_exists(path):
        return None
    return spark.read.format("delta").load(str(path))


def empty_frame(columns: list[str]) -> pd.DataFrame:
    return pd.DataFrame(columns=columns)


def latest_watchlist(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_market_watchlist_summary"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "latest_price",
                "price_change_5m_pct",
                "volume_5m",
                "volatility_level",
                "latest_signal",
                "last_updated_time",
            ]
        )

    window = Window.partitionBy("symbol").orderBy(F.col("last_updated_time").desc())
    return (
        df.withColumn("rank", F.row_number().over(window))
        .where(F.col("rank") == 1)
        .drop("rank")
        .orderBy(F.col("volume_5m").desc())
        .limit(limit)
        .toPandas()
    )


def latest_volume_spikes(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_volume_spike_signals"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "window_start",
                "window_end",
                "current_volume",
                "baseline_volume",
                "volume_spike_ratio",
                "signal_strength",
                "number_of_trades",
            ]
        )
    return df.orderBy(F.col("window_end").desc(), F.col("volume_spike_ratio").desc()).limit(limit).toPandas()


def latest_volatility(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_symbol_5min_volatility"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "window_start",
                "window_end",
                "avg_price",
                "price_change_pct",
                "volatility_level",
            ]
        )
    return df.orderBy(F.col("window_end").desc(), F.abs(F.col("price_change_pct")).desc()).limit(limit).toPandas()


def latest_ohlc(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_symbol_1min_ohlc"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "window_start",
                "window_end",
                "open_price",
                "high_price",
                "low_price",
                "close_price",
                "trade_count",
            ]
        )
    return df.orderBy(F.col("window_end").desc(), F.col("trade_count").desc()).limit(limit).toPandas()


def latest_price_alerts(spark: SparkSession, limit: int = 50) -> pd.DataFrame:
    df = read_delta_table(spark, get_gold_table_paths()["gold_price_movement_alerts"])
    if df is None:
        return empty_frame(
            [
                "symbol",
                "window_start",
                "window_end",
                "start_price",
                "end_price",
                "price_change_pct",
                "alert_type",
                "severity",
            ]
        )
    return df.orderBy(F.col("window_end").desc(), F.abs(F.col("price_change_pct")).desc()).limit(limit).toPandas()


def table_health(spark: SparkSession) -> pd.DataFrame:
    rows = []
    for table_name, path in get_gold_table_paths().items():
        exists = delta_table_exists(path)
        row = {
            "table": table_name,
            "path": str(path),
            "delta_log_exists": exists,
            "row_count": 0,
            "latest_time": None,
        }
        if exists:
            df = spark.read.format("delta").load(str(path))
            row["row_count"] = df.count()
            time_cols = [col for col in ("window_end", "last_updated_time") if col in df.columns]
            if time_cols:
                row["latest_time"] = df.agg(F.max(time_cols[0]).alias("latest_time")).collect()[0]["latest_time"]
        rows.append(row)
    return pd.DataFrame(rows)


def freshness_label(latest_time: object, now: datetime | None = None) -> str:
    if latest_time is None or pd.isna(latest_time):
        return "NO_DATA"

    now_utc = now or datetime.now(timezone.utc)
    timestamp = pd.Timestamp(latest_time)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(timezone.utc)
    age_seconds = (pd.Timestamp(now_utc) - timestamp).total_seconds()

    if age_seconds <= 120:
        return "FRESH"
    if age_seconds <= 900:
        return "LAGGING"
    return "STALE"


def overview_metrics(spark: SparkSession) -> dict[str, object]:
    watchlist = latest_watchlist(spark)
    spikes = latest_volume_spikes(spark)
    price_alerts = latest_price_alerts(spark)
    health = table_health(spark)

    latest_time = None
    if not health.empty and "latest_time" in health:
        latest_values = health["latest_time"].dropna()
        if not latest_values.empty:
            latest_time = max(latest_values)

    return {
        "symbols": int(watchlist["symbol"].nunique()) if not watchlist.empty else 0,
        "high_volume_spikes": int((spikes.get("signal_strength") == "HIGH").sum()) if not spikes.empty else 0,
        "price_alerts": int(len(price_alerts)),
        "latest_gold_time": latest_time,
        "freshness": freshness_label(latest_time),
    }


def alert_topic_messages(
    spark: SparkSession,
    bootstrap_servers: str = "localhost:9092",
    topic: str = "market.signals.alerts",
    limit: int = 50,
) -> pd.DataFrame:
    try:
        kafka_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
    except Exception:
        return empty_frame(
            [
                "topic",
                "partition",
                "offset",
                "kafka_timestamp",
                "symbol_key",
                "alert_type",
                "severity",
                "metric_value",
                "description",
                "created_at",
            ]
        )

    parsed = (
        kafka_df.select(
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
            F.col("key").cast("string").alias("symbol_key"),
            F.from_json(F.col("value").cast("string"), ALERT_SCHEMA).alias("alert"),
        )
        .select(
            "topic",
            "partition",
            "offset",
            "kafka_timestamp",
            "symbol_key",
            "alert.*",
        )
        .orderBy(F.col("kafka_timestamp").desc(), F.col("offset").desc())
    )
    return parsed.limit(limit).toPandas()
