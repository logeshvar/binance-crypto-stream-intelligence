from __future__ import annotations

import logging
import os

from sinks.alert_kafka_writer import AlertKafkaConfig
from sinks.alert_kafka_writer import build_volume_alert_events
from sinks.alert_kafka_writer import to_kafka_messages
from sinks.alert_kafka_writer import write_alerts_to_kafka
from streaming.alerts.common import AlertPublisherConfig
from streaming.alerts.common import create_alert_spark_session
from streaming.alerts.common import read_gold_delta_stream


LOGGER = logging.getLogger(__name__)


def main() -> None:
    logging.basicConfig(level=os.getenv("STREAMING_LOG_LEVEL", "INFO"))
    publisher_config = AlertPublisherConfig.from_env(
        app_name="publish-volume-alerts",
        source_table_name="gold_volume_spike_signals",
        checkpoint_subdir="alerts/volume_spikes",
    )
    kafka_config = AlertKafkaConfig.from_env("alerts/kafka_volume_spikes")

    spark = create_alert_spark_session(publisher_config.app_name)
    spark.sparkContext.setLogLevel(publisher_config.spark_log_level)

    source_df = read_gold_delta_stream(spark, publisher_config)
    alert_events_df = build_volume_alert_events(source_df)
    kafka_messages_df = to_kafka_messages(alert_events_df)
    query = write_alerts_to_kafka(kafka_messages_df, kafka_config)

    LOGGER.info(
        "Started volume alert publisher source=%s topic=%s",
        publisher_config.source_path,
        kafka_config.topic,
    )
    query.awaitTermination()


if __name__ == "__main__":
    main()
