#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_INTERNAL_BOOTSTRAP_SERVERS:-kafka:9092}"
TOPIC="${TOPIC_SIGNALS_ALERTS:-market.signals.alerts}"

docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --topic "${TOPIC}" \
  --property print.key=true \
  --property key.separator=" | " \
  --from-beginning
