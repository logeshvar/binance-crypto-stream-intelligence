#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CONFIG_FILE="${TOPIC_CONFIG_FILE:-${SCRIPT_DIR}/topic_config.yaml}"
COMPOSE_FILE="${COMPOSE_FILE:-${REPO_ROOT}/docker-compose.yml}"
KAFKA_BIN="${KAFKA_BIN:-/opt/kafka/bin/kafka-topics.sh}"
BOOTSTRAP_SERVER="${KAFKA_INTERNAL_BOOTSTRAP_SERVERS:-kafka:9092}"

if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is required to create Kafka topics." >&2
  exit 1
fi

if ! docker compose version >/dev/null 2>&1; then
  echo "Docker Compose v2 is required. Try installing/updating Docker Desktop." >&2
  exit 1
fi

if [[ ! -f "${CONFIG_FILE}" ]]; then
  echo "Topic config not found: ${CONFIG_FILE}" >&2
  exit 1
fi

parse_topics() {
  awk '
    function trim(value) {
      gsub(/^[ \t"]+|[ \t",]+$/, "", value)
      return value
    }
    /^[[:space:]]*-[[:space:]]*name:/ {
      if (name != "") {
        print name "|" partitions "|" replication_factor "|" retention_ms "|" cleanup_policy
      }
      name = $0
      sub(/^[[:space:]]*-[[:space:]]*name:[[:space:]]*/, "", name)
      name = trim(name)
      partitions = ""
      replication_factor = "1"
      retention_ms = ""
      cleanup_policy = "delete"
      next
    }
    /^[[:space:]]*partitions:/ {
      partitions = $0
      sub(/^[^:]*:[[:space:]]*/, "", partitions)
      partitions = trim(partitions)
      next
    }
    /^[[:space:]]*replication_factor:/ {
      replication_factor = $0
      sub(/^[^:]*:[[:space:]]*/, "", replication_factor)
      replication_factor = trim(replication_factor)
      next
    }
    /^[[:space:]]*retention_ms:/ {
      retention_ms = $0
      sub(/^[^:]*:[[:space:]]*/, "", retention_ms)
      retention_ms = trim(retention_ms)
      next
    }
    /^[[:space:]]*cleanup_policy:/ {
      cleanup_policy = $0
      sub(/^[^:]*:[[:space:]]*/, "", cleanup_policy)
      cleanup_policy = trim(cleanup_policy)
      next
    }
    END {
      if (name != "") {
        print name "|" partitions "|" replication_factor "|" retention_ms "|" cleanup_policy
      }
    }
  ' "${CONFIG_FILE}"
}

echo "Checking Kafka connectivity at ${BOOTSTRAP_SERVER}..."
docker compose -f "${COMPOSE_FILE}" exec -T kafka "${KAFKA_BIN}" --bootstrap-server "${BOOTSTRAP_SERVER}" --list >/dev/null

while IFS="|" read -r topic partitions replication_factor retention_ms cleanup_policy; do
  [[ -z "${topic}" ]] && continue

  partitions="${partitions:-1}"
  replication_factor="${replication_factor:-1}"
  retention_ms="${retention_ms:-259200000}"
  cleanup_policy="${cleanup_policy:-delete}"

  echo "Creating topic ${topic} with ${partitions} partition(s), retention.ms=${retention_ms}"
  docker compose -f "${COMPOSE_FILE}" exec -T kafka "${KAFKA_BIN}" \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor "${replication_factor}" \
    --config "retention.ms=${retention_ms}" \
    --config "cleanup.policy=${cleanup_policy}" < /dev/null
done < <(parse_topics)

echo
echo "Kafka topics:"
docker compose -f "${COMPOSE_FILE}" exec -T kafka "${KAFKA_BIN}" --bootstrap-server "${BOOTSTRAP_SERVER}" --list
