#!/usr/bin/env bash
------------------------------------------------------

set -euo pipefail


ENV_FILE="$(dirname "$0")/../.env"
if [[ -f "$ENV_FILE" ]]; then
  export $(grep -v '^#' "$ENV_FILE" | xargs)
else
  echo "[ERROR] .env file not found at $ENV_FILE"
  exit 1
fi

KAFKA_BIN="$KAFKA_HOME/bin"
KAFKA_CONFIG="$KAFKA_HOME/config/server.properties"

case "${1:-}" in
  format)
    echo "[INFO] Formatting storage directory..."
    "$KAFKA_BIN/kafka-storage.sh" format \
      --standalone \
      -t "$KAFKA_CLUSTER_ID" \
      -c "$KAFKA_CONFIG"
    ;;
  start)
    echo "[INFO] Starting Kafka..."
    "$KAFKA_BIN/kafka-server-start.sh" "$KAFKA_CONFIG"
    ;;
  stop)
    echo "[INFO] Stopping Kafka..."
    "$KAFKA_BIN/kafka-server-stop.sh"
    ;;
  *)
    echo "Usage: $0 {format|start|stop}"
    exit 1
    ;;
esac
