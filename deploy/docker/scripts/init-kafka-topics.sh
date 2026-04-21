#!/usr/bin/env bash
# ADR-0058 phase 4c: ensure trade-event and counter-journal are created
# at the 256-partition width the Counter vshard model expects.
#
# Idempotent:
#   - topic missing  -> create at 256 partitions
#   - topic exists at < 256 partitions -> expand with --alter
#   - topic exists at >= 256 partitions -> leave alone
#
# Run inside the kafka container image (bitnami layout) so kafka-topics.sh
# and friends are on PATH. Bootstrap server is passed in via env.

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
REPLICATION="${REPLICATION:-1}"
TARGET_PARTITIONS="${TARGET_PARTITIONS:-256}"
TOPICS=(trade-event counter-journal)

# Wait for the broker — kafka-init runs under depends_on condition
# service_healthy already, but an extra retry loop is cheap insurance
# against the occasional race where the healthcheck fires before the
# admin API is ready.
deadline=$(( $(date +%s) + 60 ))
until kafka-broker-api-versions.sh --bootstrap-server "$BOOTSTRAP" >/dev/null 2>&1; do
    if (( $(date +%s) >= deadline )); then
        echo "kafka-init: broker at $BOOTSTRAP never became ready" >&2
        exit 1
    fi
    sleep 1
done

topic_partitions() {
    # Prints the partition count for $1, or empty string if topic is missing.
    kafka-topics.sh --bootstrap-server "$BOOTSTRAP" --describe --topic "$1" 2>/dev/null \
        | awk -F'\t' '/^Topic:/{for (i=1;i<=NF;i++) if ($i ~ /^PartitionCount: /) {sub("PartitionCount: ","",$i); print $i; exit}}'
}

for topic in "${TOPICS[@]}"; do
    current="$(topic_partitions "$topic")"
    if [[ -z "$current" ]]; then
        echo "kafka-init: creating $topic at $TARGET_PARTITIONS partitions"
        kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
            --create --topic "$topic" \
            --partitions "$TARGET_PARTITIONS" \
            --replication-factor "$REPLICATION"
    elif (( current < TARGET_PARTITIONS )); then
        echo "kafka-init: expanding $topic from $current to $TARGET_PARTITIONS partitions"
        kafka-topics.sh --bootstrap-server "$BOOTSTRAP" \
            --alter --topic "$topic" --partitions "$TARGET_PARTITIONS"
    else
        echo "kafka-init: $topic already has $current partitions (>= $TARGET_PARTITIONS); skipping"
    fi
done

echo "kafka-init: done"
