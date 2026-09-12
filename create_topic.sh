set -euo pipefail

BOOTSTRAP_SERVER="${1:-localhost:9092}"
TOPIC_NAME="${2:-my-topic}"

kafka-topics.sh \
  --create \
  --topic "${TOPIC_NAME}" \
  --partitions 3 \
  --replication-factor 2 \
  --bootstrap-server "${BOOTSTRAP_SERVER}"

echo "Топик '${TOPIC_NAME}' создан. Подробности:"

kafka-topics.sh \
  --describe \
  --topic "${TOPIC_NAME}" \
  --bootstrap-server "${BOOTSTRAP_SERVER}"
