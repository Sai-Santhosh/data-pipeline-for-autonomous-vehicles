#!/usr/bin/env bash
# Create Kafka topics for the Fleet Data Pipeline (run after Kafka is up)
set -e
KAFKA_CONTAINER=${KAFKA_CONTAINER:-kafka}
docker exec -it "$KAFKA_CONTAINER" /opt/bitnami/kafka/bin/kafka-topics.sh \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic vehicle_telemetry --partitions 3 --replication-factor 1
docker exec -it "$KAFKA_CONTAINER" /opt/bitnami/kafka/bin/kafka-topics.sh \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic perception_events --partitions 3 --replication-factor 1
docker exec -it "$KAFKA_CONTAINER" /opt/bitnami/kafka/bin/kafka-topics.sh \
  --create --if-not-exists \
  --bootstrap-server localhost:9092 \
  --topic driving_events --partitions 3 --replication-factor 1
echo "Topics created."
