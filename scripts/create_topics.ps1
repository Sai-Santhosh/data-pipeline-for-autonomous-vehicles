# Create Kafka topics (Windows). Run after: docker-compose up -d
$KAFKA_CONTAINER = if ($env:KAFKA_CONTAINER) { $env:KAFKA_CONTAINER } else { "kafka" }
docker exec -it $KAFKA_CONTAINER /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic vehicle_telemetry --partitions 3 --replication-factor 1
docker exec -it $KAFKA_CONTAINER /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic perception_events --partitions 3 --replication-factor 1
docker exec -it $KAFKA_CONTAINER /opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --bootstrap-server localhost:9092 --topic driving_events --partitions 3 --replication-factor 1
Write-Host "Topics created."
